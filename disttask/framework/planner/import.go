// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package planner

import (
	"context"
	"encoding/json"
	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	"github.com/pingcap/tidb/disttask/framework/storage"
	"github.com/pingcap/tidb/sessionctx"
	"github.com/pingcap/tidb/util/sqlexec"

	"github.com/pingcap/tidb/br/pkg/lightning/checkpoints"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/disttask/framework/handle"
	"go.uber.org/zap"

	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/importinto"
	"github.com/pingcap/tidb/executor/importer"
	"github.com/pingcap/tidb/table/tables"
)

type importPlan1 struct {
	child plan
}

func (p *importPlan1) TP() string {
	return "importPlan1"
}

func (p *importPlan1) Child() plan {
	return p.child
}

func (p *importPlan1) SetChild(child plan) {
	p.child = child
}

func buildController(taskMeta *importinto.TaskMeta) (*importer.LoadDataController, error) {
	idAlloc := kv.NewPanickingAllocators(0)
	tbl, err := tables.TableFromMeta(idAlloc, taskMeta.Plan.TableInfo)
	if err != nil {
		return nil, err
	}

	astArgs, err := importer.ASTArgsFromStmt(taskMeta.Stmt)
	if err != nil {
		return nil, err
	}
	controller, err := importer.NewLoadDataController(&taskMeta.Plan, tbl, astArgs)
	if err != nil {
		return nil, err
	}
	return controller, nil
}

func toChunk(chunkCheckpoint checkpoints.ChunkCheckpoint) importinto.Chunk {
	return importinto.Chunk{
		Path:         chunkCheckpoint.FileMeta.Path,
		Offset:       chunkCheckpoint.Chunk.Offset,
		EndOffset:    chunkCheckpoint.Chunk.EndOffset,
		PrevRowIDMax: chunkCheckpoint.Chunk.PrevRowIDMax,
		Type:         chunkCheckpoint.FileMeta.Type,
		Compression:  chunkCheckpoint.FileMeta.Compression,
		Timestamp:    chunkCheckpoint.Timestamp,
	}
}

// todo: converting back and forth, we should unify struct and remove this function later.
func toChunkMap(engineCheckpoints map[int32]*checkpoints.EngineCheckpoint) map[int32][]importinto.Chunk {
	chunkMap := make(map[int32][]importinto.Chunk, len(engineCheckpoints))
	for id, ecp := range engineCheckpoints {
		chunkMap[id] = make([]importinto.Chunk, 0, len(ecp.Chunks))
		for _, chunkCheckpoint := range ecp.Chunks {
			chunkMap[id] = append(chunkMap[id], toChunk(*chunkCheckpoint))
		}
	}
	return chunkMap
}

func generateImportStepMetas(ctx context.Context, taskMeta *importinto.TaskMeta) (subtaskMetas []*importinto.ImportStepMeta, err error) {
	var chunkMap map[int32][]importinto.Chunk
	if len(taskMeta.ChunkMap) > 0 {
		chunkMap = taskMeta.ChunkMap
	} else {
		controller, err2 := buildController(taskMeta)
		if err2 != nil {
			return nil, err2
		}
		if err2 = controller.InitDataFiles(ctx); err2 != nil {
			return nil, err2
		}

		engineCheckpoints, err2 := controller.PopulateChunks(ctx)
		if err2 != nil {
			return nil, err2
		}
		chunkMap = toChunkMap(engineCheckpoints)
	}
	for id := range chunkMap {
		if id == common.IndexEngineID {
			continue
		}
		subtaskMeta := &importinto.ImportStepMeta{
			ID:     id,
			Chunks: chunkMap[id],
		}
		subtaskMetas = append(subtaskMetas, subtaskMeta)
	}
	return subtaskMetas, nil
}

func (ip *importPlan1) ToSubtasks(task *proto.Task) ([][]byte, error) {
	taskMeta := &importinto.TaskMeta{}
	err := json.Unmarshal(task.Meta, taskMeta)
	if err != nil {
		return nil, err
	}
	subtaskMetas, err := generateImportStepMetas(context.Background(), taskMeta)
	if err != nil {
		return nil, err
	}
	metaBytes := make([][]byte, 0, len(subtaskMetas))
	for _, subtaskMeta := range subtaskMetas {
		bs, err := json.Marshal(subtaskMeta)
		if err != nil {
			return nil, err
		}
		metaBytes = append(metaBytes, bs)
	}
	return metaBytes, nil
}

type importPlan2 struct {
	child plan
}

func job2Step(ctx context.Context, taskMeta *importinto.TaskMeta, step string) error {
	globalTaskManager, err := storage.GetTaskManager()
	if err != nil {
		return err
	}
	// todo: use dispatcher.TaskHandle
	// we might call this in scheduler later, there's no dispatcher.TaskHandle, so we use globalTaskManager here.
	return globalTaskManager.WithNewSession(func(se sessionctx.Context) error {
		exec := se.(sqlexec.SQLExecutor)
		return importer.Job2Step(ctx, exec, taskMeta.JobID, step)
	})
}

func (ip2 *importPlan2) ToSubtasks(task *proto.Task) ([][]byte, error) {
	taskMeta := &importinto.TaskMeta{}
	err := json.Unmarshal(task.Meta, taskMeta)
	stepMeta, err2 := toPostProcessStep(task, taskMeta)
	if err2 != nil {
		return nil, err2
	}
	if err = job2Step(context.Background(), taskMeta, importer.JobStepValidating); err != nil {
		return nil, err
	}
	log.Info("move to post-process step ", zap.Any("result", taskMeta.Result),
		zap.Any("step-meta", stepMeta))
	bs, err := json.Marshal(stepMeta)
	if err != nil {
		return nil, err
	}
	failpoint.Inject("failWhenDispatchPostProcessSubtask", func() {
		failpoint.Return(nil, errors.New("injected error after StepImport"))
	})
	return [][]byte{bs}, nil
}

func updateMeta(gTask *proto.Task, taskMeta *importinto.TaskMeta) error {
	bs, err := json.Marshal(taskMeta)
	if err != nil {
		return err
	}
	gTask.Meta = bs
	return nil
}

// we will update taskMeta in place and make gTask.Meta point to the new taskMeta.
func toPostProcessStep(gTask *proto.Task, taskMeta *importinto.TaskMeta) (*importinto.PostProcessStepMeta, error) {
	metas, err := handle.GetPreviousSubtaskMetas(gTask.ID, gTask.Step)
	if err != nil {
		return nil, err
	}

	subtaskMetas := make([]*importinto.ImportStepMeta, 0, len(metas))
	for _, bs := range metas {
		var subtaskMeta importinto.ImportStepMeta
		if err := json.Unmarshal(bs, &subtaskMeta); err != nil {
			return nil, err
		}
		subtaskMetas = append(subtaskMetas, &subtaskMeta)
	}
	var localChecksum verify.KVChecksum
	columnSizeMap := make(map[int64]int64)
	for _, subtaskMeta := range subtaskMetas {
		checksum := verify.MakeKVChecksum(subtaskMeta.Checksum.Size, subtaskMeta.Checksum.KVs, subtaskMeta.Checksum.Sum)
		localChecksum.Add(&checksum)

		taskMeta.Result.ReadRowCnt += subtaskMeta.Result.ReadRowCnt
		taskMeta.Result.LoadedRowCnt += subtaskMeta.Result.LoadedRowCnt
		for key, val := range subtaskMeta.Result.ColSizeMap {
			columnSizeMap[key] += val
		}
	}
	taskMeta.Result.ColSizeMap = columnSizeMap
	if err2 := updateMeta(gTask, taskMeta); err2 != nil {
		return nil, err2
	}
	return &importinto.PostProcessStepMeta{
		Checksum: importinto.Checksum{
			Size: localChecksum.SumSize(),
			KVs:  localChecksum.SumKVS(),
			Sum:  localChecksum.Sum(),
		},
	}, nil
}

func (p *importPlan2) TP() string {
	return "importPlan2"
}

func (p *importPlan2) Child() plan {
	return p.child
}

func (p *importPlan2) SetChildren(child plan) {
	p.child = child
}
