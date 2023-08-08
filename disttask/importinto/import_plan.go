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

package importinto

import (
	"context"
	"encoding/json"

	"github.com/pingcap/errors"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/log"
	verify "github.com/pingcap/tidb/br/pkg/lightning/verification"
	"github.com/pingcap/tidb/disttask/framework/handle"
	"github.com/pingcap/tidb/disttask/framework/planner"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"go.uber.org/zap"
)

type ImportPlan1 struct {
	child planner.Plan
}

func (p *ImportPlan1) TP() string {
	return "ImportPlan1"
}

func (p *ImportPlan1) Child() planner.Plan {
	return p.child
}

func (p *ImportPlan1) SetChild(child planner.Plan) {
	p.child = child
}

func (ip *ImportPlan1) ToSubtasks(task *proto.Task) ([][]byte, error) {
	taskMeta := &TaskMeta{}
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

type ImportPlan2 struct {
	child planner.Plan
}

func (ip2 *ImportPlan2) ToSubtasks(task *proto.Task) ([][]byte, error) {
	taskMeta := &TaskMeta{}
	err := json.Unmarshal(task.Meta, taskMeta)
	stepMeta, err2 := toPostProcessStep2(task, taskMeta)
	if err2 != nil {
		return nil, err2
	}
	if err = job2Step(context.Background(), taskMeta, "validating"); err != nil {
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

// we will update taskMeta in place and make gTask.Meta point to the new taskMeta.
func toPostProcessStep2(gTask *proto.Task, taskMeta *TaskMeta) (*PostProcessStepMeta, error) {
	metas, err := handle.GetPreviousSubtaskMetas(gTask.ID, gTask.Step)
	if err != nil {
		return nil, err
	}

	subtaskMetas := make([]*ImportStepMeta, 0, len(metas))
	for _, bs := range metas {
		var subtaskMeta ImportStepMeta
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
	return &PostProcessStepMeta{
		Checksum: Checksum{
			Size: localChecksum.SumSize(),
			KVs:  localChecksum.SumKVS(),
			Sum:  localChecksum.Sum(),
		},
	}, nil
}

func (p *ImportPlan2) TP() string {
	return "ImportPlan2"
}

func (p *ImportPlan2) Child() planner.Plan {
	return p.child
}

func (p *ImportPlan2) SetChild(child planner.Plan) {
	p.child = child
}

type importPlanBuilder struct {
}

func (b *importPlanBuilder) BuildPlan(_ context.Context) (planner.Plan, error) {
	res := &ImportPlan2{}
	res.SetChild(&ImportPlan1{})
	return res, nil
}
