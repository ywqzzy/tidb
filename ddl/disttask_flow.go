// Copyright 2023 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ddl

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"

	"github.com/pingcap/errors"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/parser/model"
	"github.com/pingcap/tidb/store/helper"
	"github.com/pingcap/tidb/table"
	"github.com/tikv/client-go/v2/tikv"
)

type litBackfillFlowHandle struct {
	d DDL
}

var _ dispatcher.TaskFlowHandle = (*litBackfillFlowHandle)(nil)

// NewLitBackfillFlowHandle creates a new litBackfillFlowHandle.
func NewLitBackfillFlowHandle(d DDL) dispatcher.TaskFlowHandle {
	return &litBackfillFlowHandle{
		d: d,
	}
}

func (*litBackfillFlowHandle) OnTicker(_ context.Context, _ *proto.Task) {
}

type baseDistTaskLogicalPlan interface {
	Child() *baseDistTaskLogicalPlan
	Valid() bool
	SetChild(*baseDistTaskLogicalPlan)
}

type DistTaskLogicalPlan struct {
	gTask        *proto.Task
	subTaskMetas [][]byte
	child        *baseDistTaskLogicalPlan
}

func (d DistTaskLogicalPlan) Child() *baseDistTaskLogicalPlan {
	return d.child
}

func (d DistTaskLogicalPlan) Valid() bool {
	return len(d.subTaskMetas) >= 0
}

func (d DistTaskLogicalPlan) SetChild(child *baseDistTaskLogicalPlan) {
	d.child = child
}

type DistPlanBuilder struct {
}

func (b *DistPlanBuilder) Build(gTask *proto.Task, d *ddl) (baseDistTaskLogicalPlan, error) {
	switch gTask.Type {
	case BackfillTaskType:
		return b.BuildBackFill(gTask, d)
	}
	// ywq todo
	return nil, nil
}

func (b *DistPlanBuilder) BuildBackFill(gTask *proto.Task, d *ddl) (plan baseDistTaskLogicalPlan, err error) {
	meta, err := b.ProcessNormalFlow(gTask, d)
	plan = &DistTaskLogicalPlan{
		gTask:        gTask,
		subTaskMetas: meta,
	}
	if plan.Valid() {
		meta, err := b.ProcessNormalFlow(gTask, d)
		if err != nil {
			return plan, err
		}
		newPlan := &DistTaskLogicalPlan{
			gTask:        gTask,
			subTaskMetas: meta,
		}
		plan.SetChild(newPlan)
		newPlan = plan
	}

}

func (b *DistPlanBuilder) processNonPartitionTableFlow(job *model.Job, tblInfo *model.TableInfo, gTask *proto.Task, d *ddl) (metas [][]byte, err error) {
	var subTaskMetas [][]byte
	switch gTask.Step {
	case proto.StepOne:
		serverNodes, err := dispatcher.GenerateSchedulerNodes(d.ctx)
		if err != nil {
			return nil, err
		}
		subTaskMetas = make([][]byte, 0, len(serverNodes))
		dummyMeta := &BackfillSubTaskMeta{}
		metaBytes, err := json.Marshal(dummyMeta)
		if err != nil {
			return nil, err
		}
		for range serverNodes {
			subTaskMetas = append(subTaskMetas, metaBytes)
		}
		gTask.Step = proto.StepTwo
		return subTaskMetas, nil
	case proto.StepTwo:
		return nil, nil
	default:
	}
	tbl, err := getTable(d.store, job.SchemaID, tblInfo)
	if err != nil {
		return nil, err
	}
	ver, err := getValidCurrentVersion(d.store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	startKey, endKey, err := getTableRange(d.jobContext(job.ID), d.ddlCtx, tbl.(table.PhysicalTable), ver.Ver, job.Priority)
	if startKey == nil && endKey == nil {
		// Empty table.
		gTask.Step = proto.StepOne
		return nil, nil
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	regionCache := d.store.(helper.Storage).GetRegionCache()
	recordRegionMetas, err := regionCache.LoadRegionsInKeyRange(tikv.NewBackofferWithVars(context.Background(), 20000, nil), startKey, endKey)
	if err != nil {
		return nil, err
	}

	subTaskMetas = make([][]byte, 0, 100)
	regionBatch := 20
	sort.Slice(recordRegionMetas, func(i, j int) bool {
		return bytes.Compare(recordRegionMetas[i].StartKey(), recordRegionMetas[j].StartKey()) < 0
	})
	for i := 0; i < len(recordRegionMetas); i += regionBatch {
		end := i + regionBatch
		if end > len(recordRegionMetas) {
			end = len(recordRegionMetas)
		}
		batch := recordRegionMetas[i:end]
		subTaskMeta := &BackfillSubTaskMeta{StartKey: batch[0].StartKey(), EndKey: batch[len(batch)-1].EndKey()}
		if i == 0 {
			subTaskMeta.StartKey = startKey
		}
		if end == len(recordRegionMetas) {
			subTaskMeta.EndKey = endKey
		}
		metaBytes, err := json.Marshal(subTaskMeta)
		if err != nil {
			return nil, err
		}
		subTaskMetas = append(subTaskMetas, metaBytes)
	}
	gTask.Step = proto.StepOne
	return subTaskMetas, nil
}

func (b *DistPlanBuilder) processPartitionTableFlow(tblInfo *model.TableInfo, gTask *proto.Task) (metas [][]byte, err error) {
	if gTask.State != proto.TaskStatePending {
		// This flow for partition table has only one step, finish task when it is not pending
		return nil, nil
	}

	defs := tblInfo.Partition.Definitions
	physicalIDs := make([]int64, len(defs))
	for i := range defs {
		physicalIDs[i] = defs[i].ID
	}

	subTaskMetas := make([][]byte, 0, len(physicalIDs))
	for _, physicalID := range physicalIDs {
		subTaskMeta := &BackfillSubTaskMeta{
			PhysicalTableID: physicalID,
		}

		metaBytes, err := json.Marshal(subTaskMeta)
		if err != nil {
			return nil, err
		}

		subTaskMetas = append(subTaskMetas, metaBytes)
	}
	gTask.Step = proto.StepOne
	return subTaskMetas, nil
}

// ProcessNormalFlow processes the normal flow.
func (b *DistPlanBuilder) ProcessNormalFlow(gTask *proto.Task, d *ddl) (metas [][]byte, err error) {
	var globalTaskMeta BackfillGlobalMeta
	if err = json.Unmarshal(gTask.Meta, &globalTaskMeta); err != nil {
		return nil, err
	}

	job := &globalTaskMeta.Job
	var tblInfo *model.TableInfo
	err = kv.RunInNewTxn(d.ctx, d.store, true, func(ctx context.Context, txn kv.Transaction) error {
		tblInfo, err = meta.NewMeta(txn).GetTable(job.SchemaID, job.TableID)
		return err
	})
	if err != nil {
		return nil, err
	}

	if tblInfo.Partition == nil {
		return b.processNonPartitionTableFlow(job, tblInfo, gTask, d)
	} else {
		return b.processPartitionTableFlow(tblInfo, gTask)
	}
}

/////

func (h *litBackfillFlowHandle) processNonPartitionTableFlow(job *model.Job, d *ddl, tblInfo *model.TableInfo, gTask *proto.Task) (metas [][]byte, err error) {
	var subTaskMetas [][]byte
	switch gTask.Step {
	case proto.StepOne:
		serverNodes, err := dispatcher.GenerateSchedulerNodes(d.ctx)
		if err != nil {
			return nil, err
		}
		subTaskMetas = make([][]byte, 0, len(serverNodes))
		dummyMeta := &BackfillSubTaskMeta{}
		metaBytes, err := json.Marshal(dummyMeta)
		if err != nil {
			return nil, err
		}
		for range serverNodes {
			subTaskMetas = append(subTaskMetas, metaBytes)
		}
		gTask.Step = proto.StepTwo
		return subTaskMetas, nil
	case proto.StepTwo:
		return nil, nil
	default:
	}
	tbl, err := getTable(d.store, job.SchemaID, tblInfo)
	if err != nil {
		return nil, err
	}
	ver, err := getValidCurrentVersion(d.store)
	if err != nil {
		return nil, errors.Trace(err)
	}
	startKey, endKey, err := getTableRange(d.jobContext(job.ID), d.ddlCtx, tbl.(table.PhysicalTable), ver.Ver, job.Priority)
	if startKey == nil && endKey == nil {
		// Empty table.
		gTask.Step = proto.StepOne
		return nil, nil
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	regionCache := d.store.(helper.Storage).GetRegionCache()
	recordRegionMetas, err := regionCache.LoadRegionsInKeyRange(tikv.NewBackofferWithVars(context.Background(), 20000, nil), startKey, endKey)
	if err != nil {
		return nil, err
	}

	subTaskMetas = make([][]byte, 0, 100)
	regionBatch := 20
	sort.Slice(recordRegionMetas, func(i, j int) bool {
		return bytes.Compare(recordRegionMetas[i].StartKey(), recordRegionMetas[j].StartKey()) < 0
	})
	for i := 0; i < len(recordRegionMetas); i += regionBatch {
		end := i + regionBatch
		if end > len(recordRegionMetas) {
			end = len(recordRegionMetas)
		}
		batch := recordRegionMetas[i:end]
		subTaskMeta := &BackfillSubTaskMeta{StartKey: batch[0].StartKey(), EndKey: batch[len(batch)-1].EndKey()}
		if i == 0 {
			subTaskMeta.StartKey = startKey
		}
		if end == len(recordRegionMetas) {
			subTaskMeta.EndKey = endKey
		}
		metaBytes, err := json.Marshal(subTaskMeta)
		if err != nil {
			return nil, err
		}
		subTaskMetas = append(subTaskMetas, metaBytes)
	}
	gTask.Step = proto.StepOne
	return subTaskMetas, nil
}

func (h *litBackfillFlowHandle) processPartitionTableFlow(tblInfo *model.TableInfo, gTask *proto.Task) (metas [][]byte, err error) {
	if gTask.State != proto.TaskStatePending {
		// This flow for partition table has only one step, finish task when it is not pending
		return nil, nil
	}

	defs := tblInfo.Partition.Definitions
	physicalIDs := make([]int64, len(defs))
	for i := range defs {
		physicalIDs[i] = defs[i].ID
	}

	subTaskMetas := make([][]byte, 0, len(physicalIDs))
	for _, physicalID := range physicalIDs {
		subTaskMeta := &BackfillSubTaskMeta{
			PhysicalTableID: physicalID,
		}

		metaBytes, err := json.Marshal(subTaskMeta)
		if err != nil {
			return nil, err
		}

		subTaskMetas = append(subTaskMetas, metaBytes)
	}
	gTask.Step = proto.StepOne
	return subTaskMetas, nil
}

// ProcessNormalFlow processes the normal flow.
func (h *litBackfillFlowHandle) ProcessNormalFlow(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task) (metas [][]byte, err error) {
	var globalTaskMeta BackfillGlobalMeta
	if err = json.Unmarshal(gTask.Meta, &globalTaskMeta); err != nil {
		return nil, err
	}
	d, ok := h.d.(*ddl)
	if !ok {
		return nil, errors.New("The getDDL result should be the type of *ddl")
	}

	job := &globalTaskMeta.Job
	var tblInfo *model.TableInfo
	err = kv.RunInNewTxn(d.ctx, d.store, true, func(ctx context.Context, txn kv.Transaction) error {
		tblInfo, err = meta.NewMeta(txn).GetTable(job.SchemaID, job.TableID)
		return err
	})
	if err != nil {
		return nil, err
	}

	if tblInfo.Partition == nil {
		return h.processNonPartitionTableFlow(job, d, tblInfo, gTask)
	} else {
		return h.processPartitionTableFlow(tblInfo, gTask)
	}
}

func (*litBackfillFlowHandle) ProcessErrFlow(_ context.Context, _ dispatcher.TaskHandle, task *proto.Task, receiveErr [][]byte) (meta []byte, err error) {
	// We do not need extra meta info when rolling back
	firstErr := receiveErr[0]
	task.Error = firstErr

	return nil, nil
}

func (*litBackfillFlowHandle) GetEligibleInstances(ctx context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return dispatcher.GenerateSchedulerNodes(ctx)
}

// IsRetryableErr implements TaskFlowHandle.IsRetryableErr interface.
func (*litBackfillFlowHandle) IsRetryableErr(error) bool {
	return true
}
