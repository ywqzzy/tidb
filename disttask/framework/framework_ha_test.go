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

package framework_test

import (
	"context"
	"github.com/pingcap/failpoint"
	"github.com/pingcap/tidb/disttask/framework/dispatcher"
	"github.com/pingcap/tidb/disttask/framework/proto"
	"github.com/pingcap/tidb/disttask/framework/scheduler"
	"github.com/pingcap/tidb/domain/infosync"
	"github.com/pingcap/tidb/testkit"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

type haTestFlowHandle struct{}

var _ dispatcher.TaskFlowHandle = (*haTestFlowHandle)(nil)

func (*haTestFlowHandle) OnTicker(_ context.Context, _ *proto.Task) {
}

func (*haTestFlowHandle) ProcessNormalFlow(_ context.Context, _ dispatcher.TaskHandle, gTask *proto.Task) (metas [][]byte, err error) {
	if gTask.State == proto.TaskStatePending {
		gTask.Step = proto.StepOne
		return [][]byte{
			[]byte("task1"),
			[]byte("task2"),
			[]byte("task3"),
			[]byte("task4"),
			[]byte("task5"),
			[]byte("task6"),
			[]byte("task7"),
			[]byte("task8"),
			[]byte("task9"),
			[]byte("task10"),
		}, nil
	}
	if gTask.Step == proto.StepOne {
		gTask.Step = proto.StepTwo
		return [][]byte{
			[]byte("task11"),
		}, nil
	}
	return nil, nil
}

func (*haTestFlowHandle) ProcessErrFlow(_ context.Context, _ dispatcher.TaskHandle, _ *proto.Task, _ []error) (meta []byte, err error) {
	return nil, nil
}

func (*haTestFlowHandle) GetEligibleInstances(_ context.Context, _ *proto.Task) ([]*infosync.ServerInfo, error) {
	return generateSchedulerNodes4Test()
}

func (*haTestFlowHandle) IsRetryableErr(error) bool {
	return true
}

func RegisterHATaskMeta(m *sync.Map) {
	dispatcher.ClearTaskFlowHandle()
	dispatcher.RegisterTaskFlowHandle(proto.TaskTypeExample, &haTestFlowHandle{})
	scheduler.ClearSchedulers()
	scheduler.RegisterTaskType(proto.TaskTypeExample)
	scheduler.RegisterSchedulerConstructor(proto.TaskTypeExample, proto.StepOne, func(_ context.Context, _ int64, _ []byte, _ int64) (scheduler.Scheduler, error) {
		return &testScheduler{}, nil
	})
	scheduler.RegisterSchedulerConstructor(proto.TaskTypeExample, proto.StepTwo, func(_ context.Context, _ int64, _ []byte, _ int64) (scheduler.Scheduler, error) {
		return &testScheduler{}, nil
	})
	scheduler.RegisterSubtaskExectorConstructor(proto.TaskTypeExample, proto.StepOne, func(_ proto.MinimalTask, _ int64) (scheduler.SubtaskExecutor, error) {
		return &testSubtaskExecutor{m: m}, nil
	})
	scheduler.RegisterSubtaskExectorConstructor(proto.TaskTypeExample, proto.StepTwo, func(_ proto.MinimalTask, _ int64) (scheduler.SubtaskExecutor, error) {
		return &testSubtaskExecutor1{m: m}, nil
	})
}

func TestHABasic(t *testing.T) {
	defer dispatcher.ClearTaskFlowHandle()
	defer scheduler.ClearSchedulers()
	var m sync.Map
	RegisterHATaskMeta(&m)
	distContext := testkit.NewDistExecutionContext(t, 4)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockCleanScheduler", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockStopManager", "4*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown", "return(\":4000\")"))
	DispatchTaskAndCheckSuccess("😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockStopManager"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockCleanScheduler"))
	distContext.Close()

	// todo
	// 3. subtask fail before update subtasks states.
	// 4. dispatcher read serverInfo fail
}

func TestHAManyNodes(t *testing.T) {
	defer dispatcher.ClearTaskFlowHandle()
	defer scheduler.ClearSchedulers()
	var m sync.Map

	RegisterHATaskMeta(&m)
	distContext := testkit.NewDistExecutionContext(t, 30)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockCleanScheduler", "return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockStopManager", "30*return()"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown", "return(\":4000\")"))
	DispatchTaskAndCheckSuccess("😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockStopManager"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockCleanScheduler"))
	distContext.Close()
}

func TestSchedulerDownAndOwnerChange(t *testing.T) {
	defer dispatcher.ClearTaskFlowHandle()
	defer scheduler.ClearSchedulers()
	var m sync.Map
	RegisterTaskMeta(&m)
	// todo change the test.
	distContext := testkit.NewDistExecutionContext(t, 3)
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockStopManager", "3*return(true)"))
	require.NoError(t, failpoint.Enable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown", "return(true)"))
	DispatchTaskAndCheckSuccess("😊", t, &m)
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockTiDBDown"))
	require.NoError(t, failpoint.Disable("github.com/pingcap/tidb/disttask/framework/scheduler/mockStopManager"))
	distContext.Close()
}
