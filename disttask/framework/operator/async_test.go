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

package operator

import (
	poolutil "github.com/pingcap/tidb/resourcemanager/util"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func NewAsyncPipeline() (*AsyncPipeline, any) {
	impl0 := NewAsyncOperatorImpl[AsyncChunk]("impl0", NewExampleAsyncOperatorImpl, poolutil.DDL, 10)
	impl1 := NewAsyncOperatorImpl[AsyncChunk]("impl1", NewExampleAsyncOperatorImpl, poolutil.DDL, 10)
	impl2 := NewAsyncOperatorImpl[AsyncChunk]("impl2", NewExampleAsyncOperatorImpl, poolutil.DDL, 10)
	pool0 := impl0.getSource()
	pool1 := impl1.getSource()
	pool2 := impl2.getSource()
	sink := &SimpleAsyncDataSink{0, 0, sync.Mutex{}}
	op1 := NewAsyncOperator(pool0.(DataSource), pool1.(DataSink), false, impl0)
	op2 := NewAsyncOperator(pool1.(DataSource), pool2.(DataSink), false, impl1)
	op3 := NewAsyncOperator(pool2.(DataSource), sink, false, impl2)

	pipeline := &AsyncPipeline{}
	pipeline.AddOperator(op1)
	pipeline.AddOperator(op2)
	pipeline.AddOperator(op3)
	return pipeline, pool0
}

func NewAsyncPipeline2() *AsyncPipeline {
	impl0 := NewAsyncOperatorImpl[AsyncChunk]("impl0", NewExampleAsyncOperatorImpl, poolutil.DDL, 10)
	impl1 := NewAsyncOperatorImpl[AsyncChunk]("impl1", NewExampleAsyncOperatorImpl, poolutil.DDL, 10)
	pool0 := impl0.getSource()
	pool1 := impl1.getSource()
	sink := &SimpleAsyncDataSink{0, 0, sync.Mutex{}}
	sink2 := &FinalAsyncDataSink{0, sync.Mutex{}}
	impl2 := NewAsyncOperatorImplWithDataSource[AsyncChunk]("impl2", sink, NewSourceFromMemoryAsyncOperatorImpl, poolutil.DDL, 10)
	op1 := NewAsyncOperator(pool0.(DataSource), pool1.(DataSink), false, impl0)
	op2 := NewAsyncOperator(pool1.(DataSource), sink, false, impl1)
	op3 := NewAsyncOperator(sink, sink2, true, impl2)

	/// add index 的 build plan 业务实现。
	pipeline := &AsyncPipeline{}

	pipeline.AddOperator(op1)
	pipeline.AddOperator(op2)
	pipeline.AddOperator(op3)
	return pipeline
}

func TestPipelineAsync(t *testing.T) {
	pipeline, source := NewAsyncPipeline()
	pipeline.AsyncExecute()
	for i := 0; i < 10000; i++ {
		source.(*AsyncDataChannel[AsyncChunk]).Write(AsyncChunk{&DemoChunk{0}})
	}
	pipeline.Wait()
	require.Equal(t, 30000, pipeline.LastOperator().GetSink().(*SimpleAsyncDataSink).Res)
}

func TestPipelineAsync2(t *testing.T) {
	pipeline := NewAsyncPipeline2()
	pipeline.AsyncExecute()
	taskCnt := 1000
	for i := 0; i < taskCnt; i++ {
		pipeline.FirstOperator().AddTask(AsyncChunk{&DemoChunk{0}})
	}
	pipeline.LastOperator().SetEnd(taskCnt)
	pipeline.Wait()
	require.Equal(t, taskCnt*4, pipeline.LastOperator().GetSink().(*FinalAsyncDataSink).Res)
}
