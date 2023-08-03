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
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"sync"
	"testing"
)

func NewAsyncPipeline() (*AsyncPipeline, any) {
	impl0 := NewAsyncOperatorImpl("impl0", NewExampleAsyncOperatorImpl)
	impl1 := NewAsyncOperatorImpl("impl1", NewExampleAsyncOperatorImpl)
	impl2 := NewAsyncOperatorImpl("impl2", NewExampleAsyncOperatorImpl)
	pool0 := impl0.GetPool()
	pool1 := impl1.GetPool()
	pool2 := impl2.GetPool()
	sink := &SimpleAsyncDataSink{0, 0, sync.Mutex{}}
	op1 := NewAsyncOperator(pool0.(DataSource), pool1.(DataSink), impl0)
	op2 := NewAsyncOperator(pool1.(DataSource), pool2.(DataSink), impl1)
	op3 := NewAsyncOperator(pool2.(DataSource), sink, impl2)

	pipeline := &AsyncPipeline{}
	pipeline.AddOperator(op1)
	pipeline.AddOperator(op2)
	pipeline.AddOperator(op3)

	return pipeline, pool0
}

func TestPipelineAsync(t *testing.T) {
	pipeline, source := NewAsyncPipeline()
	pipeline.AsyncExecute()
	for i := 0; i < 10000; i++ {
		source.(*AsyncDataChannel[AsyncChunk]).Write(AsyncChunk{&DemoChunk{0}})
	}
	pipeline.Wait()
	logutil.BgLogger().Info("ywqtest", zap.Any("res", pipeline.LastOperator().GetSink().(*SimpleAsyncDataSink).Res))
}
