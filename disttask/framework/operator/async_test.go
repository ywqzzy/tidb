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

func NewAsyncPipeline() *AsyncPipeline {
	impl0 := NewExampleAsyncOperatorImpl("impl0")
	impl1 := NewExampleAsyncOperatorImpl("impl1")
	pool0 := impl0.GetPool()
	pool1 := impl1.GetPool()
	sink := &SimpleAsyncDataSink{0, 0, sync.Mutex{}}
	impl0.SetSink(pool1.(DataSink))
	impl1.SetSink(sink)

	op1 := AsyncOperator{
		source: pool0.(DataSource),
		sink:   pool1.(DataSink),
		impl:   &impl0,
	}
	op2 := AsyncOperator{
		source: pool1.(DataSource),
		sink:   sink,
		impl:   &impl1,
	}

	pipeline := &AsyncPipeline{}
	pipeline.AddOperator(&op1)
	pipeline.AddOperator(&op2)

	return pipeline
}

func TestPipelineAsync(t *testing.T) {
	pipeline := NewAsyncPipeline()
	pipeline.AsyncExecute()

	logutil.BgLogger().Info("ywqtest", zap.Any("res", pipeline.LastOperator().GetSink().(*SimpleAsyncDataSink).Res))
}
