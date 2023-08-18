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

package newOp

import (
	"github.com/pingcap/tidb/resourcemanager/pool/workerpool"
	poolutil "github.com/pingcap/tidb/resourcemanager/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"testing"
)

func newPipe() {
	op1 := &exampleOperator{}
	pool, _ := workerpool.NewWorkerPoolWithoutCreateWorker[asyncChunk]("test", poolutil.DDL, 10)
	data := &AsyncData[asyncChunk]{Channel: pool}
	pool2, _ := workerpool.NewWorkerPoolWithoutCreateWorker[asyncChunk]("test2", poolutil.DDL, 10)
	data2 := &AsyncData[asyncChunk]{Channel: pool2}

	data3 := &simpleData{}
	op2 := &exampleOperator{}

	op1.Source = data
	op2.Source = data2
	op1.Sink = data2
	op2.Sink = data3

	pipe := &Pipeline{}
	pipe.AddOperator(op1)
	pipe.AddOperator(op2)
	pipe.Display()
	pipe.Execute()
	for i := 0; i < 20; i++ {
		data.Write(asyncChunk{res: &demoChunk{1}})
	}
	pipe.Close()

	logutil.BgLogger().Info("res", zap.Int("res", data3.Res))
}

func TestPipelineAsync(t *testing.T) {
	newPipe()
}
