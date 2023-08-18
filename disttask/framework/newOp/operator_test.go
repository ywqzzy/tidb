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
	"fmt"
	"github.com/pingcap/tidb/resourcemanager/pool/workerpool"
	poolutil "github.com/pingcap/tidb/resourcemanager/util"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"testing"
)

func newPipe(t *testing.T) {
	pool, _ := workerpool.NewWorkerPoolWithoutCreateWorker[asyncChunk]("test", poolutil.DDL, 10)
	pool2, _ := workerpool.NewWorkerPoolWithoutCreateWorker[asyncChunk]("test2", poolutil.DDL, 10)
	data := &AsyncData[asyncChunk]{Channel: pool}
	channel := &AsyncData[asyncChunk]{Channel: pool2}
	data3 := &simpleData{}
	op1 := NewExampleOperatorWithSource(data)
	op2 := NewExampleOperatorWithSink(data3)
	Compose[asyncChunk, asyncChunk, asyncChunk](&op1.OperatorWrapper, &op2.OperatorWrapper, channel)
	pipe := NewPipeline(op1, op2)
	logutil.BgLogger().Info(fmt.Sprintf("display %s", pipe.Display()))
	err := pipe.Execute()
	require.NoError(t, err)
	for i := 0; i < 20; i++ {
		err := data.write(asyncChunk{res: &demoChunk{1}})
		require.NoError(t, err)
	}
	err = pipe.Close()
	require.NoError(t, err)
	logutil.BgLogger().Info("res", zap.Int("res", data3.Res))
}

func TestPipelineAsync(t *testing.T) {
	newPipe(t)
}
