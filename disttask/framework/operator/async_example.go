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
	"github.com/pingcap/tidb/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"sync"
)

type AsyncChunk struct {
	res *DemoChunk
}

type DemoChunk struct {
	res int
}

type asyncWorker struct {
	sink DataSink
}

func (aw *asyncWorker) HandleTask(task AsyncChunk) {
	task.res.res++
	logutil.BgLogger().Info("handle task", zap.Any("task", task))
	if aw.sink.IsFull() {
		return
	}
	aw.sink.Write(task)
}

func (*asyncWorker) Close() {}

type ExampleAsyncOperatorImpl struct {
	pool DataSource
	sink DataSink
}

func NewExampleAsyncOperatorImpl() AsyncOperatorImpl {
	res := &ExampleAsyncOperatorImpl{}
	return res
}

func (oi *ExampleAsyncOperatorImpl) SetSink(sink DataSink) {
	oi.sink = sink
}

func (oi *ExampleAsyncOperatorImpl) SetPool(pool DataSource) {
	oi.pool = pool
}

func (oi *ExampleAsyncOperatorImpl) GetPool() DataSource {
	return oi.pool
}

func (oi *ExampleAsyncOperatorImpl) PreExecute() error {
	return nil
}

func (oi *ExampleAsyncOperatorImpl) Start() {
	oi.pool.(*AsyncDataChannel[AsyncChunk]).channel.SetCreateWorker(oi.createWorker)
	oi.pool.(*AsyncDataChannel[AsyncChunk]).channel.Start()
}

func (oi *ExampleAsyncOperatorImpl) Wait() {
	oi.pool.(*AsyncDataChannel[AsyncChunk]).channel.ReleaseAndWait()
}

func (oi *ExampleAsyncOperatorImpl) createWorker() workerpool.Worker[AsyncChunk] {
	return &asyncWorker{oi.sink}
}

func (oi *ExampleAsyncOperatorImpl) PostExecute() error {
	return nil
}

func (oi *ExampleAsyncOperatorImpl) Display() string {
	return "AsyncOperator"
}

type SimpleAsyncDataSink struct {
	Res int
	cnt int
	mu  sync.Mutex
}

func (sas *SimpleAsyncDataSink) IsFull() bool {
	return sas.cnt >= 10
}

func (sas *SimpleAsyncDataSink) Write(data any) error {
	sas.mu.Lock()
	defer sas.mu.Unlock()
	innerVal := data.(AsyncChunk)
	sas.Res += innerVal.res.res
	sas.cnt++
	return nil
}

func (ss *SimpleAsyncDataSink) Display() string {
	return "SimpleAsyncDataSink"
}
