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
	poolutil "github.com/pingcap/tidb/resourcemanager/util"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
	"sync"
)

type AsyncOperator struct {
	source DataSource
	sink   DataSink
	impl   AsyncOperatorImpl
}

func (op *AsyncOperator) Execute() {
	op.impl.Execute(op.source, op.sink)
}

func (op *AsyncOperator) Start() {
	op.impl.Start()
}

func (op *AsyncOperator) GetSink() DataSink {
	return op.sink
}

func (op *AsyncOperator) Wait() {
	op.impl.Wait()
}

type AsyncOperatorImpl interface {
	PreExecute() error
	Execute(source DataSource, sink DataSink) error
	PostExecute() error
	Start()
	Wait()
	Display() string
}

type AsyncChunk struct {
	res *Chunk
}

type Chunk struct {
	res int
}

type AsyncDataChannel struct {
	channel *workerpool.WorkerPool[AsyncChunk]
}

func (c *AsyncDataChannel) HasNext() bool { return false }
func (c *AsyncDataChannel) Read() (any, error) {
	logutil.BgLogger().Info("ywq test here")
	c.channel.AddTask(AsyncChunk{
		res: &Chunk{0},
	})
	return nil, nil
}
func (c *AsyncDataChannel) Display() string { return "channel" }
func (c *AsyncDataChannel) IsFull() bool    { return false }
func (c *AsyncDataChannel) Write(data any) error {
	c.channel.AddTask(data.(AsyncChunk))
	return nil
}

type asyncWorker struct {
	sink DataSink
}

func (aw *asyncWorker) HandleTask(task AsyncChunk) {
	task.res.res++
	logutil.BgLogger().Info("handle task", zap.Any("task", task))
	aw.sink.Write(task)
}

func (*asyncWorker) Close() {}

type ExampleAsyncOperatorImpl struct {
	pool DataSource
	sink DataSink
}

func NewExampleAsyncOperatorImpl(name string) ExampleAsyncOperatorImpl {
	res := ExampleAsyncOperatorImpl{}
	pool, _ := workerpool.NewWorkerPoolWithOutCreateWorker[AsyncChunk](name,
		poolutil.DDL, 10)
	res.pool = &AsyncDataChannel{pool}
	return res
}

func (oi *ExampleAsyncOperatorImpl) SetSink(sink DataSink) {
	oi.sink = sink
}

func (oi *ExampleAsyncOperatorImpl) GetPool() any {
	return oi.pool
}

func (oi *ExampleAsyncOperatorImpl) PreExecute() error {
	return nil
}

func (oi *ExampleAsyncOperatorImpl) Execute(source DataSource, sink DataSink) error {
	return nil
}

func (oi *ExampleAsyncOperatorImpl) Start() {
	oi.pool.(*AsyncDataChannel).channel.SetCreateWorker(oi.createWorker)
	oi.pool.(*AsyncDataChannel).channel.Start()
}

func (oi *ExampleAsyncOperatorImpl) Wait() {
	oi.pool.(*AsyncDataChannel).channel.ReleaseAndWait()
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

func (sks *SimpleAsyncDataSink) IsFull() bool {
	return sks.cnt > 10
}

func (sks *SimpleAsyncDataSink) Write(data any) error {
	sks.mu.Lock()
	defer sks.mu.Unlock()
	innerVal := data.(AsyncChunk)
	sks.Res += innerVal.res.res
	return nil
}

func (ss *SimpleAsyncDataSink) Display() string {
	return "SimpleAsyncDataSink"
}
