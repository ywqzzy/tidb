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
	for aw.sink.IsFull() {
		logutil.BgLogger().Info("ywq test full")
		continue
	}
	_ = aw.sink.Write(task)
}

func (*asyncWorker) Close() {}

type ExampleAsyncOperatorImpl struct {
	source DataSource
	sink   DataSink
	pool   *workerpool.WorkerPool[AsyncChunk]
}

func NewExampleAsyncOperatorImpl() AsyncOperatorImpl {
	res := &ExampleAsyncOperatorImpl{}
	return res
}

func (oi *ExampleAsyncOperatorImpl) setSink(sink DataSink) {
	oi.sink = sink
}

func (oi *ExampleAsyncOperatorImpl) setSource(source DataSource) {
	oi.source = source
}

func (oi *ExampleAsyncOperatorImpl) setPool(pool any) {
	oi.pool = pool.(*workerpool.WorkerPool[AsyncChunk])
}

func (oi *ExampleAsyncOperatorImpl) getPool() any {
	return oi.pool
}

func (oi *ExampleAsyncOperatorImpl) getSource() DataSource {
	return oi.source
}

func (oi *ExampleAsyncOperatorImpl) PreExecute() error {
	return nil
}

func (oi *ExampleAsyncOperatorImpl) Start() {
	oi.pool.SetCreateWorker(oi.createWorker)
	oi.pool.Start()
}

func (oi *ExampleAsyncOperatorImpl) Wait() {
	oi.pool.ReleaseAndWait()
}

func (oi *ExampleAsyncOperatorImpl) createWorker() workerpool.Worker[AsyncChunk] {
	return &asyncWorker{oi.sink}
}

func (oi *ExampleAsyncOperatorImpl) PostExecute() error {
	return nil
}

func (oi *ExampleAsyncOperatorImpl) Display() string {
	return "ExampleAsyncOperator"
}

func (oi *ExampleAsyncOperatorImpl) AddTask(data any) {
	oi.pool.AddTask(data.(AsyncChunk))
}

// SourceFromMemoryAsyncOperatorImpl source not from channel
type SourceFromMemoryAsyncOperatorImpl struct {
	memory   DataSource
	pool     *workerpool.WorkerPool[AsyncChunk]
	sink     DataSink
	wg       sync.WaitGroup
	closed   bool
	mu       sync.Mutex
	cnt      int
	finalCnt int
}

func NewSourceFromMemoryAsyncOperatorImpl() AsyncOperatorImpl {
	res := &SourceFromMemoryAsyncOperatorImpl{}
	return res
}

func (oi *SourceFromMemoryAsyncOperatorImpl) AddTask(data any) {
	oi.pool.AddTask(data.(AsyncChunk))
}

func (oi *SourceFromMemoryAsyncOperatorImpl) setPool(pool any) {
	oi.pool = pool.(*workerpool.WorkerPool[AsyncChunk])
}

func (oi *SourceFromMemoryAsyncOperatorImpl) getPool() any {
	return oi.pool
}

func (oi *SourceFromMemoryAsyncOperatorImpl) setSink(sink DataSink) {
	oi.sink = sink
}

func (oi *SourceFromMemoryAsyncOperatorImpl) setSource(source DataSource) {
	oi.memory = source
}

func (oi *SourceFromMemoryAsyncOperatorImpl) getSource() DataSource {
	return oi.memory
}

func (oi *SourceFromMemoryAsyncOperatorImpl) PreExecute() error {
	return nil
}

func (oi *SourceFromMemoryAsyncOperatorImpl) Start() {
	oi.pool.SetCreateWorker(oi.createWorker)
	oi.pool.Start()
}

func (oi *SourceFromMemoryAsyncOperatorImpl) Wait() {
	oi.pool.ReleaseAndWait()
}

func (oi *SourceFromMemoryAsyncOperatorImpl) createWorker() workerpool.Worker[AsyncChunk] {
	return &asyncWorker{oi.sink}
}

func (oi *SourceFromMemoryAsyncOperatorImpl) PostExecute() error {
	return nil
}

func (oi *SourceFromMemoryAsyncOperatorImpl) Display() string {
	return "AsyncOperator"
}

type SimpleAsyncDataSink struct {
	Res int
	cnt int
	mu  sync.Mutex
}

func (sas *SimpleAsyncDataSink) IsFull() bool {
	return false
}

func (sas *SimpleAsyncDataSink) Write(data any) error {
	sas.mu.Lock()
	defer sas.mu.Unlock()
	innerVal := data.(AsyncChunk)
	logutil.BgLogger().Info("ywq test write")
	sas.Res += innerVal.res.res
	sas.cnt++
	return nil
}

func (sas *SimpleAsyncDataSink) HasNext() bool {
	logutil.BgLogger().Info("has next")
	sas.mu.Lock()
	defer sas.mu.Unlock()
	return sas.cnt > 0
}

func (sas *SimpleAsyncDataSink) Read() (any, error) {
	sas.mu.Lock()
	defer sas.mu.Unlock()
	sas.cnt--
	logutil.BgLogger().Info("ywq read")
	return AsyncChunk{&DemoChunk{3}}, nil
}

func (sas *SimpleAsyncDataSink) Display() string {
	return "SimpleAsyncDataSink"
}

type FinalAsyncDataSink struct {
	Res int
	mu  sync.Mutex
}

func (sas *FinalAsyncDataSink) IsFull() bool {
	return false
}

func (sas *FinalAsyncDataSink) Write(data any) error {
	sas.mu.Lock()
	defer sas.mu.Unlock()
	logutil.BgLogger().Info("ywq final aysncsink write")
	innerVal := data.(AsyncChunk)
	sas.Res += innerVal.res.res
	return nil
}

func (sas *FinalAsyncDataSink) HasNext() bool {
	return false
}

func (sas *FinalAsyncDataSink) Read() (any, error) {
	return nil, nil
}

func (sas *FinalAsyncDataSink) Display() string {
	return "FinalAsyncDataSink"
}
