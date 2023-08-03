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
)

type AsyncOperator struct {
	source DataSource
	sink   DataSink
	impl   AsyncOperatorImpl
}

func NewAsyncOperator(source DataSource, sink DataSink, impl AsyncOperatorImpl) *AsyncOperator {
	impl.SetSink(sink)
	return &AsyncOperator{
		source: source,
		sink:   sink,
		impl:   impl,
	}
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

func (op *AsyncOperator) Close() {
	op.impl.Close()
}

type AsyncOperatorImpl interface {
	SetSink(sink DataSink)
	SetSource(source DataSource)
	GetSource() DataSource
	SetPool(pool any)
	PreExecute() error
	PostExecute() error
	Start()
	Wait()
	Close()
	Display() string
}

func NewAsyncOperatorImpl[T any](name string, newImpl func() AsyncOperatorImpl) AsyncOperatorImpl {
	res := newImpl()
	pool, _ := workerpool.NewWorkerPoolWithOutCreateWorker[T](name,
		poolutil.DDL, 10)
	res.SetSource(&AsyncDataChannel[T]{pool})
	return res
}

func NewAsyncMemoryOperatorImpl[T any](name string, source DataSource, newImpl func() AsyncOperatorImpl) AsyncOperatorImpl {
	res := newImpl()
	pool, _ := workerpool.NewWorkerPoolWithOutCreateWorker[T](name,
		poolutil.DDL, 10)
	res.SetPool(pool)
	res.SetSource(source)
	return res
}

type AsyncDataChannel[T any] struct {
	channel *workerpool.WorkerPool[T]
}

func (c *AsyncDataChannel[T]) HasNext() bool      { return false }
func (c *AsyncDataChannel[T]) Read() (any, error) { return nil, nil }
func (c *AsyncDataChannel[T]) Display() string    { return "AsyncDataChannel" }
func (c *AsyncDataChannel[T]) IsFull() bool       { return false }
func (c *AsyncDataChannel[T]) Write(data any) error {
	c.channel.AddTask(data.(T))
	return nil
}
