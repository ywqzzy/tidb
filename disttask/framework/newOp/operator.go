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
	"sync"
)

type Operator interface {
	open() error
	close() error
	display() string
}

type Data[T any] interface {
	read() (T, error)
	write(data T) error
	close() error
	open() error
	display() string
}

// BaseOperator have DataSource and DataSink.
type BaseOperator[T any, U any] struct {
	Source Data[T]
	Sink   Data[U]
}

type OperatorWrapper[T, U any] struct {
	BaseOperator[T, U]
}

// read from source.
func (r *OperatorWrapper[T, U]) read() (T, error) {
	return r.Source.read()
}

// write to sink.
func (r *OperatorWrapper[T, U]) write(data U) error {
	return r.Sink.write(data)
}

func Compose[T, U, W any](op *OperatorWrapper[T, U], op1 *OperatorWrapper[U, W], data *AsyncData[U]) {
	op.Sink = data
	op1.Source = data
}

type Pipeline struct {
	ops []Operator
}

// NewPipeline create a new AsyncPipeline.
func NewPipeline(ops ...Operator) *Pipeline {
	return &Pipeline{
		ops: ops,
	}
}

// Execute start all operators waiting to handle tasks.
func (p *Pipeline) Execute() error {
	// Start running each operator.
	for _, op := range p.ops {
		err := op.open()
		if err != nil {
			return err
		}
	}
	return nil
}

// Close wait all tasks done.
func (p *Pipeline) Close() error {
	for _, op := range p.ops {
		err := op.close()
		if err != nil {
			return err
		}
	}
	return nil
}

// AddOperator insert operator to the end of the list
func (p *Pipeline) AddOperator(op Operator) {
	p.ops = append(p.ops, op)
}

// LastOperator get the last operator.
func (p *Pipeline) LastOperator() Operator {
	return p.ops[len(p.ops)-1]
}

// Display show the pipeline.
func (p *Pipeline) Display() string {
	level := 0
	res := ""
	for i, op := range p.ops {
		for j := 0; j < level; j++ {
			res += " "
		}
		res += op.display()
		if i != len(p.ops)-1 {
			res += "\n"
		}
		level++
	}
	return res
}

type AsyncData[T any] struct {
	Channel *workerpool.WorkerPool[T]
}

// Next read data.
func (*AsyncData[T]) read() (T, error) {
	var res T
	return res, nil
}

func (d *AsyncData[T]) close() error {
	d.Channel.ReleaseAndWait()
	return nil
}

func (d *AsyncData[T]) open() error {
	d.Channel.Start()
	return nil
}

// Display show the name.
func (*AsyncData[T]) display() string { return "AsyncDataChannel" }

// Write data to sink.
func (c *AsyncData[T]) write(data T) error {
	c.Channel.AddTask(data)
	return nil
}

// example....
type asyncChunk struct {
	res *demoChunk
}

type demoChunk struct {
	res int
}

type exampleOperator struct {
	OperatorWrapper[asyncChunk, asyncChunk]
}

func NewExampleOperatorWithSource(source Data[asyncChunk]) *exampleOperator {
	res := &exampleOperator{}
	res.Source = source
	return res
}

func NewExampleOperatorWithSink(sink Data[asyncChunk]) *exampleOperator {
	res := &exampleOperator{}
	res.Sink = sink
	return res
}

// Close implements AsyncOperator.
func (oi *exampleOperator) close() error {
	return oi.Source.close()
}

// Open implements AsyncOperator.
func (oi *exampleOperator) open() error {
	oi.Source.(*AsyncData[asyncChunk]).Channel.SetCreateWorker(
		func() workerpool.Worker[asyncChunk] {
			return &asyncWorker{oi.Sink}
		},
	)
	_ = oi.Source.open()
	return nil
}

// Display implements AsyncOperator.
func (oi *exampleOperator) display() string {
	return "ExampleAsyncOperator{source: " + oi.Source.display() + ", sink: " + oi.Sink.display() + "}"
}

type asyncWorker struct {
	sink Data[asyncChunk]
}

// HandleTask define the basic running process for each operator.
func (aw *asyncWorker) HandleTask(task asyncChunk) {
	task.res.res++
	_ = aw.sink.write(task)
}

// Close implement the Close interface for workerpool.
func (*asyncWorker) Close() {}

type simpleData struct {
	Res int
	mu  sync.Mutex
}

func (*simpleData) open() error {
	return nil
}

func (*simpleData) close() error {
	return nil
}

func (s *simpleData) write(data asyncChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	innerVal := data
	s.Res += innerVal.res.res
	return nil
}

func (s *simpleData) read() (asyncChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return asyncChunk{res: &demoChunk{s.Res}}, nil
}

// Display show the DataSink.
func (*simpleData) display() string {
	return "simpleDataSink"
}
