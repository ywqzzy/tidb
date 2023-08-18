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
	Read() (T, error)
	Write(data T) error
	Close() error
	Start() error
	Display() string
}

// BaseOperator have DataSource and DataSink.
type BaseOperator[T any, U any] struct {
	Source Data[T]
	Sink   Data[U]
}

type Pipeline struct {
	ops []Operator
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
func (*AsyncData[T]) Read() (T, error) {
	var res T
	return res, nil
}

func (d *AsyncData[T]) Close() error {
	d.Channel.ReleaseAndWait()
	return nil
}

func (d *AsyncData[T]) Start() error {
	d.Channel.Start()
	return nil
}

// Display show the name.
func (*AsyncData[T]) Display() string { return "AsyncDataChannel" }

// Write data to sink.
func (c *AsyncData[T]) Write(data T) error {
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
	BaseOperator[asyncChunk, asyncChunk]
}

// Close implements AsyncOperator.
func (oi *exampleOperator) close() error {
	return oi.Source.Close()
}

// Open implements AsyncOperator.
func (oi *exampleOperator) open() error {
	oi.Source.(*AsyncData[asyncChunk]).Channel.SetCreateWorker(
		func() workerpool.Worker[asyncChunk] {
			return &asyncWorker{oi.Sink}
		},
	)
	oi.Source.Start()
	return nil
}

// Display implements AsyncOperator.
func (oi *exampleOperator) display() string {
	return "ExampleAsyncOperator{ source: " + oi.Source.Display() + ", sink: " + oi.Sink.Display() + "}"
}

type asyncWorker struct {
	sink Data[asyncChunk]
}

// HandleTask define the basic running process for each operator.
func (aw *asyncWorker) HandleTask(task asyncChunk) {
	task.res.res++
	_ = aw.sink.Write(task)
}

// Close implement the Close interface for workerpool.
func (*asyncWorker) Close() {}

type simpleData struct {
	Res int
	mu  sync.Mutex
}

func (*simpleData) Start() error {
	return nil
}

func (*simpleData) Close() error {
	return nil
}

func (s *simpleData) Write(data asyncChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	innerVal := data
	s.Res += innerVal.res.res
	return nil
}

func (s *simpleData) Read() (asyncChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return asyncChunk{res: &demoChunk{s.Res}}, nil
}

// Display show the DataSink.
func (*simpleData) Display() string {
	return "simpleDataSink"
}
