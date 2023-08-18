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
	"fmt"
	"io"

	"github.com/pingcap/tidb/resourcemanager/pool/workerpool"
	"golang.org/x/sync/errgroup"
)

// Operator defines the interface for each operator.
// Operator is the basic operation unit in the task execution.
// In each Operator, it will use a `workerpool` to run several workers.
type Operator interface {
	Open() error
	Close() error
	Display() string
}

// AsyncOperator can serve as DataSource and DataSink.
// Each Operator can use it to pass tasks.
//
//	Eg: op1 use AsyncOperator as sink, op2 use AsyncOperator as source.
//	    op1 call sink.Write, then op2's worker will handle the task.
type AsyncOperator[T, R any] struct {
	channel *workerpool.WorkerPool[T, R]
	closed  bool
}

// Open implements the DataSource Start.
func (c *AsyncOperator[T, R]) Open() error {
	c.channel.Start()
	return nil
}

// Close implement the DataSource Close.
func (c *AsyncOperator[T, R]) Close() error {
	if c.closed {
		return nil
	}
	c.channel.ReleaseAndWait()
	c.closed = true
	return nil
}

// Read reads data from source. Not used.
func (c *AsyncOperator[T, R]) Read() (R, error) {
	v, ok := <-c.channel.GetResultChan()
	if !ok {
		var zero R
		return zero, io.EOF
	}
	return v, nil
}

// Display show the name.
func (*AsyncOperator[T, R]) Display() string {
	var zT T
	var zR R
	return fmt.Sprintf("AsyncOperator[%T, %T]", zT, zR)
}

// Write data to sink.
func (c *AsyncOperator[T, R]) Write(data T) error {
	c.channel.AddTask(data)
	return nil
}

type ComposeOperator struct {
	g   errgroup.Group
	fns []func() error
}

func (c *ComposeOperator) Open() error {
	for _, fn := range c.fns {
		c.g.Go(fn)
	}
	return nil
}

func (c *ComposeOperator) Close() error {
	return c.g.Wait()
}

func (c *ComposeOperator) Display() string {
	return "ComposeOperator"
}

func Compose[A any](composer *ComposeOperator, op1 DataSource[A], op2 DataSink[A]) {
	composer.fns = append(composer.fns, func() error {
		for {
			b, err := op1.Read()
			if err != nil {
				if err == io.EOF {
					return op2.Close()
				}
				return err
			}
			err = op2.Write(b)
			if err != nil {
				if err == io.EOF {
					return op2.Close()
				}
				return err
			}
		}
	})
}

type NoopOperator struct {
}

func (n NoopOperator) Open() error {
	return nil
}

func (n NoopOperator) Close() error {
	return nil
}

func (n NoopOperator) Display() string {
	return ""
}
