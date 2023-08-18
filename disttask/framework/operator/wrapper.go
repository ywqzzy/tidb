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
	"github.com/pingcap/tidb/resourcemanager/util"
	"io"
)

type simpleSource[T interface{}] struct {
	generator func() T
}

func (s *simpleSource[T]) Read() (T, error) {
	res := s.generator()
	if res == nil {
		var zT T
		return zT, io.EOF
	}
	return res, nil
}

func (s *simpleSource[T]) Display() string {
	return "simpleSource"
}

func newSimpleSource[T any](generator func() T) simpleSource[T] {
	return simpleSource[T]{generator: generator}
}

type simpleSink[R interface{}] struct {
	drainer func(R)
}

func (s *simpleSink[R]) Write(data R) error {
	res := s.drainer()
	if res == nil {
		var zT T
		return zT, io.EOF
	}
	return res, nil
}

func (s *simpleSink[R]) Display() string {
	return "simpleSink"
}

type simpleOperator[T, R any] struct {
	AsyncOperator[T, R]
	transform func(T) R
}

func (s simpleOperator[T, R]) Display() string {
	return "simpleOperator"
}

func newSimpleOperator[T, R any](transform func(task T) R, concurrency int) simpleOperator[T, R] {
	pool, _ := workerpool.NewWorkerPool("simple", util.UNKNOWN, concurrency,
		func() workerpool.Worker[T, R] {
			return simpleWorker[T, R]{transform: transform}
		})
	return simpleOperator[T, R]{
		AsyncOperator: AsyncOperator[T, R]{channel: pool},
	}
}

type simpleWorker[T, R any] struct {
	transform func(T) R
}

func (s simpleWorker[T, R]) HandleTask(task T) R {
	return s.transform(task)
}

func (s simpleWorker[T, R]) Close() {}
