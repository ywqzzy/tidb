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
	"io"

	"github.com/pingcap/tidb/resourcemanager/pool/workerpool"
	"github.com/pingcap/tidb/resourcemanager/util"
)

type simpleSource[T comparable] struct {
	generator func() T
	NoopOperator
}

func newSimpleSource[T comparable](generator func() T) simpleSource[T] {
	return simpleSource[T]{generator: generator}
}

func (s simpleSource[T]) Read() (T, error) {
	res := s.generator()
	var zT T
	if res == zT {
		return zT, io.EOF
	}
	return res, nil
}

func (s simpleSource[T]) Display() string {
	return "simpleSource"
}

type simpleSink[R interface{}] struct {
	drainer func(R)
	NoopOperator
}

func newSimpleSink[R interface{}](drainer func(R)) *simpleSink[R] {
	return &simpleSink[R]{drainer: drainer}
}

func (s *simpleSink[R]) Write(data R) error {
	s.drainer(data)
	return nil
}

func (s *simpleSink[R]) Display() string {
	return "simpleSink"
}

type simpleOperator[T, R any] struct {
	AsyncOperator[T, R]
	transform func(T) R
}

func (s *simpleOperator[T, R]) Display() string {
	return "simpleOperator"
}

func newSimpleOperator[T, R any](transform func(task T) R, concurrency int) *simpleOperator[T, R] {
	pool, _ := workerpool.NewWorkerPool("simple", util.UNKNOWN, concurrency,
		func() workerpool.Worker[T, R] {
			return simpleWorker[T, R]{transform: transform}
		})
	return &simpleOperator[T, R]{
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
