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
	"sync"

	"github.com/pingcap/tidb/resourcemanager/pool/workerpool"
	poolutil "github.com/pingcap/tidb/resourcemanager/util"
	"github.com/pingcap/tidb/util/logutil"
)

type asyncChunk struct {
	res *demoChunk
}

type demoChunk struct {
	res int
}

func newExampleOperator(
	name string,
	component poolutil.Component,
	concurrency int,
) *AsyncOperator[asyncChunk, asyncChunk] {
	pool, _ := workerpool.NewWorkerPool(name, component, concurrency, newAsyncWorker)
	return &AsyncOperator[asyncChunk, asyncChunk]{channel: pool}
}

type asyncWorker struct {
}

func newAsyncWorker() workerpool.Worker[asyncChunk, asyncChunk] {
	return &asyncWorker{}
}

// HandleTask define the basic running process for each operator.
func (aw *asyncWorker) HandleTask(task asyncChunk) asyncChunk {
	task.res.res++
	return task
}

// Close implement the Close interface for workerpool.
func (*asyncWorker) Close() {}

type simpleDataSink struct {
	Res int
	mu  sync.Mutex
}

func newExampleSink(initVal int) *simpleDataSink {
	return &simpleDataSink{initVal, sync.Mutex{}}
}

// Write data to sink.
func (s *simpleDataSink) Write(data asyncChunk) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	innerVal := data
	s.Res += innerVal.res.res
	return nil
}

// Display show the DataSink.
func (*simpleDataSink) Display() string {
	return "simpleDataSink[asyncChunk]"
}

// Open the sink.
func (*simpleDataSink) Open() error {
	return nil
}

// Close the sink.
func (*simpleDataSink) Close() error {
	return nil
}

type simpleDataSource struct {
	cnt int
	mu  sync.Mutex
}

// Open the source.
func (*simpleDataSource) Open() error {
	logutil.BgLogger().Info("simpleDataSource open")
	return nil
}

// Close the source.
func (s *simpleDataSource) Close() error {
	return nil
}

// Read reads data.
func (s *simpleDataSource) Read() (asyncChunk, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.cnt > 0 {
		s.cnt--
		return asyncChunk{&demoChunk{0}}, nil
	}
	return asyncChunk{}, io.EOF
}

func newExampleSource(cnt int) *simpleDataSource {
	return &simpleDataSource{cnt: cnt, mu: sync.Mutex{}}
}

// Display show the DataSource.
func (*simpleDataSource) Display() string {
	return "simpleDataSource[asyncChunk]"
}
