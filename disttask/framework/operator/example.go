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

type SimpleDataSource struct {
	cnt int
}

func (ss *SimpleDataSource) HasNext() bool {
	return ss.cnt < 10
}

func (ss *SimpleDataSource) Read() (any, error) {
	ss.cnt++
	return &DataChunk{1}, nil
}

func (ss *SimpleDataSource) Display() string {
	return "SimpleDataSource"
}

type SimpleDataChannel struct {
	data   any
	isFull bool
}

func (cs *SimpleDataChannel) IsFull() bool {
	return cs.isFull
}

func (cs *SimpleDataChannel) Write(data any) error {
	cs.isFull = true
	cs.data = data
	return nil
}

func (ss *SimpleDataChannel) Display() string {
	return "SimpleDataChannel"
}

func (cs *SimpleDataChannel) HasNext() bool {
	return cs.isFull
}

func (cs *SimpleDataChannel) Read() (any, error) {
	cs.isFull = false
	return cs.data, nil
}

type SimpleDataSink struct {
	Res int
	cnt int
}

func (sks *SimpleDataSink) IsFull() bool {
	return sks.cnt > 10
}

func (sks *SimpleDataSink) Write(data any) error {
	innerVal := data.(*DataChunk)
	sks.Res += innerVal.data.(int)
	return nil
}

func (ss *SimpleDataSink) Display() string {
	return "SimpleDataSink"
}

type SimpleOperatorImpl struct {
}

func (oi *SimpleOperatorImpl) PreExecute() error {
	return nil
}

func (oi *SimpleOperatorImpl) Execute(data any) error {
	res := data.(*DataChunk)
	innerVal := res.data.(int)
	innerVal++
	res.data = innerVal
	return nil
}

func (oi *SimpleOperatorImpl) PostExecute() error {
	return nil
}

func (oi *SimpleOperatorImpl) Display() string {
	return "SimpleOperator"
}
