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

type SimpleDataSink struct {
	Res int
}

func (sk *SimpleDataSink) IsFull() bool {
	return sk.Res > 20
}

func (sk *SimpleDataSink) Write(data any) error {
	innerVal := data.(*DataChunk)
	sk.Res += innerVal.data.(int)
	return nil
}

type SimpleOperatorImpl struct {
}

func (oi *SimpleOperatorImpl) PreExecute(data any) error {
	return nil
}

func (oi *SimpleOperatorImpl) Execute(data any) error {
	res := data.(*DataChunk)
	innerVal := res.data.(int)
	innerVal++
	res.data = innerVal
	return nil
}

func (oi *SimpleOperatorImpl) PostExecute(data any) error {
	return nil
}
