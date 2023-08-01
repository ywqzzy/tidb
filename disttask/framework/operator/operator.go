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

type OperatorImpl interface {
	PreExecute(data any) error
	Execute(data any) error
	PostExecute(data any) error
}

type DataSource interface {
	HasNext() bool
	Read() (any, error)
}

type DataSink interface {
	IsFull() bool
	Write(data any) error
}

type Operator struct {
	source DataSource
	Sink   DataSink
	impl   OperatorImpl
}

type DataChunk struct {
	data any
}

func (o *Operator) PreExecute(data any) error {
	return o.impl.PreExecute(data)
}

func (o *Operator) Execute(data any) error {
	return o.impl.Execute(data)
}

func (o *Operator) PostExecute(data any) error {
	return o.impl.PostExecute(data)
}

func (o *Operator) ReadFromSource() (any, error) {
	return o.source.Read()
}

func (o *Operator) WriteToSink(data any) error {
	return o.Sink.Write(data)
}

func (o *Operator) HasNext() bool {
	return o.source.HasNext()
}

func ExecuteOperator(o *Operator) {
	for o.HasNext() {
		data, _ := o.ReadFromSource()
		o.PreExecute(data)
		o.Execute(data)
		o.PostExecute(data)
		if !o.Sink.IsFull() {
			o.WriteToSink(data)
		}
	}
}
