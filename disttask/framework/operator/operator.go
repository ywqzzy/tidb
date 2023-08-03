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

import "errors"

type PushOperatorImpl interface {
	PreExecute() error
	Execute(data any) error
	PostExecute() error
	Display() string
}

type DataSource interface {
	HasNext() bool
	Read() (any, error)
	Display() string
}

type DataSink interface {
	IsFull() bool
	Write(data any) error
	Display() string
}

type Operator struct {
	source DataSource
	sink   DataSink
	impl   PushOperatorImpl
}

type DataChunk struct {
	data any
}

func (o *Operator) PreExecute() error {
	if o.source == nil || o.sink == nil || o.impl == nil {
		return errors.New("operator init failed")
	}
	return o.impl.PreExecute()
}

func (o *Operator) Execute(data any) error {
	return o.impl.Execute(data)
}

func (o *Operator) PostExecute() error {
	return o.impl.PostExecute()
}

func (o *Operator) ReadFromSource() (any, error) {
	return o.source.Read()
}

func (o *Operator) WriteToSink(data any) error {
	return o.sink.Write(data)
}

func (o *Operator) GetSink() DataSink {
	return o.sink
}

func (o *Operator) HasNext() bool {
	return o.source.HasNext()
}

func (o *Operator) Display() string {
	return o.source.Display() + "-->" + o.impl.Display() + "-->" + o.sink.Display()
}

// bool hasData
func (o *Operator) Next() (bool, error) {
	if o.HasNext() {
		data, _ := o.ReadFromSource()
		err := o.PreExecute()
		if err != nil {
			return false, err
		}
		err = o.Execute(data)
		if err != nil {
			return false, err
		}
		err = o.PostExecute()
		if err != nil {
			return false, err
		}
		if !o.sink.IsFull() {
			err = o.WriteToSink(data)
			if err != nil {
				return false, err
			}
		} else {
			return false, nil
		}
		return true, nil
	}
	return false, nil
}
