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
	"errors"
)

type Pipeline struct {
	operators []*Operator
}

func (p *Pipeline) AddOperator(op *Operator) {
	p.operators = append(p.operators, op)
}

func (p *Pipeline) LastOperator() *Operator {
	return p.operators[len(p.operators)-1]
}

func (p *Pipeline) PreExecute() error {
	if len(p.operators) < 1 {
		return errors.New("operator number < 1")
	}
	return nil
}

func (p *Pipeline) PostExecute() {}

func (p *Pipeline) Execute() (hasNext bool, err error) {
	hasNext, err = p.operators[0].Next()
	if err != nil {
		return false, err
	}
	if hasNext {
		for i := 1; i < len(p.operators); i++ {
			_, err = p.operators[i].Next()
			if err != nil {
				return false, err
			}
		}
	}
	return p.operators[0].HasNext(), nil
}

func (p *Pipeline) Display() string {
	level := 0
	fmtString := ""
	for _, operator := range p.operators {
		for i := 0; i < level; i++ {
			fmtString += " "
		}
		fmtString += operator.Display()
		fmtString += "\n"
		level++
	}
	return fmtString
}
