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

type AsyncPipeline struct {
	ops []*AsyncOperator
}

func (p *AsyncPipeline) AsyncExecute() error {
	// start running each operator.
	for _, op := range p.ops {
		op.Start()
	}
	return nil
}

func (p *AsyncPipeline) Wait() {
	for _, op := range p.ops {
		op.Wait()
		op.Close()
	}
}

func (p *AsyncPipeline) AddOperator(op *AsyncOperator) {
	p.ops = append(p.ops, op)
}

func (p *AsyncPipeline) LastOperator() *AsyncOperator {
	return p.ops[len(p.ops)-1]
}
