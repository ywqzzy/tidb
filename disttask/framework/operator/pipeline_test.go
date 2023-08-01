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
	"testing"

	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
)

func NewSimplePipeline1() *Pipeline {
	source1 := &SimpleDataSource{0}
	channel := &SimpleDataChannel{nil, false}
	sink2 := &SimpleDataSink{0, 0}

	op1 := &Operator{
		source1,
		channel,
		&SimpleOperatorImpl{},
	}
	op2 := &Operator{
		channel,
		sink2,
		&SimpleOperatorImpl{},
	}

	pipeline := &Pipeline{}
	pipeline.AddOperator(op1)
	pipeline.AddOperator(op2)

	return pipeline
}

func TestPipelineBasic(t *testing.T) {
	pipeline := NewSimplePipeline1()
	for {
		hasNext, err := pipeline.Execute()
		require.NoError(t, err)
		if !hasNext {
			break
		}
	}
	// check 30
	require.Equal(t, 30, pipeline.LastOperator().GetSink().(*SimpleDataSink).Res)
	logutil.BgLogger().Info(pipeline.Display())
}
