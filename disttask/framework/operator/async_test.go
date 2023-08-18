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

	poolutil "github.com/pingcap/tidb/resourcemanager/util"
	"github.com/stretchr/testify/require"
)

func TestPipelineAsyncBasic(t *testing.T) {
	src := newExampleSource(20)
	op0 := newExampleOperator("op0", poolutil.DDL, 10)
	sink := newExampleSink(0)

	comp := &ComposeOperator{}
	Compose[asyncChunk](comp, src, op0)
	Compose[asyncChunk](comp, op0, sink)

	pipeline := NewAsyncPipeline(comp, src, op0, sink)
	err := pipeline.Execute()
	require.NoError(t, err)
	pipeline.Close()
	require.Equal(t, "ComposeOperator\n simpleDataSource[asyncChunk]\n  AsyncOperator[operator.asyncChunk, operator.asyncChunk]\n   simpleDataSink[asyncChunk]", pipeline.Display())
	require.Equal(t, 20, sink.Res)
}
