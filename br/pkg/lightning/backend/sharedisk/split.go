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

package sharedisk

import "github.com/pingcap/tidb/kv"

type RangeSplitter struct {
	maxSize  uint64
	maxKeys  uint64
	maxWays  uint64
	propIter *MergePropIter
}

func NewRangeSplitter(maxSize, maxKeys, maxWays uint64, propIter *MergePropIter) *RangeSplitter {
	return &RangeSplitter{
		maxSize:  maxSize,
		maxKeys:  maxKeys,
		maxWays:  maxWays,
		propIter: propIter,
	}
}

func (r *RangeSplitter) SplitOne() kv.Key {
	var curSize, curKeys uint64
	curWays := make(map[int]struct{})
	for r.propIter.Next() {
		prop := r.propIter.currProp.p
		curSize += prop.rangeOffsets.Size
		curKeys += prop.rangeOffsets.Keys
		curWays[r.propIter.currProp.fileOffset] = struct{}{}
		if curSize >= r.maxSize || curKeys >= r.maxKeys || uint64(len(curWays)) >= r.maxWays {
			return prop.Key
		}
	}
	return nil
}

func (r *RangeSplitter) FileCount() int {
	return len(r.propIter.statFileReader)
}
