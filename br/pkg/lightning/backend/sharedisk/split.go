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

import (
	"encoding/hex"
	"strings"

	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
	"go.uber.org/zap"
)

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

func (r *RangeSplitter) SplitOne() (kv.Key, []string, []string, error) {
	var curSize, curKeys uint64
	curWays := make(map[int]struct{})
	dataFiles := make([]string, 0, 16)
	statsFiles := make([]string, 0, 16)
	var curKey kv.Key
	var curFileIdx int
	for r.propIter.Next() {
		if err := r.propIter.Error(); err != nil {
			return nil, nil, nil, err
		}
		if curKey.Cmp(r.propIter.currProp.p.Key) > 0 {
			logutil.BgLogger().Info("unexpected descending key",
				zap.String("prev", hex.EncodeToString(curKey)),
				zap.Int("prevFileIdx", curFileIdx),
				zap.String("curr", hex.EncodeToString(r.propIter.currProp.p.Key)))
		}
		curKey = kv.Key(r.propIter.currProp.p.Key).Clone()
		curFileIdx = r.propIter.currProp.fileOffset
		prop := r.propIter.currProp.p
		curSize += prop.rangeOffsets.Size
		curKeys += prop.rangeOffsets.Keys
		fileOffset := r.propIter.currProp.fileOffset
		if _, found := curWays[fileOffset]; !found {
			curWays[fileOffset] = struct{}{}
			statFile := r.propIter.statFilePaths[fileOffset]
			statsFiles = append(statsFiles, statFile)
			dataFiles = append(dataFiles, strings.Replace(statFile, "_stat", "", 1))
		}
		if curSize >= r.maxSize || curKeys >= r.maxKeys || uint64(len(curWays)) >= r.maxWays {
			return prop.Key, dataFiles, statsFiles, nil
		}
	}
	return nil, dataFiles, statsFiles, nil
}

func (r *RangeSplitter) FileCount() int {
	return len(r.propIter.statFileReader)
}
