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
	"strings"

	"github.com/pingcap/tidb/kv"
)

type RangeSplitter struct {
	maxSize   uint64
	maxKeys   uint64
	maxWays   uint64
	propIter  *MergePropIter
	dataFiles []string
	exhausted bool
	curWays   map[int]struct{}
}

func NewRangeSplitter(maxSize, maxKeys, maxWays uint64, propIter *MergePropIter, dataFiles []string) *RangeSplitter {
	return &RangeSplitter{
		maxSize:   maxSize,
		maxKeys:   maxKeys,
		maxWays:   maxWays,
		propIter:  propIter,
		dataFiles: dataFiles,
		curWays:   make(map[int]struct{}),
	}
}

func (r *RangeSplitter) SplitOne() (kv.Key, []string, []string, error) {
	if r.exhausted {
		return nil, nil, nil, nil
	}
	var curSize, curKeys uint64
	var lastFileIdx int
	var lastWays int
	var exhaustedFileIdx []int
	for r.propIter.Next() {
		if err := r.propIter.Error(); err != nil {
			return nil, nil, nil, err
		}
		prop := r.propIter.currProp.p
		curSize += prop.rangeOffsets.Size
		curKeys += prop.rangeOffsets.Keys
		fileIdx := r.propIter.currProp.fileOffset
		ways := r.propIter.propHeap.Len()
		if ways < lastWays {
			exhaustedFileIdx = append(exhaustedFileIdx, lastFileIdx)
		}
		r.curWays[fileIdx] = struct{}{}
		if curSize >= r.maxSize || curKeys >= r.maxKeys || uint64(len(r.curWays)) >= r.maxWays {
			dataFiles, statsFiles := r.collectFiles()
			for _, idx := range exhaustedFileIdx {
				delete(r.curWays, idx)
			}
			return prop.Key, dataFiles, statsFiles, nil
		}
		lastFileIdx = fileIdx
		lastWays = ways
	}
	dataFiles, statsFiles := r.collectFiles()
	return nil, dataFiles, statsFiles, r.propIter.Error()
}

func (r *RangeSplitter) collectFiles() (data []string, stats []string) {
	dataFiles := make([]string, 0, len(r.curWays))
	statsFiles := make([]string, 0, len(r.curWays))
	for fileIdx := range r.curWays {
		statFile := r.propIter.statFilePaths[fileIdx]
		statsFiles = append(statsFiles, statFile)
		dataFiles = append(dataFiles, strings.Replace(statFile, "_stat", "", 1))
	}
	return dataFiles, statsFiles
}
