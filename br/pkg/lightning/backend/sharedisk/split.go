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
	"context"
	"strings"

	"github.com/pingcap/tidb/br/pkg/storage"
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

func (r *RangeSplitter) FirstStartKey(ctx context.Context) (kv.Key, error) {
	offsets := make([]uint64, len(r.dataFiles))
	iter, err := NewMergeIter(ctx, r.dataFiles, offsets, r.propIter.statFileReader[0].exStorage, 4096)
	if err != nil {
		return nil, err
	}
	if iter.Next() {
		return iter.Key(), nil
	}
	return nil, nil
}

func (r *RangeSplitter) SplitOne(ctx context.Context) (kv.Key, []string, []string, error) {
	if r.exhausted {
		return nil, nil, nil, nil
	}
	var curSize, curKeys uint64
	var lastFileIdx int
	var lastOffset uint64
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
		lastOffset = prop.offset
		lastWays = ways
	}
	if err := r.propIter.Error(); err != nil {
		return nil, nil, nil, err
	}
	exStorage := r.propIter.statFileReader[0].exStorage
	maxKey, err := findMaxKey(ctx, r.dataFiles[lastFileIdx], lastOffset, exStorage)
	r.exhausted = true
	dataFiles, statsFiles := r.collectFiles()
	return maxKey, dataFiles, statsFiles, err
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

func findMaxKey(ctx context.Context, lastFile string, offset uint64, exStorage storage.ExternalStorage) (kv.Key, error) {
	maxKeyFile := []string{lastFile}
	startOffset := []uint64{offset}
	iter, err := NewMergeIter(ctx, maxKeyFile, startOffset, exStorage, 4096)
	if err != nil {
		return nil, err
	}
	var maxKey kv.Key
	for iter.Next() {
		if err := iter.Error(); err != nil {
			return nil, err
		}
		maxKey = iter.Key()
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	return maxKey, nil
}

func (r *RangeSplitter) FileCount() int {
	return len(r.propIter.statFileReader)
}
