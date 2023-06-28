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
	"bytes"
	"container/heap"
	"context"
	sst "github.com/pingcap/kvproto/pkg/import_sstpb"
	"github.com/pingcap/tidb/br/pkg/storage"
)

type kvPair struct {
	key        []byte
	value      []byte
	fileOffset int
}

type kvPairHeap []*kvPair

func (h kvPairHeap) Len() int {
	return len(h)
}

func (h kvPairHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].key, h[j].key) < 0
}

func (h kvPairHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *kvPairHeap) Push(x interface{}) {
	*h = append(*h, x.(*kvPair))
}

func (h kvPairHeap) Pop() interface{} {
	old := h
	n := len(old)
	x := old[n-1]
	h = old[0 : n-1]
	return x
}

type mergeIter struct {
	startKey       []byte
	endKey         []byte
	dataFilePaths  []string
	dataFileReader []*DataFileReader
	exStorage      storage.ExternalStorage
	kvHeap         kvPairHeap
	currKV         *kvPair

	firstKey []byte

	err error
}

func NewMergeIter(ctx context.Context, startKey, endKey []byte, paths []string, exStorage storage.ExternalStorage) (*mergeIter, error) {
	it := &mergeIter{
		startKey:      startKey,
		endKey:        endKey,
		dataFilePaths: paths,
	}
	it.dataFileReader = make([]*DataFileReader, 0, len(paths))
	it.kvHeap = make([]*kvPair, 0, len(paths))
	for i, path := range paths {
		rd := DataFileReader{ctx: ctx, name: path}
		it.dataFileReader = append(it.dataFileReader, &rd)
		k, v, err := rd.GetNextKV()
		if err != nil {
			return nil, err
		}
		if len(k) == 0 {
			continue
		}
		pair := kvPair{key: k, value: v, fileOffset: i}
		it.kvHeap.Push(&pair)
	}
	heap.Init(&it.kvHeap)
	return it, nil
}

func (i *mergeIter) Seek(key []byte) bool {
	// Don't support.
	return false
}

func (i *mergeIter) Error() error {
	return i.err
}

func (i *mergeIter) First() bool {
	// Don't support.
	return false
}

func (i *mergeIter) Last() bool {
	// Don't support.
	return false
}

func (i *mergeIter) Valid() bool {
	return i.currKV != nil
}

func (i *mergeIter) Next() bool {
	if i.kvHeap.Len() == 0 {
		return false
	}
	i.currKV = heap.Pop(&i.kvHeap).(*kvPair)
	k, v, err := i.dataFileReader[i.currKV.fileOffset].GetNextKV()
	if err != nil {
		i.err = err
		return false
	}
	if len(k) > 0 {
		heap.Push(&i.kvHeap, &kvPair{k, v, i.currKV.fileOffset})
	}

	return true
}

func (i *mergeIter) Key() []byte {
	return i.currKV.key
}

func (i *mergeIter) Value() []byte {
	return i.currKV.value
}

func (i *mergeIter) Close() []byte {
	return i.firstKey
}

func (i *mergeIter) OpType() sst.Pair_OP {
	return sst.Pair_Put
}
