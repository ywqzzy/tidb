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
	"context"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/encode"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/membuf"
	"github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/keyspace"
	tidbkv "github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/tikv/client-go/v2/tikv"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

var ReadByteForTest atomic.Uint64
var ReadTimeForTest atomic.Uint64
var ReadIOCnt atomic.Uint64

type rangeOffsets struct {
	Size uint64
	Keys uint64
}

type RangeProperty struct {
	Key    []byte
	offset uint64
	rangeOffsets
}

// RangePropertiesCollector collects range properties for each range.
type RangePropertiesCollector struct {
	props               []*RangeProperty
	currProp            *RangeProperty
	lastOffsets         rangeOffsets
	lastKey             []byte
	currentOffsets      rangeOffsets
	propSizeIdxDistance uint64
	propKeysIdxDistance uint64
}

func (rc *RangePropertiesCollector) reset() {
	rc.props = rc.props[:0]
	rc.currProp = &RangeProperty{
		rangeOffsets: rangeOffsets{},
	}
	rc.lastOffsets = rangeOffsets{}
	rc.lastKey = nil
	rc.currentOffsets = rangeOffsets{}
}

func (rc *RangePropertiesCollector) Encode() []byte {
	b := make([]byte, 0, 1024)
	idx := 0
	for _, p := range rc.props {
		// Size.
		b = append(b, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(b[idx:], uint32(28+len(p.Key)))
		idx += 4

		b = append(b, 0, 0, 0, 0)
		binary.BigEndian.PutUint32(b[idx:], uint32(len(p.Key)))
		idx += 4
		b = append(b, p.Key...)
		idx += len(p.Key)

		b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(b[idx:], p.Size)
		idx += 8

		b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(b[idx:], p.Keys)
		idx += 8

		b = append(b, 0, 0, 0, 0, 0, 0, 0, 0)
		binary.BigEndian.PutUint64(b[idx:], p.offset)
		idx += 8
	}
	return b
}

func NewEngine(sizeDist, keyDist uint64) *Engine {
	return &Engine{
		rc: &RangePropertiesCollector{
			// TODO(tangenta): decide the preserved size of props.
			props:               nil,
			currProp:            &RangeProperty{},
			propSizeIdxDistance: sizeDist,
			propKeysIdxDistance: keyDist,
		},
	}
}

type Engine struct {
	rc *RangePropertiesCollector
}

var WriteBatchSize = 8 * 1024
var MemQuota = 1024 * 1024 * 1024

type OnCloseFunc func(writerID int, seq int, min tidbkv.Key, max tidbkv.Key)

func DummyOnCloseFunc(int, int, tidbkv.Key, tidbkv.Key) {}

func NewWriter(ctx context.Context, externalStorage storage.ExternalStorage,
	prefix string, writerID int, onClose OnCloseFunc) *Writer {
	// TODO(tangenta): make it configurable.
	engine := NewEngine(2048, 256)
	pool := membuf.NewPool()
	filePrefix := filepath.Join(prefix, strconv.Itoa(writerID))
	return &Writer{
		ctx:               ctx,
		engine:            engine,
		memtableSizeLimit: MemQuota,
		keyAdapter:        &local.NoopKeyAdapter{},
		exStorage:         externalStorage,
		memBufPool:        pool,
		kvBuffer:          pool.NewBuffer(),
		writeBatch:        make([]common.KvPair, 0, WriteBatchSize),
		currentSeq:        0,
		tikvCodec:         keyspace.CodecV1,
		filenamePrefix:    filePrefix,
		writerID:          writerID,
		kvStore:           nil,
		onClose:           onClose,
		closed:            false,
	}
}

// Writer is used to write data into external storage.
type Writer struct {
	ctx context.Context
	sync.Mutex
	engine            *Engine
	memtableSizeLimit int
	keyAdapter        local.KeyAdapter
	exStorage         storage.ExternalStorage

	// bytes buffer for writeBatch
	memBufPool *membuf.Pool
	kvBuffer   *membuf.Buffer
	writeBatch []common.KvPair
	batchSize  int

	currentSeq int
	onClose    OnCloseFunc
	closed     bool

	tikvCodec      tikv.Codec
	filenamePrefix string
	writerID       int
	minKey         tidbkv.Key
	maxKey         tidbkv.Key

	kvStore *KeyValueStore
}

// AppendRows appends rows to the external storage.
func (w *Writer) AppendRows(ctx context.Context, columnNames []string, rows encode.Rows) error {
	kvs := kv.Rows2KvPairs(rows)
	if len(kvs) == 0 {
		return nil
	}

	//if w.engine.closed.Load() {
	//	return errorEngineClosed
	//}

	for i := range kvs {
		kvs[i].Key = w.tikvCodec.EncodeKey(kvs[i].Key)
	}

	w.Lock()
	defer w.Unlock()

	keyAdapter := w.keyAdapter
	for _, pair := range kvs {
		w.batchSize += len(pair.Key) + len(pair.Val)
		buf := w.kvBuffer.AllocBytes(keyAdapter.EncodedLen(pair.Key, pair.RowID))
		key := keyAdapter.Encode(buf[:0], pair.Key, pair.RowID)
		val := w.kvBuffer.AddBytes(pair.Val)
		w.writeBatch = append(w.writeBatch, common.KvPair{Key: key, Val: val})
		if w.batchSize >= w.memtableSizeLimit {
			if err := w.flushKVs(ctx); err != nil {
				return err
			}
			w.writeBatch = w.writeBatch[:0]
			w.kvBuffer.Reset()
			w.batchSize = 0
		}
	}

	return nil
}

func (w *Writer) IsSynced() bool {
	return false
}

func (w *Writer) Close(ctx context.Context) (backend.ChunkFlushStatus, error) {
	if w.closed {
		return status(true), nil
	}
	logutil.BgLogger().Info("close writer", zap.Int("writerID", w.writerID),
		zap.String("minKey", hex.EncodeToString(w.minKey)), zap.String("maxKey", hex.EncodeToString(w.maxKey)))
	w.closed = true
	defer w.memBufPool.Destroy()
	err := w.flushKVs(ctx)
	if err != nil {
		return status(false), err
	}
	err = w.kvStore.Finish()
	if err != nil {
		return status(false), err
	}
	w.onClose(w.writerID, w.currentSeq, w.minKey, w.maxKey)
	return status(true), nil
}

func (w *Writer) recordMinMax(newMin, newMax tidbkv.Key) {
	if len(w.minKey) == 0 || newMin.Cmp(w.minKey) < 0 {
		w.minKey = newMin.Clone()
	}
	if len(w.maxKey) == 0 || newMax.Cmp(w.maxKey) > 0 {
		w.maxKey = newMax.Clone()
	}
}

type status bool

func (s status) Flushed() bool {
	return bool(s)
}

func CheckDataCnt(file string, exStorage storage.ExternalStorage) error {
	iter, err := NewMergeIter(context.Background(), []string{file}, []uint64{0}, exStorage, 4096)
	if err != nil {
		return err
	}
	var cnt int
	var firstKey, lastKey tidbkv.Key
	for iter.Next() {
		cnt++
		if len(firstKey) == 0 {
			firstKey = iter.Key()
			firstKey = firstKey.Clone()
		}
		lastKey = iter.Key()
	}
	lastKey = lastKey.Clone()
	logutil.BgLogger().Info("check data cnt", zap.Int("cnt", cnt),
		zap.Strings("name", PrettyFileNames([]string{file})),
		zap.String("first", hex.EncodeToString(firstKey)), zap.String("last", hex.EncodeToString(lastKey)))
	return iter.Error()
}

func (w *Writer) flushKVs(ctx context.Context) error {
	dataWriter, statWriter, err := w.createStorageWriter()
	if err != nil {
		return err
	}
	//defer func() {
	//	err := CheckDataCnt(filepath.Join(w.filenamePrefix, strconv.Itoa(w.currentSeq-1)), w.exStorage)
	//	if err != nil {
	//		logutil.BgLogger().Error("check data cnt failed", zap.Error(err))
	//	}
	//}()
	defer func() {
		dataWriter.Close(w.ctx)
		statWriter.Close(w.ctx)
	}()
	w.currentSeq++

	slices.SortFunc(w.writeBatch, func(i, j common.KvPair) bool {
		return bytes.Compare(i.Key, j.Key) < 0
	})

	w.kvStore, err = Create(w.ctx, dataWriter, statWriter)
	w.kvStore.rc = w.engine.rc

	for i := 0; i < len(w.writeBatch); i++ {
		err = w.kvStore.AddKeyValue(w.writeBatch[i].Key, w.writeBatch[i].Val)
		if err != nil {
			return err
		}
	}

	if w.engine.rc.currProp.Keys > 0 {
		newProp := *w.engine.rc.currProp
		w.engine.rc.props = append(w.engine.rc.props, &newProp)
	}
	_, err = statWriter.Write(w.ctx, w.engine.rc.Encode())
	if err != nil {
		return err
	}

	w.recordMinMax(w.writeBatch[0].Key, w.writeBatch[len(w.writeBatch)-1].Key)

	w.engine.rc.reset()
	return nil
}

func (w *Writer) createStorageWriter() (storage.ExternalFileWriter, storage.ExternalFileWriter, error) {
	dataPath := filepath.Join(w.filenamePrefix, strconv.Itoa(w.currentSeq))
	statPath := filepath.Join(w.filenamePrefix+"_stat", strconv.Itoa(w.currentSeq))
	dataWriter, err := w.exStorage.Create(w.ctx, dataPath)
	logutil.BgLogger().Debug("new data writer", zap.Any("name", dataPath))
	if err != nil {
		return nil, nil, err
	}
	statWriter, err := w.exStorage.Create(w.ctx, statPath)
	logutil.BgLogger().Debug("new stat writer", zap.Any("name", statPath))
	if err != nil {
		return nil, nil, err
	}
	return dataWriter, statWriter, nil
}

func PrettyFileNames(files []string) []string {
	names := make([]string, 0, len(files))
	for _, f := range files {
		dir, file := filepath.Split(f)
		names = append(names, fmt.Sprintf("%s/%s", filepath.Base(dir), file))
	}
	return names
}
