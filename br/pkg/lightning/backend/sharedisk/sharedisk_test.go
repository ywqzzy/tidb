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
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	kv2 "github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/membuf"
	storage2 "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

//func TestWriter(t *testing.T) {
//	bucket := "nfs"
//	prefix := "tools_test_data/sharedisk"
//	uri := fmt.Sprintf("s3://%s/%s?access-key=%s&secret-access-key=%s&endpoint=http://%s:%s&force-path-style=true",
//		bucket, prefix, "minioadmin", "minioadmin", "127.0.0.1", "9000")
//	backend, err := storage2.ParseBackend(uri, nil)
//	require.NoError(t, err)
//	storage, err := storage2.New(context.Background(), backend, &storage2.ExternalStorageOptions{})
//	require.NoError(t, err)
//
//	writer := NewWriter(context.Background(), storage, "test", 0, func(int, int) {})
//	writer.filenamePrefix = "test"
//	writeBufferSize = 1024
//
//	pool := membuf.NewPool()
//	defer pool.Destroy()
//	writer.kvBuffer = pool.NewBuffer()
//
//	ctx := context.Background()
//	var kvs []common.KvPair
//	value := make([]byte, 128)
//	for i := 0; i < 16; i++ {
//		binary.BigEndian.PutUint64(value[i*8:], uint64(i))
//	}
//	for i := 1; i <= 20000; i++ {
//		var kv common.KvPair
//		kv.Key = make([]byte, 16)
//		kv.Val = make([]byte, 128)
//		copy(kv.Val, value)
//		key := rand.Intn(10000000)
//		binary.BigEndian.PutUint64(kv.Key, uint64(key))
//		binary.BigEndian.PutUint64(kv.Key[8:], uint64(i))
//		kvs = append(kvs, kv)
//	}
//	err = writer.AppendRows(ctx, nil, kv2.MakeRowsFromKvPairs(kvs))
//	err = writer.flushKVs(context.Background())
//	require.NoError(t, err)
//	err = writer.kvStore.Finish()
//	require.NoError(t, err)
//
//	logutil.BgLogger().Info("writer info", zap.Any("seq", writer.currentSeq))
//
//	defer func() {
//		for i := 0; i < writer.currentSeq; i++ {
//			storage.DeleteFile(ctx, "test/"+strconv.Itoa(i))
//			storage.DeleteFile(ctx, "test_stat/"+strconv.Itoa(i))
//		}
//	}()
//
//	i := 0
//	for _, fileName := range []string{"test/0", "test/1", "test/2"} {
//		dataReader := DataFileReader{ctx: ctx, name: fileName, exStorage: storage}
//		dataReader.readBuffer = make([]byte, 4096)
//
//		for {
//			k, v, err := dataReader.GetNextKV()
//			require.NoError(t, err)
//			if k == nil && v == nil {
//				break
//			}
//			i++
//			//logutil.BgLogger().Info("print kv", zap.Any("key", k), zap.Any("value", v))
//		}
//	}
//	logutil.BgLogger().Info("flush cnt", zap.Any("cnt", writer.currentSeq+1))
//
//	require.Equal(t, 20000, i)
//
//	statReader := statFileReader{ctx: ctx, name: "test_stat/2", exStorage: storage}
//	statReader.readBuffer = make([]byte, 4096)
//
//	j := 0
//	for {
//		prop, err := statReader.GetNextProp()
//		require.NoError(t, err)
//		if prop == nil {
//			break
//		}
//		j++
//		logutil.BgLogger().Info("print prop", zap.Any("offset", prop.offset))
//	}
//
//	dataFileName := make([]string, 0)
//	fileStartOffsets := make([]uint64, 0)
//	for i := 0; i < writer.currentSeq; i++ {
//		dataFileName = append(dataFileName, "test/"+strconv.Itoa(i))
//		fileStartOffsets = append(fileStartOffsets, 0)
//	}
//	mIter, err := NewMergeIter(ctx, dataFileName, fileStartOffsets, storage, 4096)
//	require.NoError(t, err)
//	mCnt := 0
//	var prevKey []byte
//	for mIter.Next() {
//		mCnt++
//		if len(prevKey) > 0 {
//			currKey := mIter.Key()
//			require.Equal(t, 1, bytes.Compare(currKey, prevKey))
//			prevKey = currKey
//		}
//	}
//	require.Equal(t, 20000, mCnt)
//}

func randomString(n int) string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)
	for i := range bytes {
		bytes[i] = alphanum[rand.Intn(len(alphanum))]
	}
	return string(bytes)
}

func TestWriterPerf(t *testing.T) {
	var keySize = 256
	var valueSize = 1000
	var rowCnt = 50000
	var readBufferSize = 64 * 1024 * 1024
	var memLimit = 64 * 1024 * 1024
	MemQuota = memLimit

	bucket := "globalsorttest"
	prefix := "tools_test_data/sharedisk"
	//uri := fmt.Sprintf("s3://%s/%s&force-path-style=true",
	//	bucket, prefix)
	uri := fmt.Sprintf("s3://%s/%s?access-key=%s&secret-access-key=%s&endpoint=http://%s:%s&force-path-style=true",
		bucket, prefix, "minioadmin", "minioadmin", "127.0.0.1", "9000")
	backend, err := storage2.ParseBackend(uri, nil)
	require.NoError(t, err)
	storage, err := storage2.New(context.Background(), backend, &storage2.ExternalStorageOptions{})
	require.NoError(t, err)

	writer := NewWriter(context.Background(), storage, "test", 0, func(int, int) {})
	writer.filenamePrefix = "test"
	writeBufferSize = 1024

	pool := membuf.NewPool()
	defer pool.Destroy()
	writer.kvBuffer = pool.NewBuffer()

	ctx := context.Background()
	for i := 0; i < rowCnt; i += 10000 {
		var kvs []common.KvPair
		for j := 0; j < 10000; j++ {
			var kv common.KvPair
			kv.Key = []byte(randomString(keySize))
			kv.Val = []byte(randomString(valueSize))
			kvs = append(kvs, kv)
		}
		err = writer.AppendRows(ctx, nil, kv2.MakeRowsFromKvPairs(kvs))
	}
	err = writer.flushKVs(context.Background())
	require.NoError(t, err)
	err = writer.kvStore.Finish()
	require.NoError(t, err)

	logutil.BgLogger().Info("writer info", zap.Any("seq", writer.currentSeq))

	defer func() {
		for i := 0; i < writer.currentSeq; i++ {
			storage.DeleteFile(ctx, "test/"+strconv.Itoa(i))
			storage.DeleteFile(ctx, "test_stat/"+strconv.Itoa(i))
		}
	}()

	dataFileName := make([]string, 0)
	fileStartOffsets := make([]uint64, 0)
	for i := 0; i < writer.currentSeq; i++ {
		dataFileName = append(dataFileName, "test/"+strconv.Itoa(i))
		fileStartOffsets = append(fileStartOffsets, 0)
	}

	startTs := time.Now()
	mIter, err := NewMergeIter(ctx, dataFileName, fileStartOffsets, storage, uint64(readBufferSize))
	require.NoError(t, err)
	mCnt := 0
	prevKey := make([]byte, 0, keySize)
	for mIter.Next() {
		mCnt++
		if len(prevKey) > 0 {
			currKey := mIter.Key()
			require.Equal(t, 1, bytes.Compare(currKey, prevKey))
			copy(prevKey, currKey)
		}
	}
	require.Equal(t, rowCnt, mCnt)
	logutil.BgLogger().Info("read data rate", zap.Any("sort total/ ms", time.Since(startTs).Milliseconds()), zap.Any("io cnt", ReadIOCnt.Load()), zap.Any("bytes", ReadByteForTest.Load()), zap.Any("time", ReadTimeForTest.Load()), zap.Any("rate: m/s", ReadByteForTest.Load()*1000000.0/ReadTimeForTest.Load()/1024.0/1024.0))
}
