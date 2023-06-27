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
	"encoding/binary"
	"fmt"
	kv2 "github.com/pingcap/tidb/br/pkg/lightning/backend/kv"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/local"
	"github.com/pingcap/tidb/br/pkg/lightning/common"
	"github.com/pingcap/tidb/br/pkg/membuf"
	storage2 "github.com/pingcap/tidb/br/pkg/storage"
	"github.com/pingcap/tidb/keyspace"
	"github.com/pingcap/tidb/util/logutil"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"math/rand"
	"strconv"
	"testing"
)

func TestWriter(t *testing.T) {
	bucket := "nfs"
	prefix := "tools_test_data/sharedisk"
	uri := fmt.Sprintf("s3://%s/%s?access-key=%s&secret-access-key=%s&endpoint=http://%s:%s&force-path-style=true",
		bucket, prefix, ak, sak, hostname, port)
	backend, err := storage2.ParseBackend(uri, nil)
	require.NoError(t, err)
	storage, err := storage2.New(context.Background(), backend, &storage2.ExternalStorageOptions{})
	require.NoError(t, err)

	eg := &Engine{rc: &RangePropertiesCollector{}}
	eg.rc.propSizeIdxDistance = 2048
	eg.rc.propKeysIdxDistance = 256
	writer := &Writer{ctx: context.Background(), engine: eg, memtableSizeLimit: 8 * 1024, keyAdapter: &local.NoopKeyAdapter{}}
	writer.tikvCodec = keyspace.CodecV1
	writer.exStorage = storage
	writer.filenamePrefix = "test"
	writeBufferSize = 1024

	pool := membuf.NewPool()
	defer pool.Destroy()
	writer.kvBuffer = pool.NewBuffer()

	ctx := context.Background()
	var kvs []common.KvPair
	value := make([]byte, 128)
	for i := 0; i < 16; i++ {
		binary.BigEndian.PutUint64(value[i*8:], uint64(i))
	}
	for i := 1; i <= 20000; i++ {
		var kv common.KvPair
		kv.Key = make([]byte, 16)
		kv.Val = make([]byte, 128)
		copy(kv.Val, value)
		key := rand.Intn(1000)
		binary.BigEndian.PutUint64(kv.Key, uint64(key))
		binary.BigEndian.PutUint64(kv.Key[8:], uint64(i))
		kvs = append(kvs, kv)
	}
	err = writer.AppendRows(ctx, nil, kv2.MakeRowsFromKvPairs(kvs))
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

	dataReader := DataFileReader{ctx: ctx, name: "test/0", exStorage: storage}
	dataReader.readBuffer = make([]byte, 4096)

	i := 0
	for {
		k, v, err := dataReader.GetNextKV()
		require.NoError(t, err)
		if k == nil && v == nil {
			break
		}
		i++
		logutil.BgLogger().Info("print kv", zap.Any("key", k), zap.Any("value", v))
	}
	require.Equal(t, 20000, i)

	statReader := statFileReader{ctx: ctx, name: "test_stat/0", exStorage: storage}
	statReader.readBuffer = make([]byte, 4096)

	j := 0
	for {
		prop, err := statReader.GetNextProp()
		require.NoError(t, err)
		if prop == nil {
			break
		}
		j++
		logutil.BgLogger().Info("print prop", zap.Any("offset", prop.offset))
	}

}
