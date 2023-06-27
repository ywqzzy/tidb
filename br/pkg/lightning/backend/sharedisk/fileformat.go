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
	"github.com/pingcap/tidb/br/pkg/storage"
)

var writeBufferSize = 8192 * 1024

type KeyValueStore struct {
	dataWriter storage.ExternalFileWriter
	statWriter storage.ExternalFileWriter

	rc           *RangePropertiesCollector
	ctx          context.Context
	writeBuffer  []byte
	statBuffer   []byte
	bufferOffset int
	offset       uint64
	keyCnt       uint64
}

func Create(ctx context.Context, dataWriter, statWriter storage.ExternalFileWriter) (*KeyValueStore, error) {
	kvStore := &KeyValueStore{dataWriter: dataWriter, statWriter: statWriter, ctx: ctx}
	kvStore.writeBuffer = make([]byte, writeBufferSize)
	return kvStore, nil
}

func (s *KeyValueStore) AddKeyValue(key, value []byte) error {
	kvLen := len(key) + len(value) + 16

	_, err := s.dataWriter.Write(s.ctx, binary.BigEndian.AppendUint64(nil, uint64(len(key))))
	if err != nil {
		return err
	}
	_, err = s.dataWriter.Write(s.ctx, key)
	if err != nil {
		return err
	}
	_, err = s.dataWriter.Write(s.ctx, binary.BigEndian.AppendUint64(nil, uint64(len(value))))
	if err != nil {
		return err
	}
	_, err = s.dataWriter.Write(s.ctx, value)
	if err != nil {
		return err
	}

	if len(s.rc.lastKey) == 0 || s.rc.currProp.Size >= s.rc.propSizeIdxDistance ||
		s.rc.currProp.Keys >= s.rc.propKeysIdxDistance {
		if len(s.rc.lastKey) != 0 {
			s.rc.props = append(s.rc.props, s.rc.currProp)
		}
		s.rc.currProp = &RangeProperty{
			Key:    key,
			offset: s.offset,
			rangeOffsets: rangeOffsets{
				Size: 0,
				Keys: 0,
			},
		}
	}

	s.rc.lastKey = key
	s.bufferOffset += kvLen
	s.offset += uint64(kvLen)
	s.keyCnt++

	s.rc.currProp.Size += uint64(len(key) + len(value))
	s.rc.currProp.Keys++

	return nil
}

func (s *KeyValueStore) Finish() error {
	if s.rc.currProp.Keys > 0 {
		s.rc.props = append(s.rc.props, s.rc.currProp)
	}
	_, err := s.statWriter.Write(s.ctx, s.rc.Encode())
	if err != nil {
		return err
	}
	return s.statWriter.Close(s.ctx)
}
