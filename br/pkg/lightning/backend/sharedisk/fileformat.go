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

type KeyValueStore struct {
	dataWriter storage.ExternalFileWriter

	rc     *RangePropertiesCollector
	ctx    context.Context
	offset uint64
	keyCnt uint64
}

func Create(ctx context.Context, dataWriter storage.ExternalFileWriter) (*KeyValueStore, error) {
	kvStore := &KeyValueStore{dataWriter: dataWriter, ctx: ctx}
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
			newProp := *s.rc.currProp
			s.rc.props = append(s.rc.props, &newProp)
		}
		s.rc.currProp.Key = key
		s.rc.currProp.offset = s.offset
		s.rc.currProp.rangeOffsets = rangeOffsets{}
	}

	s.rc.lastKey = key
	s.offset += uint64(kvLen)
	s.keyCnt++

	s.rc.currProp.Size += uint64(len(key) + len(value))
	s.rc.currProp.Keys++

	return nil
}

func (s *KeyValueStore) Finish() error {
	return nil
}
