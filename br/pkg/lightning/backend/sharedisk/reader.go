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
	"github.com/pingcap/tidb/util/mathutil"
)

type kvReader struct {
	byteReader *byteReader
	key        []byte
	val        []byte
}

func newKVReader(ctx context.Context, name string, store storage.ExternalStorage, initFileOffset uint64, bufSize int) (*kvReader, error) {
	br, err := newByteReader(ctx, store, name, initFileOffset, bufSize)
	if err != nil {
		return nil, err
	}
	return &kvReader{
		byteReader: br,
		key:        nil,
		val:        nil,
	}, nil
}

func (r *kvReader) nextKV() (key, val []byte, err error) {
	if r.byteReader.eof() {
		return nil, nil, nil
	}
	var lenBuf [8]byte
	err = r.byteReader.fillNext(lenBuf[:])
	if err != nil {
		return nil, nil, err
	}
	keyLen := binary.BigEndian.Uint64(lenBuf[:])
	if cap(r.key) < int(keyLen) {
		r.key = make([]byte, keyLen)
	}
	err = r.byteReader.fillNext(r.key[:keyLen])
	if err != nil {
		return nil, nil, err
	}
	err = r.byteReader.fillNext(lenBuf[:])
	if err != nil {
		return nil, nil, err
	}
	valLen := binary.BigEndian.Uint64(lenBuf[:])
	if cap(r.val) < int(valLen) {
		r.val = make([]byte, valLen)
	}
	err = r.byteReader.fillNext(r.val[:valLen])
	if err != nil {
		return nil, nil, err
	}
	return r.key[:keyLen], r.val[:valLen], nil
}

type statsReader struct {
	byteReader *byteReader
	propBytes  []byte
}

func newStatsReader(ctx context.Context, store storage.ExternalStorage, name string, bufSize int) (*statsReader, error) {
	br, err := newByteReader(ctx, store, name, 0, bufSize)
	if err != nil {
		return nil, err
	}
	return &statsReader{
		byteReader: br,
		propBytes:  nil,
	}, nil
}

func (r *statsReader) nextProp() (*RangeProperty, error) {
	if r.byteReader.eof() {
		return nil, nil
	}
	var lenBuf [4]byte
	err := r.byteReader.fillNext(lenBuf[:])
	if err != nil {
		return nil, err
	}
	propLen := binary.BigEndian.Uint32(lenBuf[:])
	if cap(r.propBytes) < int(propLen) {
		r.propBytes = make([]byte, propLen)
	}
	err = r.byteReader.fillNext(r.propBytes[:propLen])
	if err != nil {
		return nil, err
	}
	return decodeProp(r.propBytes)
}

func decodeProp(data []byte) (*RangeProperty, error) {
	rp := &RangeProperty{}
	keyLen := binary.BigEndian.Uint32(data[0:4])
	rp.Key = data[4 : 4+keyLen]
	rp.Size = binary.BigEndian.Uint64(data[4+keyLen : 12+keyLen])
	rp.Keys = binary.BigEndian.Uint64(data[12+keyLen : 20+keyLen])
	rp.offset = binary.BigEndian.Uint64(data[20+keyLen : 28+keyLen])
	return rp, nil
}

type byteReader struct {
	ctx   context.Context
	name  string
	store storage.ExternalStorage

	buf       []byte
	bufOffset int

	fileStart uint64
	fileMax   uint64

	//prefetchInfo *prefetchInfo
}

func newByteReader(ctx context.Context, store storage.ExternalStorage, name string, initFileOffset uint64, bufSize int) (*byteReader, error) {
	maxOffset, err := storage.GetFileMaxOffset(ctx, store, name)
	if err != nil {
		return nil, err
	}
	br := &byteReader{
		ctx:       ctx,
		name:      name,
		store:     store,
		buf:       make([]byte, bufSize),
		bufOffset: 0,
		fileStart: initFileOffset,
		fileMax:   maxOffset,
	}
	err = br.reload()
	return br, err
}

func (r *byteReader) fillNext(dest []byte) error {
	b := r.next(len(dest))
	copy(dest, b)
	bLen := len(b)
	if bLen < len(dest) {
		err := r.reload()
		if err != nil {
			return err
		}
		rest := r.next(len(dest) - bLen)
		copy(dest[bLen:], rest)
	}
	return nil
}

func (r *byteReader) eof() bool {
	return r.fileStart == r.fileMax && len(r.buf) == r.bufOffset
}

func (r *byteReader) next(n int) []byte {
	end := mathutil.Min(r.bufOffset+n, len(r.buf))
	ret := r.buf[r.bufOffset:end]
	r.bufOffset += len(ret)
	return ret
}

func (r *byteReader) reload() error {
	start := r.fileStart
	end := mathutil.Min(r.fileStart+uint64(len(r.buf)), r.fileMax)
	nBytes, err := storage.ReadPartialFileDirectly(r.ctx, r.store, r.name, start, end, r.buf)
	if err != nil {
		return err
	}
	r.fileStart += nBytes
	r.bufOffset = 0
	if nBytes < uint64(len(r.buf)) {
		// The last batch.
		r.buf = r.buf[:nBytes]
	}
	return nil
}
