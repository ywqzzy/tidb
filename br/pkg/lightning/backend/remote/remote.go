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

package remote

import (
	"context"
	"fmt"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/pingcap/tidb/br/pkg/lightning/backend"
	"github.com/pingcap/tidb/br/pkg/lightning/backend/sharedisk"
	"github.com/pingcap/tidb/br/pkg/storage"
)

const defaultRetryBackoffTime = 3 * time.Second

type Config struct {
	Bucket          string
	Prefix          string
	AccessKey       string
	SecretAccessKey string
	Host            string
	Port            string
}

func NewRemoteBackend(ctx context.Context, cfg *Config) (backend.Backend, error) {
	uri := fmt.Sprintf("s3://%s/%s?access-key=%s&secret-access-key=%s&endpoint=http://%s:%s&force-path-style=true",
		cfg.Bucket, cfg.Prefix, cfg.AccessKey, cfg.SecretAccessKey, cfg.Host, cfg.Port)
	bd, err := storage.ParseBackend(uri, nil)
	if err != nil {
		return nil, err
	}
	extStore, err := storage.New(ctx, bd, &storage.ExternalStorageOptions{})
	return &remoteBackend{
		externalStorage: extStore,
		mu: struct {
			sync.RWMutex
			maxWriterID int
			writersSeq  map[int]int
		}{},
	}, nil
}

type remoteBackend struct {
	externalStorage storage.ExternalStorage
	mu              struct {
		sync.RWMutex
		maxWriterID int
		writersSeq  map[int]int
	}
}

func (r *remoteBackend) Close() {
}

func (r *remoteBackend) RetryImportDelay() time.Duration {
	return defaultRetryBackoffTime
}

func (r *remoteBackend) ShouldPostProcess() bool {
	return true
}

func (r *remoteBackend) OpenEngine(ctx context.Context, config *backend.EngineConfig, engineUUID uuid.UUID) error {
	return nil
}

func (r *remoteBackend) CloseEngine(ctx context.Context, config *backend.EngineConfig, engineUUID uuid.UUID) error {
	return nil
}

func (r *remoteBackend) ImportEngine(ctx context.Context, engineUUID uuid.UUID, regionSplitSize, regionSplitKeys int64) error {
	return nil
}

func (r *remoteBackend) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	for wid, seq := range r.mu.writersSeq {
		filePrefix := fmt.Sprintf("%s_%d", engineUUID.String(), wid)
		for i := 1; i <= seq; i++ {
			dataPath := filepath.Join(filePrefix, strconv.Itoa(i))
			err := r.externalStorage.DeleteFile(ctx, dataPath)
			if err != nil {
				return err
			}
			statPath := filepath.Join(filePrefix+"_stat", strconv.Itoa(i))
			err = r.externalStorage.DeleteFile(ctx, statPath)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *remoteBackend) FlushEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return nil
}

func (r *remoteBackend) FlushAllEngines(ctx context.Context) error {
	return nil
}

func (r *remoteBackend) ResetEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return nil
}

func (r *remoteBackend) LocalWriter(ctx context.Context, cfg *backend.LocalWriterConfig, engineUUID uuid.UUID) (backend.EngineWriter, error) {
	onClose := func(writerID, currentSeq int) {
		r.saveWriterSeq(writerID, currentSeq)
	}
	return sharedisk.NewWriter(ctx, r.externalStorage, engineUUID, r.allocWriterID(), onClose), nil
}

func (r *remoteBackend) allocWriterID() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	seq := r.mu.maxWriterID
	r.mu.maxWriterID++
	return seq
}

// saveWriterSeq stores the current writer sequence for the given writer so that
// we can remove the correct file in external storage when cleaning up.
func (r *remoteBackend) saveWriterSeq(writerID int, currentSeq int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.writersSeq[writerID] = currentSeq
}
