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
	"math"
	"path/filepath"
	"strconv"
	"strings"
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

func NewRemoteBackend(ctx context.Context, cfg *Config, jobID int64) (backend.Backend, error) {
	uri := fmt.Sprintf("s3://%s/%s?access-key=%s&secret-access-key=%s&endpoint=http://%s:%s&force-path-style=true",
		cfg.Bucket, cfg.Prefix, cfg.AccessKey, cfg.SecretAccessKey, cfg.Host, cfg.Port)
	bd, err := storage.ParseBackend(uri, nil)
	if err != nil {
		return nil, err
	}
	extStore, err := storage.New(ctx, bd, &storage.ExternalStorageOptions{})
	return &Backend{
		jobID:           jobID,
		externalStorage: extStore,
		mu: struct {
			sync.RWMutex
			maxWriterID int
			writersSeq  map[int]int
		}{},
	}, nil
}

type Backend struct {
	jobID           int64
	externalStorage storage.ExternalStorage
	mu              struct {
		sync.RWMutex
		maxWriterID int
		writersSeq  map[int]int
	}
}

func (r *Backend) Close() {
}

func (r *Backend) RetryImportDelay() time.Duration {
	return defaultRetryBackoffTime
}

func (r *Backend) ShouldPostProcess() bool {
	return true
}

func (r *Backend) OpenEngine(ctx context.Context, config *backend.EngineConfig, engineUUID uuid.UUID) error {
	return nil
}

func (r *Backend) CloseEngine(ctx context.Context, config *backend.EngineConfig, engineUUID uuid.UUID) error {
	return nil
}

func (r *Backend) ImportEngine(ctx context.Context, engineUUID uuid.UUID, regionSplitSize, regionSplitKeys int64) error {
	return nil
}

func (r *Backend) CleanupEngine(ctx context.Context, engineUUID uuid.UUID) error {
	for wid, seq := range r.mu.writersSeq {
		jobIDStr := strconv.Itoa(int(r.jobID))
		widStr := strconv.Itoa(wid)
		filePrefix := filepath.Join(jobIDStr, engineUUID.String(), widStr)
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

func (r *Backend) FlushEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return nil
}

func (r *Backend) FlushAllEngines(ctx context.Context) error {
	return nil
}

func (r *Backend) ResetEngine(ctx context.Context, engineUUID uuid.UUID) error {
	return nil
}

func (r *Backend) LocalWriter(ctx context.Context, cfg *backend.LocalWriterConfig, engineUUID uuid.UUID) (backend.EngineWriter, error) {
	onClose := func(writerID, currentSeq int) {
		r.saveWriterSeq(writerID, currentSeq)
	}
	return sharedisk.NewWriter(ctx, r.externalStorage, r.jobID, engineUUID, r.allocWriterID(), onClose), nil
}

func (r *Backend) allocWriterID() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	seq := r.mu.maxWriterID
	r.mu.maxWriterID++
	return seq
}

// saveWriterSeq stores the current writer sequence for the given writer so that
// we can remove the correct file in external storage when cleaning up.
func (r *Backend) saveWriterSeq(writerID int, currentSeq int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mu.writersSeq[writerID] = currentSeq
}

// GetRangeSplitter returns a RangeSplitter that can be used to split the range into multiple subtasks.
func (r *Backend) GetRangeSplitter(ctx context.Context, instanceCnt int) (*sharedisk.RangeSplitter, error) {
	statsFiles, err := r.getAllStatsFileNames(ctx)
	if err != nil {
		return nil, err
	}
	if len(statsFiles) == 0 {
		return nil, nil
	}
	mergePropIter, err := sharedisk.NewMergePropIter(ctx, statsFiles, r.externalStorage)
	if err != nil {
		return nil, err
	}
	// TODO(tangenta): determine the max size and max ways.
	maxKeys := uint64(len(statsFiles) * sharedisk.WriteBatchSize / instanceCnt)
	rs := sharedisk.NewRangeSplitter(math.MaxUint64, maxKeys, math.MaxUint64, mergePropIter)
	return rs, nil
}

func (r *Backend) getAllStatsFileNames(ctx context.Context) ([]string, error) {
	var stats []string
	jobIDStr := strconv.Itoa(int(r.jobID))
	err := r.externalStorage.WalkDir(ctx, &storage.WalkOption{SubDir: jobIDStr}, func(path string, size int64) error {
		if strings.Contains(path, "_stat") {
			stats = append(stats, path)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return stats, nil
}
