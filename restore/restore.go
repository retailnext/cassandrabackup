// Copyright 2019 RetailNext, Inc.
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

package restore

import (
	"cassandrabackup/bucket"
	"cassandrabackup/digest"
	"cassandrabackup/manifests"
	"cassandrabackup/paranoid"
	"cassandrabackup/writefile"
	"context"
	"os"
	"os/user"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

type worker struct {
	identity manifests.NodeIdentity
	ctx      context.Context
	cache    *digest.Cache
	client   *bucket.Client
	target   writefile.Config

	limiter    chan struct{}
	wg         sync.WaitGroup
	fileErrors FileErrors
	lock       sync.Mutex
}

func newWorker(identity manifests.NodeIdentity) *worker {
	lgr := zap.S()
	cassandraUser, err := user.Lookup("cassandra")
	if err != nil {
		lgr.Panicw("cassandra_user_lookup_error", "err", err)
	}
	cassandraUid, err := strconv.Atoi(cassandraUser.Uid)
	if err != nil {
		lgr.Panicw("cassandra_user_id_lookup_error", "uid", cassandraUser.Uid, "err", err)
	}
	cassandraGid, err := strconv.Atoi(cassandraUser.Gid)
	if err != nil {
		lgr.Panicw("cassandra_group_id_lookup_error", "gid", cassandraUser.Gid, "err", err)
	}

	return &worker{
		identity: identity,
		cache:    digest.OpenShared(),
		client:   bucket.NewClient(),
		target: writefile.Config{
			Directory:                "/var/lib/cassandra/data",
			DirectoryMode:            0755,
			DirectoryUID:             cassandraUid,
			DirectoryGID:             cassandraGid,
			EnsureDirectoryOwnership: true,
			FileMode:                 0644,
			FileUID:                  cassandraUid,
			FileGID:                  cassandraGid,
			EnsureFileOwnership:      true,
		},
	}
}

func (w *worker) restoreFiles(ctx context.Context, files map[string]digest.ForRestore) error {
	registerMetrics()
	w.ctx = ctx
	w.limiter = make(chan struct{}, 4)

	doneCh := ctx.Done()
	for name, forRestore := range files {
		select {
		case _ = <-doneCh:
			break
		case w.limiter <- struct{}{}:
			w.wg.Add(1)
			go w.restoreFile(name, forRestore)
		}
	}
	w.wg.Wait()
	err := ctx.Err()
	if err == nil {
		if w.fileErrors != nil {
			err = w.fileErrors
		}
	}
	return err
}

func (w *worker) restoreFile(name string, forRestore digest.ForRestore) {
	lgr := zap.S()
	var err error
	defer func() {
		if err != nil {
			lgr.Errorw("restore_file_error", "path", name, "err", err)
			w.lock.Lock()
			if w.fileErrors == nil {
				w.fileErrors = make(FileErrors)
			}
			w.fileErrors[name] = err
			w.lock.Unlock()
		}
		<-w.limiter
		w.wg.Done()
	}()

	path := filepath.Join(w.target.Directory, name)
	if maybeFile, maybeFileErr := paranoid.NewFile(path); maybeFileErr == nil {
		if forUpload, forUploadErr := w.cache.Get(w.ctx, maybeFile); forUploadErr == nil {
			if forUpload.ForRestore() == forRestore {
				skippedBytes.Add(float64(maybeFile.Len()))
				skippedFiles.Inc()
				return
			} else {
				lgr.Infow("existing_file_digest_mismatch", "path", path)
			}
		} else {
			lgr.Infow("existing_file_digest_error", "path", path, "err", forUploadErr)
		}
	}

	err = w.target.WriteFile(name, func(file *os.File) error {
		start := time.Now()
		downloadErr := w.client.DownloadBlob(w.ctx, forRestore, file)
		if downloadErr != nil {
			downloadErrors.Inc()
			return downloadErr
		}

		d := time.Since(start)
		downloadFiles.Inc()
		downloadSeconds.Add(d.Seconds())
		if info, infoErr := file.Stat(); infoErr != nil {
			lgr.Warnw("stat_error", "err", err)
		} else {
			downloadBytes.Add(float64(info.Size()))
			// Prime the cache with this file since it's still in the kernel block cache
			pfile := paranoid.NewFileFromInfo(file.Name(), info)
			_, _ = w.cache.Get(w.ctx, pfile)
		}
		return nil
	})

	if err == nil {
		lgr.Infow("restored_file", "path", name)
	}
}

var (
	skippedFiles = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "restore",
		Name:      "skipped_files_total",
		Help:      "Number of files skipped during restore due to already being on disk.",
	})
	skippedBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "restore",
		Name:      "skipped_bytes_total",
		Help:      "Number of bytes skipped during restore due to already being on disk.",
	})
	downloadFiles = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "restore",
		Name:      "download_files_total",
		Help:      "Number of files downloaded during the restore.",
	})
	downloadBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "restore",
		Name:      "download_bytes_total",
		Help:      "Number of bytes downloaded during the restore.",
	})
	downloadErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "restore",
		Name:      "download_errors_total",
		Help:      "Number files that failed to download during the restore.",
	})
	downloadSeconds = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "restore",
		Name:      "download_seconds_total",
		Help:      "Time spent downloading files during the restore.",
	})

	registerOnce sync.Once
)

func registerMetrics() {
	registerOnce.Do(func() {
		prometheus.MustRegister(skippedFiles)
		prometheus.MustRegister(skippedBytes)
		prometheus.MustRegister(downloadSeconds)
		prometheus.MustRegister(downloadFiles)
		prometheus.MustRegister(downloadBytes)
		prometheus.MustRegister(downloadErrors)
	})
}
