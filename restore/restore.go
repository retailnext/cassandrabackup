// Copyright 2020 RetailNext, Inc.
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
	"context"
	"fmt"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"github.com/retailnext/cassandrabackup/bucket"
	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/metrics"
	"github.com/retailnext/cassandrabackup/paranoid"
	"github.com/retailnext/cassandrabackup/restore/plan"
	"github.com/retailnext/writefile"
	"go.uber.org/zap"
)

type worker struct {
	ctx    context.Context
	cache  *digest.Cache
	client bucket.Client
	target writefile.Config

	limiter    chan struct{}
	wg         sync.WaitGroup
	fileErrors FileErrors
	lock       sync.Mutex
}

func newWorker(config *config.Config, directory string, ensureOwnership bool) *worker {
	w := worker{
		target: writefile.Config{
			Directory:                directory,
			DirectoryMode:            0755,
			EnsureDirectoryOwnership: ensureOwnership,
			FileMode:                 0644,
			EnsureFileOwnership:      ensureOwnership,
		},
	}

	if ensureOwnership {
		lgr := zap.S()
		userName := "cassandra"
		osUser, err := user.Lookup(userName)
		if err != nil {
			lgr.Panicw("user_lookup_error", "user", userName, "err", err)
		}

		uid, err := strconv.Atoi(osUser.Uid)
		if err != nil {
			lgr.Panicw("user_id_lookup_error", "uid", osUser.Uid, "err", err)
		}
		w.target.DirectoryUID = uid
		w.target.FileUID = uid

		gid, err := strconv.Atoi(osUser.Gid)
		if err != nil {
			lgr.Panicw("group_id_lookup_error", "gid", osUser.Gid, "err", err)
		}
		w.target.DirectoryGID = gid
		w.target.FileGID = gid
	}

	w.cache = digest.OpenShared(config)
	w.client = bucket.OpenShared(config)

	return &w
}

func (w *worker) restoreFiles(ctx context.Context, files map[string]digest.ForRestore) error {
	metrics.Restore.RegisterMetrics()
	w.ctx = ctx
	w.limiter = make(chan struct{}, 4)

	doneCh := ctx.Done()
	for name, forRestore := range files {
		select {
		case <-doneCh:
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
				metrics.Restore.SkippedBytes.Add(float64(maybeFile.Len()))
				metrics.Restore.SkippedFiles.Inc()
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
		downloadErr := bucket.GetBlob(w.ctx, w.client, forRestore, file)
		if downloadErr != nil {
			metrics.Restore.DownloadErrors.Inc()
			return downloadErr
		}

		d := time.Since(start)
		metrics.Restore.DownloadFiles.Inc()
		metrics.Restore.DownloadSeconds.Add(d.Seconds())
		if info, infoErr := file.Stat(); infoErr != nil {
			lgr.Warnw("stat_error", "err", err)
		} else {
			metrics.Restore.DownloadBytes.Add(float64(info.Size()))
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

type downloadPlan struct {
	files        map[string]digest.ForRestore
	changedFiles map[string][]digest.ForRestore
}

func (dp *downloadPlan) addHost(prefix string, nodePlan plan.NodePlan) {
	if dp.files == nil {
		dp.files = make(map[string]digest.ForRestore)
	}
	for fileName, fileDigest := range nodePlan.Files {
		if prefix != "" {
			fileName = path.Join(prefix, fileName)
		}
		dp.files[fileName] = fileDigest
	}
	if len(nodePlan.ChangedFiles) > 0 {
		if dp.changedFiles == nil {
			dp.changedFiles = make(map[string][]digest.ForRestore)
		}
		for fileName, historyEntries := range nodePlan.ChangedFiles {
			if prefix != "" {
				fileName = path.Join(prefix, fileName)
			}
			historyForFile := dp.changedFiles[fileName]
			for _, entry := range historyEntries {
				historyForFile = append(historyForFile, entry.Digest)
			}
			dp.changedFiles[fileName] = historyForFile
		}
	}
}

func (dp *downloadPlan) includeChanged(prefix string) map[string]digest.ForRestore {
	if len(dp.changedFiles) == 0 {
		return dp.files
	}

	result := make(map[string]digest.ForRestore)
	for fileName, fileDigest := range dp.files {
		result[fileName] = fileDigest
	}
	for fileName, versions := range dp.changedFiles {
		for versionId, versionDigest := range versions {
			name := path.Join(prefix, fileName, fmt.Sprint(versionId))
			result[name] = versionDigest
		}
	}
	return result
}
