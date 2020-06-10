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

package backup

import (
	"context"
	"sync"

	"github.com/retailnext/cassandrabackup/bucket/config"
	"go.uber.org/zap"
)

func (p *processor) uploadFiles() {
	lgr := zap.S()
	defer close(p.uploadedFiles)

	var wg sync.WaitGroup
	limiter := make(chan struct{}, 2)
	for {
		record, ok := <-p.prospectedFiles
		if !ok {
			lgr.Debug("prospecting_done")
			break
		}
		limiter <- struct{}{}
		wg.Add(1)
		go p.uploadFile(record, &wg, limiter)
	}
	wg.Wait()
}

func (p *processor) uploadFile(record fileRecord, wg *sync.WaitGroup, limiter <-chan struct{}) {
	lgr := zap.S()
	defer func() {
		p.uploadedFiles <- record
		<-limiter
		wg.Done()
	}()

	if record.ProspectError != nil {
		return
	}

	record.UploadError = p.bucketClient.PutBlob(p.ctx, record.File, record.Digests)
	switch record.UploadError {
	case nil:
		lgr.Debugw("upload_done", "path", record.File.Name(), "size", record.File.Len())
	case context.Canceled:
		lgr.Infow("upload_cancelled", "path", record.File.Name(), "size", record.File.Len())
	case config.UploadSkipped:
		lgr.Debugw("upload_skipped", "path", record.File.Name(), "size", record.File.Len())
	default:
		lgr.Warnw("upload_failed", "path", record.File.Name(), "err", record.UploadError)
	}
}
