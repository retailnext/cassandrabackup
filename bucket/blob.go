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

package bucket

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/metrics"
	"github.com/retailnext/cassandrabackup/paranoid"
	"go.uber.org/zap"
)

var UploadSkipped = errors.New("upload skipped")

func PutBlob(ctx context.Context, c Client, file paranoid.File, digests digest.ForUpload) error {
	if exists, err := c.BlobExists(ctx, digests); err != nil {
		metrics.Bucket.UploadErrors.Inc()
		return err
	} else if exists {
		metrics.Bucket.SkippedFiles.Inc()
		metrics.Bucket.SkippedBytes.Add(float64(file.Len()))
		return UploadSkipped
	}

	if err := c.UploadBlob(ctx, file, digests); err != nil {
		metrics.Bucket.UploadErrors.Inc()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}
	metrics.Bucket.UploadedFiles.Inc()
	metrics.Bucket.UploadedBytes.Add(float64(file.Len()))
	return nil
}

func GetBlob(ctx context.Context, c Client, digests digest.ForRestore, file *os.File) error {
	attempts := 0
	for {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			zap.S().Panicw("get_blob_seek_error", "err", err)
		}
		if err := file.Truncate(0); err != nil {
			zap.S().Panicw("get_blob_truncate_error", "err", err)
		}
		err := c.DownloadBlob(ctx, digests, file)
		if err != nil {
			attempts++
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if attempts > config.GetBlobRetriesLimit {
				return err
			}
			zap.S().Errorw("get_blob_error", "err", err, "attempts", attempts)
		} else {
			return digests.Verify(ctx, file)
		}
	}
}
