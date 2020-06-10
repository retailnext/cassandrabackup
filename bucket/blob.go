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

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/paranoid"
	"go.uber.org/zap"
)

var UploadSkipped = errors.New("upload skipped")

func (c *awsClient) KeyStore() *KeyStore {
	return &c.keyStore
}

func (c *awsClient) PutBlob(ctx context.Context, file paranoid.File, digests digest.ForUpload) error {
	key := c.keyStore.AbsoluteKeyForBlob(digests.ForRestore())
	if exists, err := c.blobExists(ctx, digests); err != nil {
		uploadErrors.Inc()
		return err
	} else if exists {
		skippedFiles.Inc()
		skippedBytes.Add(float64(file.Len()))
		return UploadSkipped
	}

	if err := c.uploader.UploadFile(ctx, key, file, digests); err != nil {
		uploadErrors.Inc()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}
	uploadedFiles.Inc()
	uploadedBytes.Add(float64(file.Len()))
	return nil
}

func (c *awsClient) DownloadBlob(ctx context.Context, digests digest.ForRestore, file *os.File) error {
	key := c.keyStore.AbsoluteKeyForBlob(digests)
	getObjectInput := &s3.GetObjectInput{
		Bucket: &c.keyStore.bucket,
		Key:    &key,
	}
	attempts := 0
	for {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			zap.S().Panicw("get_blob_seek_error", "err", err)
		}
		if err := file.Truncate(0); err != nil {
			zap.S().Panicw("get_blob_truncate_error", "err", err)
		}
		_, err := c.downloader.DownloadWithContext(ctx, file, getObjectInput)
		if err != nil {
			attempts++
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if IsNoSuchKey(err) || attempts > getBlobRetriesLimit {
				return err
			}
			zap.S().Errorw("get_blob_s3_error", "err", err, "attempts", attempts)
		} else {
			return digests.Verify(ctx, file)
		}
	}
}

func (c *awsClient) blobExists(ctx context.Context, digests digest.ForUpload) (bool, error) {
	key := c.keyStore.AbsoluteKeyForBlob(digests.ForRestore())
	if c.existsCache.Get(digests.ForRestore()) {
		return true, nil
	}

	headObjectInput := &s3.HeadObjectInput{
		Bucket: &c.keyStore.bucket,
		Key:    &key,
	}
	headObjectOutput, err := c.s3Svc.HeadObjectWithContext(ctx, headObjectInput)
	if err != nil {
		if IsNoSuchKey(err) {
			return false, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
		return false, err
	}
	if headObjectOutput.DeleteMarker != nil && *headObjectOutput.DeleteMarker {
		zap.S().Infow("blob_exists_saw_delete_marker", "key", key)
		return false, nil
	}
	expectedLength := digests.PartDigests().TotalLength()
	actualLength := *headObjectOutput.ContentLength
	if actualLength != expectedLength {
		zap.S().Infow("blob_exists_saw_wrong_length", "key", key, "expected", expectedLength, "actual", actualLength)
		return false, nil
	}

	if headObjectOutput.ObjectLockRetainUntilDate != nil {
		c.existsCache.Put(digests.ForRestore(), *headObjectOutput.ObjectLockRetainUntilDate)
	}

	return true, nil
}

var (
	skippedBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "bucket",
		Name:      "skipped_bytes_total",
		Help:      "Total bytes not uploaded due to them already existing in the bucket.",
	})
	skippedFiles = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "bucket",
		Name:      "skipped_files_total",
		Help:      "Number of files not uploaded due to them already existing in the bucket.",
	})
	uploadedBytes = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "bucket",
		Name:      "upload_bytes_total",
		Help:      "Total bytes uploaded to the bucket.",
	})
	uploadedFiles = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "bucket",
		Name:      "upload_files_total",
		Help:      "Number of files uploaded to the bucket.",
	})
	uploadErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "bucket",
		Name:      "upload_errors_total",
		Help:      "Number of failed file uploads.",
	})
)

func init() {
	prometheus.MustRegister(skippedBytes)
	prometheus.MustRegister(skippedFiles)
	prometheus.MustRegister(uploadedBytes)
	prometheus.MustRegister(uploadedFiles)
	prometheus.MustRegister(uploadErrors)
}
