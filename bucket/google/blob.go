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

package google

import (
	"context"
	"io"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/bucket/keystore"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/metrics"
	"github.com/retailnext/cassandrabackup/paranoid"
	"go.uber.org/zap"
)

func (c *gcsClient) KeyStore() *keystore.KeyStore {
	return &c.keyStore
}

func (c *gcsClient) PutBlob(ctx context.Context, file paranoid.File, digests digest.ForUpload) error {
	key := c.keyStore.AbsoluteKeyForBlob(digests.ForRestore())
	if exists, err := c.blobExists(ctx, digests); err != nil {
		metrics.Bucket.UploadErrors.Inc()
		return err
	} else if exists {
		metrics.Bucket.SkippedFiles.Inc()
		metrics.Bucket.SkippedBytes.Add(float64(file.Len()))
		return config.UploadSkipped
	}

	if err := c.uploadFile(ctx, key, file, digests); err != nil {
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

const objectLockSafetyMargin = 12 * time.Hour

func (c *gcsClient) uploadFile(ctx context.Context, key string, file paranoid.File, digests digest.ForUpload) error {
	sourceFile, err := file.Open()
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := sourceFile.Close(); closeErr != nil {
			panic(closeErr)
		}
	}()

	targetWriter := c.storageClient.Bucket(c.keyStore.Bucket).Object(key).NewWriter(ctx)
	_, err = io.Copy(targetWriter, sourceFile)
	if err != nil {
		return err
	}

	return targetWriter.Close()
}

func (c *gcsClient) DownloadBlob(ctx context.Context, digests digest.ForRestore, file *os.File) error {
	key := c.keyStore.AbsoluteKeyForBlob(digests)
	attempts := 0
	for {
		if _, err := file.Seek(0, io.SeekStart); err != nil {
			zap.S().Panicw("get_blob_seek_error", "err", err)
		}
		if err := file.Truncate(0); err != nil {
			zap.S().Panicw("get_blob_truncate_error", "err", err)
		}
		err := c.downloadFile(ctx, key, file)
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

func (c *gcsClient) downloadFile(ctx context.Context, key string, targetFile *os.File) error {
	sourceReader, err := c.storageClient.Bucket(c.keyStore.Bucket).Object(key).NewReader(ctx)
	if err != nil {
		return err
	}
	_, err = io.Copy(targetFile, sourceReader)
	return err
}

func (c *gcsClient) blobExists(ctx context.Context, digests digest.ForUpload) (bool, error) {
	if c.existsCache.Get(digests.ForRestore()) {
		return true, nil
	}

	key := c.keyStore.AbsoluteKeyForBlob(digests.ForRestore())
	attrs, err := c.storageClient.Bucket(c.keyStore.Bucket).Object(key).Attrs(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			return false, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return false, ctxErr
		}
		return false, err
	}
	expectedLength := digests.TotalLength()
	actualLength := attrs.Size
	if actualLength != expectedLength {
		zap.S().Infow("blob_exists_saw_wrong_length", "key", key, "expected", expectedLength, "actual", actualLength)
		return false, nil
	}

	if attrs.RetentionExpirationTime.Unix() < time.Now().Add(c.objectRetention-objectLockSafetyMargin).Unix() {
		attrs, err = c.resetObjectRetention(ctx, key)
		if err != nil {
			return false, err
		}

		c.existsCache.Put(digests.ForRestore(), attrs.RetentionExpirationTime)
		zap.S().Infow("blob_update_retention", "key", key, "expiration", attrs.RetentionExpirationTime)
	}

	return true, nil
}

func (c *gcsClient) resetObjectRetention(ctx context.Context, key string) (*storage.ObjectAttrs, error) {
	if _, err := c.storageClient.Bucket(c.keyStore.Bucket).Object(key).Update(ctx, storage.ObjectAttrsToUpdate{
		EventBasedHold: true,
	}); err != nil {
		return nil, err
	}
	return c.storageClient.Bucket(c.keyStore.Bucket).Object(key).Update(ctx, storage.ObjectAttrsToUpdate{
		EventBasedHold: false,
	})
}
