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
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/paranoid"
	"go.uber.org/zap"
)

const objectLockSafetyMargin = 12 * time.Hour

func (c *gcsClient) UploadBlob(ctx context.Context, file paranoid.File, digests digest.ForUpload) error {
	key := c.keyStore.AbsoluteKeyForBlob(digests.ForRestore())
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
	targetWriter.ObjectAttrs.MD5 = digests.MD5()
	defer func() {
		if closeErr := targetWriter.Close(); closeErr != nil {
			panic(closeErr)
		}
	}()

	_, err = io.Copy(targetWriter, sourceFile)
	if err != nil {
		return err
	}

	return nil
}

func (c *gcsClient) DownloadBlob(ctx context.Context, digests digest.ForRestore, file *os.File) error {
	key := c.keyStore.AbsoluteKeyForBlob(digests)
	sourceReader, err := c.storageClient.Bucket(c.keyStore.Bucket).Object(key).NewReader(ctx)
	if err != nil {
		return err
	}
	defer sourceReader.Close()

	_, err = io.Copy(file, sourceReader)
	return err
}

func (c *gcsClient) BlobExists(ctx context.Context, digests digest.ForUpload) (bool, error) {
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
	expectedLength := digests.ContentLength()
	actualLength := attrs.Size
	if actualLength != expectedLength {
		zap.S().Infow("blob_exists_saw_wrong_length", "key", key, "expected", expectedLength, "actual", actualLength)
		return false, nil
	}

	if time.Until(attrs.RetentionExpirationTime) < c.objectRetention-objectLockSafetyMargin {
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
