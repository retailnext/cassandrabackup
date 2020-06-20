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

package aws

import (
	"context"
	"os"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/paranoid"
	"go.uber.org/zap"
)

func (c *awsClient) UploadBlob(ctx context.Context, file paranoid.File, digests digest.ForUpload) error {
	key := c.keyStore.AbsoluteKeyForBlob(digests.ForRestore())
	awsDigests, ok := digests.(*digest.AWSForUpload)
	if !ok {
		panic("needs to be AWSForUpload")
	}
	return c.uploader.UploadFile(ctx, key, file, awsDigests)
}

func (c *awsClient) DownloadBlob(ctx context.Context, digests digest.ForRestore, file *os.File) error {
	key := c.keyStore.AbsoluteKeyForBlob(digests)
	getObjectInput := &s3.GetObjectInput{
		Bucket: &c.keyStore.Bucket,
		Key:    &key,
	}
	_, err := c.downloader.DownloadWithContext(ctx, file, getObjectInput)
	return err
}

func (c *awsClient) BlobExists(ctx context.Context, digests digest.ForUpload) (bool, error) {
	if c.existsCache.Get(digests.ForRestore()) {
		return true, nil
	}

	key := c.keyStore.AbsoluteKeyForBlob(digests.ForRestore())
	headObjectInput := &s3.HeadObjectInput{
		Bucket: &c.keyStore.Bucket,
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
	expectedLength := digests.TotalLength()
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
