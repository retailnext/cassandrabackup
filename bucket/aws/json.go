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
	"bytes"
	"compress/gzip"
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/mailru/easyjson"
	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/manifests"
	"go.uber.org/zap"
)

func (c *awsClient) PutManifest(ctx context.Context, absoluteKey string, manifest manifests.Manifest) error {
	var encodeBuffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&encodeBuffer)
	if _, err := easyjson.MarshalToWriter(manifest, gzipWriter); err != nil {
		panic(err)
	}
	if err := gzipWriter.Close(); err != nil {
		panic(err)
	}

	attempts := 0
	for {
		putObjectInput := &s3.PutObjectInput{
			Bucket:               &c.keyStore.Bucket,
			Key:                  &absoluteKey,
			ContentType:          aws.String("application/json"),
			ContentEncoding:      aws.String("gzip"),
			ServerSideEncryption: c.serverSideEncryption,
			Body:                 bytes.NewReader(encodeBuffer.Bytes()),
		}
		_, err := c.s3Svc.PutObjectWithContext(ctx, putObjectInput)
		if err != nil {
			attempts++
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if attempts > config.PutJsonRetriesLimit {
				return err
			}
			zap.S().Warnw("s3_put_object_error", "err", err, "attempts", attempts)
			time.Sleep(time.Duration(attempts) * config.RetrySleepPerAttempt)
		} else {
			return nil
		}
	}
}

func (c *awsClient) GetManifest(ctx context.Context, absoluteKey string) (manifests.Manifest, error) {
	getObjectInput := &s3.GetObjectInput{
		Bucket: &c.keyStore.Bucket,
		Key:    &absoluteKey,
	}
	attempts := 0
	for {
		getObjectOutput, err := c.s3Svc.GetObjectWithContext(ctx, getObjectInput)
		if err != nil {
			if IsNoSuchKey(err) {
				return manifests.Manifest{}, err
			}
			attempts++
			if ctxErr := ctx.Err(); ctxErr != nil {
				return manifests.Manifest{}, ctxErr
			}
			if attempts > config.GetJsonRetriesLimit {
				return manifests.Manifest{}, err
			}
			zap.S().Warnw("s3_get_object_error", "err", err, "attempts", attempts)
			time.Sleep(time.Duration(attempts) * config.RetrySleepPerAttempt)
		} else {
			var manifest manifests.Manifest
			err = easyjson.UnmarshalFromReader(getObjectOutput.Body, &manifest)
			_ = getObjectOutput.Body.Close()
			return manifest, err
		}
	}
}
