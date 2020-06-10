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
	"bytes"
	"compress/gzip"
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/mailru/easyjson"
	"go.uber.org/zap"
)

func (c *awsClient) putDocument(ctx context.Context, absoluteKey string, v easyjson.Marshaler) error {
	var encodeBuffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&encodeBuffer)
	if _, err := easyjson.MarshalToWriter(v, gzipWriter); err != nil {
		panic(err)
	}
	if err := gzipWriter.Close(); err != nil {
		panic(err)
	}

	putObjectInput := &s3.PutObjectInput{
		Bucket:               &c.keyStore.bucket,
		Key:                  &absoluteKey,
		ContentType:          aws.String("application/json"),
		ContentEncoding:      aws.String("gzip"),
		ServerSideEncryption: c.serverSideEncryption,
		Body:                 bytes.NewReader(encodeBuffer.Bytes()),
	}
	attempts := 0
	for {
		_, err := c.s3Svc.PutObjectWithContext(ctx, putObjectInput)
		if err != nil {
			attempts++
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if attempts > putJsonRetriesLimit {
				return err
			}
			zap.S().Warnw("s3_put_object_error", "err", err, "attempts", attempts)
			time.Sleep(time.Duration(attempts) * retrySleepPerAttempt)
		} else {
			return nil
		}
	}
}

func (c *awsClient) getDocument(ctx context.Context, absoluteKey string, v easyjson.Unmarshaler) error {
	getObjectInput := &s3.GetObjectInput{
		Bucket: &c.keyStore.bucket,
		Key:    &absoluteKey,
	}
	attempts := 0
	for {
		getObjectOutput, err := c.s3Svc.GetObjectWithContext(ctx, getObjectInput)
		if err != nil {
			if IsNoSuchKey(err) {
				return err
			}
			attempts++
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if attempts > getJsonRetriesLimit {
				return err
			}
			zap.S().Warnw("s3_get_object_error", "err", err, "attempts", attempts)
			time.Sleep(time.Duration(attempts) * retrySleepPerAttempt)
		} else {
			err = easyjson.UnmarshalFromReader(getObjectOutput.Body, v)
			_ = getObjectOutput.Body.Close()
			return err
		}
	}
}
