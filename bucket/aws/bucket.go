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
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"

	"github.com/retailnext/cassandrabackup/bucket/aws/safeuploader"
	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/bucket/existscache"
	"github.com/retailnext/cassandrabackup/bucket/keystore"
	"github.com/retailnext/cassandrabackup/cache"
	"go.uber.org/zap"
)

type awsClient struct {
	s3Svc                s3iface.S3API
	uploader             *safeuploader.SafeUploader
	downloader           s3manageriface.DownloaderAPI
	existsCache          *existscache.ExistsCache
	keyStore             *keystore.KeyStore
	serverSideEncryption *string
}

func NewAWSClient(config *config.Config, keyStore *keystore.KeyStore) *awsClient {
	cache.OpenShared(config)

	awsConf := aws.NewConfig().WithRegion(config.BucketRegion)
	awsSession, err := session.NewSession(awsConf)
	if err != nil {
		zap.S().Fatalw("aws_new_session_error", "err", err)
	}

	s3Svc := s3.New(awsSession)
	c := &awsClient{
		s3Svc: s3Svc,
		uploader: &safeuploader.SafeUploader{
			S3:                   s3Svc,
			Bucket:               config.BucketName,
			ServerSideEncryption: aws.String(s3.ServerSideEncryptionAes256),
			StorageClass:         aws.String(config.S3StorageClass),
		},
		downloader: s3manager.NewDownloaderWithClient(s3Svc, func(d *s3manager.Downloader) {
			d.PartSize = 64 * 1024 * 1024 // 64MB per part
		}),
		existsCache:          existscache.NewExistsCache(),
		keyStore:             keyStore,
		serverSideEncryption: aws.String(s3.ServerSideEncryptionAes256),
	}
	c.validateEncryptionConfiguration()
	return c
}

func (c *awsClient) validateEncryptionConfiguration() {
	input := &s3.GetBucketEncryptionInput{
		Bucket: &c.keyStore.Bucket,
	}
	output, err := c.s3Svc.GetBucketEncryption(input)
	if err != nil {
		zap.S().Fatalw("failed_to_validate_bucket_encryption", "err", err)
	}
	for _, rule := range output.ServerSideEncryptionConfiguration.Rules {
		if rule.ApplyServerSideEncryptionByDefault != nil {
			if rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm != nil {
				return
			}
		}
	}
	zap.S().Fatalw("bucket_not_configured_with_sse_algorithm", "bucket", c.keyStore.Bucket)
}
