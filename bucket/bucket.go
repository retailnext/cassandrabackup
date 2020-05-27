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

package bucket

import (
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/aws/aws-sdk-go/service/s3/s3manager/s3manageriface"
	"github.com/retailnext/cassandrabackup/bucket/safeuploader"
	"github.com/retailnext/cassandrabackup/cache"
	"go.uber.org/zap"
	"gopkg.in/alecthomas/kingpin.v2"
)

const putJsonRetriesLimit = 3
const getJsonRetriesLimit = 3
const getBlobRetriesLimit = 3
const listManifestsRetriesLimit = 3
const retrySleepPerAttempt = time.Second

type Client struct {
	s3Svc       s3iface.S3API
	uploader    *safeuploader.SafeUploader
	downloader  s3manageriface.DownloaderAPI
	existsCache *ExistsCache

	bucket               string
	prefix               string
	serverSideEncryption *string
}

var (
	bucketName             = kingpin.Flag("s3-bucket", "S3 bucket name.").Required().String()
	bucketRegion           = kingpin.Flag("s3-region", "S3 bucket region.").Envar("AWS_REGION").Required().String()
	bucketKeyPrefix        = kingpin.Flag("s3-key-prefix", "Set the prefix for files in the S3 bucket").Default("/").String()
	bucketBlobStorageClass = kingpin.Flag("s3-storage-class", "Set the storage class for files in S3").Default(s3.StorageClassStandardIa).String()
)

var (
	Shared *Client
	once   sync.Once
)

func GetBucketFlags() (*string, *string) {
	return bucketName, bucketRegion
}

func OpenShared() *Client {
	once.Do(func() {
		Shared = newClient()
	})
	return Shared
}

func newClient() *Client {
	cache.OpenShared()

	awsConf := aws.NewConfig().WithRegion(*bucketRegion)
	awsSession, err := session.NewSession(awsConf)
	if err != nil {
		zap.S().Fatalw("aws_new_session_error", "err", err)
	}

	s3Svc := s3.New(awsSession)
	c := &Client{
		s3Svc: s3Svc,
		uploader: &safeuploader.SafeUploader{
			S3:                   s3Svc,
			Bucket:               *bucketName,
			ServerSideEncryption: aws.String(s3.ServerSideEncryptionAes256),
			StorageClass:         bucketBlobStorageClass,
		},
		downloader: s3manager.NewDownloaderWithClient(s3Svc, func(d *s3manager.Downloader) {
			d.PartSize = 64 * 1024 * 1024 // 64MB per part
		}),
		existsCache: &ExistsCache{
			cache: cache.Shared.Cache("bucket_exists"),
		},
		bucket:               *bucketName,
		prefix:               strings.Trim(*bucketKeyPrefix, "/"),
		serverSideEncryption: aws.String(s3.ServerSideEncryptionAes256),
	}
	c.validateEncryptionConfiguration()
	return c
}

func (c *Client) validateEncryptionConfiguration() {
	input := &s3.GetBucketEncryptionInput{
		Bucket: &c.bucket,
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
	zap.S().Fatalw("bucket_not_configured_with_sse_algorithm", "bucket", c.bucket)
}
