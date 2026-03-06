// Copyright 2023 RetailNext, Inc.
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
	"os"
	"strings"
	"sync"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/retailnext/cassandrabackup/bucket/safeuploader"
	"github.com/retailnext/cassandrabackup/cache"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/manifests"
	"github.com/retailnext/cassandrabackup/paranoid"
	"github.com/retailnext/cassandrabackup/unixtime"
	"go.uber.org/zap"
)

const (
	putJsonRetriesLimit       = 3
	getJsonRetriesLimit       = 3
	getBlobRetriesLimit       = 3
	listManifestsRetriesLimit = 3
	retrySleepPerAttempt      = time.Second
)

type Client interface {
	ListManifests(ctx context.Context, identity manifests.NodeIdentity, startAfter, notAfter unixtime.Seconds) (manifests.ManifestKeys, error)
	GetManifests(ctx context.Context, identity manifests.NodeIdentity, keys manifests.ManifestKeys) ([]manifests.Manifest, error)
	PutManifest(ctx context.Context, identity manifests.NodeIdentity, manifest manifests.Manifest) error
	ListHostNames(ctx context.Context, cluster string) ([]manifests.NodeIdentity, error)
	ListClusters(ctx context.Context) ([]string, error)
	DownloadBlob(ctx context.Context, digests digest.ForRestore, file *os.File) error
	PutBlob(ctx context.Context, file paranoid.File, digests digest.ForUpload) error
	KeyStore() *KeyStore
}

type awsClient struct {
	s3Svc       s3API
	uploader    *safeuploader.SafeUploader
	existsCache *ExistsCache

	keyStore             KeyStore
	serverSideEncryption *string
}

var (
	bucketName             = kingpin.Flag("s3-bucket", "S3 bucket name.").Required().String()
	bucketRegion           = kingpin.Flag("s3-region", "S3 bucket region.").Envar("AWS_REGION").Required().String()
	bucketKeyPrefix        = kingpin.Flag("s3-key-prefix", "Set the prefix for files in the S3 bucket").Default("/").String()
	bucketBlobStorageClass = kingpin.Flag("s3-storage-class", "Set the storage class for files in S3").Default(string(types.StorageClassStandardIa)).String()
)

var (
	Shared Client
	once   sync.Once
)

func GetBucketFlags() (*string, *string) {
	return bucketName, bucketRegion
}

func OpenShared() Client {
	once.Do(func() {
		Shared = newAWSClient()
	})
	return Shared
}

func newAWSClient() *awsClient {
	cache.OpenShared()

	cfg, err := config.LoadDefaultConfig(context.Background(), config.WithRegion(*bucketRegion))
	if err != nil {
		zap.S().Fatalw("aws_load_config_error", "err", err)
	}

	s3Svc := s3.NewFromConfig(cfg)
	sse := string(types.ServerSideEncryptionAes256)
	c := &awsClient{
		s3Svc: s3Svc,
		uploader: &safeuploader.SafeUploader{
			S3:                   s3Svc,
			Bucket:               *bucketName,
			ServerSideEncryption: &sse,
			StorageClass:         bucketBlobStorageClass,
		},
		existsCache: &ExistsCache{
			cache: cache.Shared.Cache("bucket_exists"),
		},
		keyStore:             newKeyStore(*bucketName, strings.Trim(*bucketKeyPrefix, "/")),
		serverSideEncryption: &sse,
	}
	c.validateEncryptionConfiguration()
	return c
}

func (c *awsClient) validateEncryptionConfiguration() {
	input := &s3.GetBucketEncryptionInput{
		Bucket: &c.keyStore.bucket,
	}
	output, err := c.s3Svc.GetBucketEncryption(context.Background(), input)
	if err != nil {
		zap.S().Fatalw("failed_to_validate_bucket_encryption", "err", err)
	}
	for _, rule := range output.ServerSideEncryptionConfiguration.Rules {
		if rule.ApplyServerSideEncryptionByDefault != nil {
			if rule.ApplyServerSideEncryptionByDefault.SSEAlgorithm != "" {
				return
			}
		}
	}
	zap.S().Fatalw("bucket_not_configured_with_sse_algorithm", "bucket", c.keyStore.bucket)
}
