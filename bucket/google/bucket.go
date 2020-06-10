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
	"strings"

	"cloud.google.com/go/storage"
	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/bucket/existscache"
	"github.com/retailnext/cassandrabackup/bucket/keystore"
	"github.com/retailnext/cassandrabackup/cache"
	"go.uber.org/zap"
)

type gcsClient struct {
	storageClient *storage.Client
	existsCache   *existscache.ExistsCache

	keyStore             keystore.KeyStore
	serverSideEncryption *string
}

func NewGCSClient(config *config.Config) *gcsClient {
	cache.OpenShared(config)

	ctx := context.Background()
	storageClient, err := storage.NewClient(ctx)
	if err != nil {
		zap.S().Fatalw("gcs_new_client_error", "err", err)
	}

	c := &gcsClient{
		storageClient: storageClient,
		existsCache:   existscache.NewExistsCache(),
		keyStore:      keystore.NewKeyStore(config.BucketName, strings.Trim(config.BucketKeyPrefix, "/")),
	}

	//c.validateEncryptionConfiguration()

	return c
}

func (c *gcsClient) validateEncryptionConfiguration() {
	ctx := context.Background()
	attrs, err := c.storageClient.Bucket(c.keyStore.Bucket).Attrs(ctx)
	if err != nil {
		zap.S().Fatalw("failed_to_validate_bucket_encryption", "err", err)
	}
	if attrs.Encryption != nil {
		return
	}
	zap.S().Fatalw("bucket_not_configured_with_customer_managed_key", "bucket", c.keyStore.Bucket)
}
