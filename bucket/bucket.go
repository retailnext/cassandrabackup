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
	"fmt"
	"os"
	"sync"

	"github.com/retailnext/cassandrabackup/bucket/aws"
	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/bucket/google"
	"github.com/retailnext/cassandrabackup/bucket/keystore"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/manifests"
	"github.com/retailnext/cassandrabackup/paranoid"
	"github.com/retailnext/cassandrabackup/unixtime"
	"go.uber.org/zap"
)

type Client interface {
	ListManifests(ctx context.Context, identity manifests.NodeIdentity, startAfter, notAfter unixtime.Seconds) (manifests.ManifestKeys, error)
	GetManifest(ctx context.Context, absoluteKey string) (manifests.Manifest, error)
	PutManifest(ctx context.Context, absoluteKey string, manifest manifests.Manifest) error
	ListHostNames(ctx context.Context, cluster string) ([]manifests.NodeIdentity, error)
	ListClusters(ctx context.Context) ([]string, error)
	DownloadBlob(ctx context.Context, digests digest.ForRestore, file *os.File) error
	PutBlob(ctx context.Context, file paranoid.File, digests digest.ForUpload) error
	KeyStore() *keystore.KeyStore
}

var (
	Shared Client
	once   sync.Once
)

func OpenShared(config *config.Config) Client {
	once.Do(func() {
		if config.Provider == "aws" {
			Shared = aws.NewAWSClient(config)
		} else if config.Provider == "google" {
			Shared = google.NewGCSClient(config)
		} else {
			err := fmt.Errorf("cloud provider not supported: %s", config.Provider)
			zap.S().Fatalw("cloud_provider_error", "err", err)
		}
	})
	return Shared
}

func GetManifests(ctx context.Context, c Client, identity manifests.NodeIdentity, keys manifests.ManifestKeys) ([]manifests.Manifest, error) {
	var results []manifests.Manifest
	doneCh := ctx.Done()
	for _, manifestKey := range keys {
		select {
		case <-doneCh:
			return nil, nil
		default:
		}
		absoluteKey := c.KeyStore().AbsoluteKeyForManifest(identity, manifestKey)
		manifest, err := c.GetManifest(ctx, absoluteKey)
		if err != nil {
			zap.S().Errorw("get_manifest_error", "key", absoluteKey, "err", err)
			return nil, err
		}
		results = append(results, manifest)
	}
	return results, nil
}
