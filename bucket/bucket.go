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
	"errors"
	"os"
	"strings"
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
	GetManifest(ctx context.Context, identity manifests.NodeIdentity, manifestKey manifests.ManifestKey) (manifests.Manifest, error)
	PutManifest(ctx context.Context, identity manifests.NodeIdentity, manifest manifests.Manifest) error
	ListHostNames(ctx context.Context, cluster string) ([]manifests.NodeIdentity, error)
	ListClusters(ctx context.Context) ([]string, error)
	DownloadBlob(ctx context.Context, digests digest.ForRestore, file *os.File) error
	UploadBlob(ctx context.Context, file paranoid.File, digests digest.ForUpload) error
	BlobExists(ctx context.Context, digests digest.ForUpload) (bool, error)
}

var (
	Shared   Client
	KeyStore keystore.KeyStore
	once     sync.Once
)

func OpenShared(cfg config.Config) Client {
	once.Do(func() {
		KeyStore = keystore.NewKeyStore(cfg.BucketName, strings.Trim(cfg.BucketKeyPrefix, "/"))

		if cfg.IsAWS() {
			Shared = aws.NewAWSClient(cfg, KeyStore)
		} else if cfg.IsGCS() {
			Shared = google.NewGCSClient(cfg, KeyStore)
		} else {
			err := errors.New("cloud provider not supported")
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
		manifest, err := c.GetManifest(ctx, identity, manifestKey)
		if err != nil {
			zap.S().Errorw("get_manifest_error", "identity", identity, "manifestKey", manifestKey, "err", err)
			return nil, err
		}
		results = append(results, manifest)
	}
	return results, nil
}
