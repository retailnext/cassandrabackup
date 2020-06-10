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

	"cloud.google.com/go/storage"
	"github.com/retailnext/cassandrabackup/manifests"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

func (c *gcsClient) ListHostNames(ctx context.Context, cluster string) ([]manifests.NodeIdentity, error) {
	lgr := zap.S()
	prefix := c.keyStore.AbsoluteKeyPrefixForClusterHosts(cluster)
	query := &storage.Query{
		Delimiter: "/",
		Prefix:    prefix,
	}

	var result []manifests.NodeIdentity
	objects := c.storageClient.Bucket(c.keyStore.Bucket).Objects(ctx, query)
	for {
		attrs, err := objects.Next()
		if err == iterator.Done {
			return result, nil
		}
		if attrs.Name != "" {
			continue
		}
		if err != nil {
			lgr.Errorw("list_clusters_error", "err", err)
			return result, err
		}

		if ni, err := c.keyStore.NodeIdentityFromKey(attrs.Prefix); err != nil {
			lgr.Errorw("decode_cluster_error", "err", err)
			return result, err
		} else {
			result = append(result, ni)
		}
	}
}

func (c *gcsClient) ListClusters(ctx context.Context) ([]string, error) {
	lgr := zap.S()
	prefix := c.keyStore.AbsoluteKeyPrefixForClusters()
	query := &storage.Query{
		Delimiter: "/",
		Prefix:    prefix,
	}

	var result []string
	objects := c.storageClient.Bucket(c.keyStore.Bucket).Objects(ctx, query)
	for {
		attrs, err := objects.Next()
		if err == iterator.Done {
			return result, nil
		}
		if attrs.Name != "" {
			continue
		}
		if err != nil {
			lgr.Errorw("list_clusters_error", "err", err)
			return result, err
		}

		if cluster, err := c.keyStore.DecodeCluster(attrs.Prefix); err != nil {
			lgr.Errorw("decode_cluster_error", "err", err)
			return result, err
		} else {
			result = append(result, cluster)
		}
	}
}
