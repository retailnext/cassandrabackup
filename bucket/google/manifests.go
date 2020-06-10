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
	"math"
	"path/filepath"

	"cloud.google.com/go/storage"
	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/manifests"
	"github.com/retailnext/cassandrabackup/unixtime"
	"go.uber.org/zap"
	"google.golang.org/api/iterator"
)

func (c *gcsClient) ListManifests(ctx context.Context, identity manifests.NodeIdentity, startAfter, notAfter unixtime.Seconds) (manifests.ManifestKeys, error) {
	lgr := zap.S()
	prefixKey := c.keyStore.AbsoluteKeyPrefixForManifests(identity)
	query := &storage.Query{
		Delimiter: "/",
		Prefix:    prefixKey,
	}
	err := query.SetAttrSelection([]string{"Name"})
	if err != nil {
		lgr.Errorw("set_attr_selection", "err", err)
	}

	if notAfter == 0 {
		notAfter = unixtime.Seconds(math.MaxInt64)
	}
	startAfterKey := c.keyStore.AbsoluteKeyForManifestTimeRange(identity, startAfter)
	notAfterKey := c.keyStore.AbsoluteKeyForManifestTimeRange(identity, notAfter)

	attempts := 0
	for {
		var err error
		var keys manifests.ManifestKeys
		objects := c.storageClient.Bucket(c.keyStore.Bucket).Objects(ctx, query)
		for {
			var attrs *storage.ObjectAttrs
			attrs, err = objects.Next()

			if err == iterator.Done {
				err = nil
				break
			}
			if err != nil {
				lgr.Errorw("list_manifests_google_error", "err", err)
				break
			}

			if attrs.Name > startAfterKey && attrs.Name < notAfterKey {
				name := filepath.Base(attrs.Name)
				var manifestKey manifests.ManifestKey
				if err := manifestKey.PopulateFromFileName(name); err != nil {
					lgr.Warnw("list_manifests_ignoring_bad_filename", "name", name, "err", err)
				} else {
					keys = append(keys, manifestKey)
				}
			}
		}
		if err != nil {
			attempts++
			if attempts > config.ListManifestsRetriesLimit {
				return nil, err
			}
			lgr.Errorw("list_manifests_google_error", "err", err, "attempts", attempts)
		} else {
			return keys, nil
		}
	}
}
