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
	"context"
	"path/filepath"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/retailnext/cassandrabackup/manifests"
	"github.com/retailnext/cassandrabackup/unixtime"
	"go.uber.org/zap"
)

func (c *Client) ListManifests(ctx context.Context, identity manifests.NodeIdentity, startAfter, notAfter unixtime.Seconds) (manifests.ManifestKeys, error) {
	lgr := zap.S()
	prefixKey := c.absoluteKeyPrefixForManifests(identity)
	startAfterKey := c.absoluteKeyForManifestTimeRange(identity, startAfter)
	input := &s3.ListObjectsV2Input{
		Bucket:     &c.bucket,
		Delimiter:  aws.String("/"),
		Prefix:     &prefixKey,
		StartAfter: &startAfterKey,
	}

	notAfterKey := ""
	if notAfter > 0 {
		notAfterKey = c.absoluteKeyForManifestTimeRange(identity, notAfter)
	}
	attempts := 0
	for {
		var keys manifests.ManifestKeys
		err := c.s3Svc.ListObjectsV2PagesWithContext(ctx, input, func(output *s3.ListObjectsV2Output, b bool) bool {
			var done bool
			for _, commonPrefix := range output.CommonPrefixes {
				lgr.Debugw("list_manifests_saw_common_prefix", "prefix", commonPrefix.Prefix)
			}
			for _, obj := range output.Contents {
				key := *obj.Key
				if notAfterKey != "" && key > notAfterKey {
					done = true
				} else {
					name := filepath.Base(key)
					var manifestKey manifests.ManifestKey
					if err := manifestKey.PopulateFromFileName(name); err != nil {
						lgr.Warnw("list_manifests_ignoring_bad_filename", "name", name, "err", err)
					} else {
						keys = append(keys, manifestKey)
					}
				}
			}
			return !done
		})
		if err != nil {
			attempts++
			if ctxErr := ctx.Err(); ctxErr != nil {
				return nil, ctxErr
			}
			if IsNoSuchKey(err) || attempts > listManifestsRetriesLimit {
				return nil, err
			}
			lgr.Errorw("list_manifests_s3_error", "err", err, "attempts", attempts)
		} else {
			return keys, nil
		}
	}
}

func (c *Client) PutManifest(ctx context.Context, identity manifests.NodeIdentity, manifest manifests.Manifest) error {
	if manifest.ManifestType == manifests.ManifestTypeInvalid {
		panic("invalid manifest type")
	}
	absoluteKey := c.absoluteKeyForManifest(identity, manifest.Key())
	return c.putDocument(ctx, absoluteKey, manifest)
}

func (c *Client) GetManifests(ctx context.Context, identity manifests.NodeIdentity, keys manifests.ManifestKeys) ([]manifests.Manifest, error) {
	var results []manifests.Manifest
	for _, manifestKey := range keys {
		absoluteKey := c.absoluteKeyForManifest(identity, manifestKey)
		var m manifests.Manifest
		if err := c.getDocument(ctx, absoluteKey, &m); err != nil {
			zap.S().Errorw("get_manifest_error", "key", absoluteKey, "err", err)
			return nil, err
		}
		results = append(results, m)
	}
	return results, nil
}
