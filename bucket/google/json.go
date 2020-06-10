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
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"time"

	"github.com/mailru/easyjson"
	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/manifests"
	"go.uber.org/zap"
)

func (c *gcsClient) PutManifest(ctx context.Context, identity manifests.NodeIdentity, manifest manifests.Manifest) error {
	var encodeBuffer bytes.Buffer
	gzipWriter := gzip.NewWriter(&encodeBuffer)
	if _, err := easyjson.MarshalToWriter(manifest, gzipWriter); err != nil {
		panic(err)
	}
	if err := gzipWriter.Close(); err != nil {
		panic(err)
	}

	absoluteKey := c.keyStore.AbsoluteKeyForManifest(identity, manifest.Key())
	attempts := 0
	for {
		wc := c.storageClient.Bucket(c.keyStore.Bucket).Object(absoluteKey).NewWriter(ctx)
		wc.ObjectAttrs.ContentType = "application/json"
		wc.ObjectAttrs.ContentEncoding = "gzip"

		if _, err := io.Copy(wc, bytes.NewReader(encodeBuffer.Bytes())); err != nil {
			wc.Close()
			attempts++
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			if attempts > config.PutJsonRetriesLimit {
				return err
			}
			zap.S().Warnw("put_manifest_error", "err", err, "attempts", attempts)
			time.Sleep(time.Duration(attempts) * config.RetrySleepPerAttempt)
		} else {
			if err := wc.Close(); err != nil {
				return err
			}
			return nil
		}
	}
}

func (c *gcsClient) GetManifest(ctx context.Context, identity manifests.NodeIdentity, manifestKey manifests.ManifestKey) (manifests.Manifest, error) {
	absoluteKey := c.keyStore.AbsoluteKeyForManifest(identity, manifestKey)
	attempts := 0
	for {
		rc, err := c.storageClient.Bucket(c.keyStore.Bucket).Object(absoluteKey).NewReader(ctx)
		if err != nil {
			rc.Close()
			attempts++
			if ctxErr := ctx.Err(); ctxErr != nil {
				return manifests.Manifest{}, ctxErr
			}
			if attempts > config.GetJsonRetriesLimit {
				return manifests.Manifest{}, err
			}
			zap.S().Warnw("get_manifest_error", "err", err, "attempts", attempts)
			time.Sleep(time.Duration(attempts) * config.RetrySleepPerAttempt)
		} else {
			var manifest manifests.Manifest
			defer rc.Close()
			return manifest, easyjson.UnmarshalFromReader(rc, &manifest)
		}
	}
}
