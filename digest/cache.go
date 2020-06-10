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

package digest

import (
	"context"
	"fmt"
	"time"

	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/cache"
	"github.com/retailnext/cassandrabackup/metrics"
	"github.com/retailnext/cassandrabackup/paranoid"
	"go.uber.org/zap"
)

type Cache struct {
	c *cache.Cache
	f ForUploadFactory
}

func newForUploadFactory(config *config.Config) ForUploadFactory {
	if config.IsAWS() {
		return &awsForUploadFactory{}
	} else if config.IsGCS() {
		return &gcsForUploadFactory{}
	} else {
		err := fmt.Errorf("cloud provider not supported: %s", config.Provider)
		zap.S().Fatalw("cloud_provider_error", "err", err)
		return nil
	}
}

func OpenShared(config *config.Config) *Cache {
	cache.OpenShared(config)
	return &Cache{
		c: cache.Shared.Cache(cacheName),
		f: newForUploadFactory(config),
	}
}

const cacheName = "digests"

func (c *Cache) CreateForUpload() ForUpload {
	return c.f.CreateForUpload()
}

func (c *Cache) Get(ctx context.Context, file paranoid.File) (ForUpload, error) {
	key := file.CacheKey()
	var result ForUpload

	getErr := c.c.Get(key, func(wrapped []byte) error {
		if unwrapped := file.UnwrapCacheEntry(key, wrapped); unwrapped != nil {
			maybeResult := c.CreateForUpload()
			if err := maybeResult.UnmarshalBinary(unwrapped); err == nil {
				result = maybeResult
				return nil
			} else {
				return cache.DoNotPromote
			}
		} else {
			return cache.DoNotPromote
		}
	})

	switch getErr {
	case nil:
		metrics.Digest.HitFilesTotal.Inc()
		metrics.Digest.HitBytesTotal.Add(float64(file.Len()))
		return result, nil
	case cache.NotFound, cache.DoNotPromote:
	default:
		return nil, getErr
	}

	t0 := time.Now()
	result = c.CreateForUpload()
	if populateErr := result.Populate(ctx, file); populateErr != nil {
		return nil, populateErr
	}
	metrics.Digest.MissFilesTotal.Inc()
	metrics.Digest.MissBytesTotal.Add(float64(file.Len()))
	metrics.Digest.MissSecondsTotal.Add(time.Since(t0).Seconds())

	marshalled, marshalErr := result.MarshalBinary()
	if marshalErr != nil {
		panic(marshalErr)
	}
	wrapped := file.WrapCacheEntry(marshalled)
	if putErr := c.c.Put(key, wrapped); putErr != nil {
		return nil, putErr
	}
	return result, nil
}
