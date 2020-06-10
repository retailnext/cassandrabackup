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
	"time"

	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/cache"
	"github.com/retailnext/cassandrabackup/metrics"
	"github.com/retailnext/cassandrabackup/paranoid"
)

type Cache struct {
	c *cache.Cache
	p config.CloudProvider
}

func OpenShared(cfg config.Config) *Cache {
	cache.OpenShared(cfg)
	return &Cache{
		c: cache.Shared.Cache(cacheName),
		p: cfg.Provider,
	}
}

const cacheName = "digests"

func (c *Cache) Get(ctx context.Context, file paranoid.File) (ForUpload, error) {
	key := file.CacheKey()
	if !c.p.IsAWS() {
		key = append(key, byte(c.p))
	}
	var result ForUpload

	getErr := c.c.Get(key, func(wrapped []byte) error {
		if unwrapped := file.UnwrapCacheEntry(key, wrapped); unwrapped != nil {
			var maybeResult ForUpload
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
		return ForUpload{}, getErr
	}

	t0 := time.Now()
	result = ForUpload{}
	if populateErr := result.populate(ctx, file, c.p); populateErr != nil {
		return ForUpload{}, populateErr
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
		return ForUpload{}, putErr
	}
	return result, nil
}
