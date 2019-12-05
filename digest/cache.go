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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/retailnext/cassandrabackup/cache"
	"github.com/retailnext/cassandrabackup/paranoid"
)

type Cache struct {
	c *cache.Cache
}

func OpenShared() *Cache {
	cache.OpenShared()
	return &Cache{
		c: cache.Shared.Cache(cacheName),
	}
}

const cacheName = "digests"

func (c *Cache) Get(ctx context.Context, file paranoid.File) (ForUpload, error) {
	key := file.CacheKey()
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
		hitFilesTotal.Inc()
		hitBytesTotal.Add(float64(file.Len()))
		return result, nil
	case cache.NotFound, cache.DoNotPromote:
	default:
		return ForUpload{}, getErr
	}

	t0 := time.Now()
	result = ForUpload{}
	if populateErr := result.populate(ctx, file); populateErr != nil {
		return ForUpload{}, populateErr
	}
	missFilesTotal.Inc()
	missBytesTotal.Add(float64(file.Len()))
	missSecondsTotal.Add(time.Since(t0).Seconds())

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

var (
	hitFilesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "digestcache",
		Name:      "hit_files_total",
		Help:      "Number of digest requests that were a cache hit.",
	})
	hitBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "digestcache",
		Name:      "hit_bytes_total",
		Help:      "Total file size of digest requests processed that were a cache hit.",
	})
	missFilesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "digestcache",
		Name:      "miss_files_total",
		Help:      "Number of digest requests that were a cache miss.",
	})
	missBytesTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "digestcache",
		Name:      "miss_bytes_total",
		Help:      "Total file size of digest requests processed that were a cache miss.",
	})
	missSecondsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "digestcache",
		Name:      "miss_seconds_total",
		Help:      "Total time spent calculating new digests.",
	})
)

func init() {
	prometheus.MustRegister(hitBytesTotal)
	prometheus.MustRegister(hitFilesTotal)
	prometheus.MustRegister(missBytesTotal)
	prometheus.MustRegister(missFilesTotal)
	prometheus.MustRegister(missSecondsTotal)
}
