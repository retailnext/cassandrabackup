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

package existscache

import (
	"time"

	"github.com/retailnext/cassandrabackup/cache"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/metrics"
	"github.com/retailnext/cassandrabackup/unixtime"
	"go.uber.org/zap"
)

const objectLockSafetyMargin = 12 * time.Hour

type ExistsCache struct {
	cache *cache.Cache
}

func NewExistsCache() *ExistsCache {
	return &ExistsCache{
		cache: cache.Shared.Cache("bucket_exists"),
	}
}

func (e *ExistsCache) Get(restore digest.ForRestore) bool {
	var exists bool
	key, err := restore.MarshalBinary()
	if err != nil {
		panic(err)
	}
	err = e.cache.Get(key, func(value []byte) error {
		var lockedUntil unixtime.Seconds
		if err := lockedUntil.UnmarshalBinary(value); err != nil {
			return err
		}
		if time.Now().Add(objectLockSafetyMargin).Unix() < int64(lockedUntil) {
			exists = true
			return nil
		} else {
			metrics.ExistsCache.ExistsCacheLockTimeMisses.Inc()
		}
		return cache.DoNotPromote
	})
	if err != nil {
		switch err {
		case cache.NotFound, cache.DoNotPromote:
		default:
			zap.S().Warnw("blob_exists_cache_get_error", "key", restore, "err", err)
		}
	}
	return exists
}

func (e *ExistsCache) Put(restore digest.ForRestore, lockedUntil time.Time) {
	key, err := restore.MarshalBinary()
	if err != nil {
		panic(err)
	}
	seconds := unixtime.Seconds(lockedUntil.Unix())
	value, err := seconds.MarshalBinary()
	if err != nil {
		panic(err)
	}
	err = e.cache.Put(key, value)
	if err != nil {
		zap.S().Warnw("blob_exists_cache_put_error", "key", restore, "err", err)
	}
}
