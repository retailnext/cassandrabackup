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

package cache

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.etcd.io/bbolt"
	"go.uber.org/zap"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	DoNotPromote = errors.New("do not promote")
	NotFound     = errors.New("not found")
)

var (
	Shared *Storage
	once   sync.Once

	sharedCacheFile = kingpin.Flag("cache.file", "Location of local cache file.").Required().String()
)

func Open(path string, mode os.FileMode) (*Storage, error) {
	db, err := bbolt.Open(path, mode, nil)
	if err != nil {
		return nil, err
	}
	ensureFileOwnership(path)
	s := &Storage{
		db:           db,
		bucketPeriod: 1 << 20, // ~12 days
	}
	return s, nil
}

// ensureFileOwnership keeps the database owned by the same uid/gid as the containing directory.
// Without this, running the tool as root to restore can make the cache db unusable by the user it normally runs as.
func ensureFileOwnership(path string) {
	if os.Geteuid() != 0 {
		return
	}
	lgr := zap.S()
	dbInfo, err := os.Stat(path)
	if err != nil {
		lgr.Errorw("cache_db_stat_error", "err", err)
		return
	}
	parent := filepath.Dir(path)
	parentInfo, err := os.Stat(parent)
	if err != nil {
		lgr.Errorw("cache_db_stat_error", "err", err)
		return
	}
	dbStat, ok := dbInfo.Sys().(*syscall.Stat_t)
	if !ok {
		lgr.Warnw("cache_db_stat_unsupported")
		return
	}
	parentStat, ok := parentInfo.Sys().(*syscall.Stat_t)
	if !ok {
		lgr.Warnw("cache_db_stat_unsupported")
		return
	}
	if dbStat.Uid != parentStat.Uid || dbStat.Gid != parentStat.Gid {
		err = os.Chown(path, int(parentStat.Uid), int(parentStat.Gid))
		if err != nil {
			lgr.Errorw("cache_db_chown_error", "err", err)
		} else {
			lgr.Infow("cache_db_chown_ok", "uid", parentStat.Uid, "gid", parentStat.Gid)
		}
	}
}

func OpenShared() {
	once.Do(func() {
		c, err := Open(*sharedCacheFile, 0644)
		if err != nil {
			panic(err)
		}
		Shared = c
	})
}

type Storage struct {
	db           *bbolt.DB
	bucketPeriod int64
}

func (s *Storage) Close() error {
	if s == nil {
		return nil
	}
	return s.db.Close()
}

type Cache struct {
	storage *Storage
	name    []byte

	hits       prometheus.Counter
	misses     prometheus.Counter
	promotions prometheus.Counter
	puts       prometheus.Counter
}

func (s *Storage) Cache(name string) *Cache {
	return &Cache{
		storage: s,
		name:    []byte(name),

		hits:       getHitsVec.WithLabelValues(name),
		misses:     getMissesVec.WithLabelValues(name),
		promotions: getPromotionsVec.WithLabelValues(name),
		puts:       putsVec.WithLabelValues(name),
	}
}

type WithValueFunc func(value []byte) error

func (c *Cache) Get(key []byte, f WithValueFunc) error {
	var valueToPromote []byte
	viewErr := c.storage.db.View(func(tx *bbolt.Tx) error {
		currentTop, previousTop := c.storage.currentAndPreviousTopBuckets()
		if topBucket := tx.Bucket(currentTop); topBucket != nil {
			if bucket := topBucket.Bucket(c.name); bucket != nil {
				if value := bucket.Get(key); value != nil {
					return f(value)
				}
			}
		}
		if topBucket := tx.Bucket(previousTop); topBucket != nil {
			if bucket := topBucket.Bucket(c.name); bucket != nil {
				if value := bucket.Get(key); value != nil {
					valueToPromote = make([]byte, len(value))
					copy(valueToPromote, value)
					return f(value)
				}
			}
		}
		return NotFound
	})
	if viewErr != nil {
		c.misses.Inc()
		return viewErr
	}
	c.hits.Inc()
	if valueToPromote == nil {
		return nil
	}
	c.promotions.Inc()
	return c.put(key, valueToPromote)
}

func (c *Cache) Put(key, value []byte) error {
	c.puts.Inc()
	return c.put(key, value)
}

func (c *Cache) put(key, value []byte) error {
	lgr := zap.S()
	return c.storage.db.Update(func(tx *bbolt.Tx) error {
		currentTop, previousTop := c.storage.currentAndPreviousTopBuckets()
		topBucket := tx.Bucket(currentTop)
		if topBucket == nil {
			// Current (time-based) top bucket does not exist. Purge old ones then create it.

			var topBucketsToDelete [][]byte
			iterBucketsErr := tx.ForEach(func(topBucketName []byte, b *bbolt.Bucket) error {
				if bytes.Equal(topBucketName, currentTop) || bytes.Equal(topBucketName, previousTop) {
					return nil
				}
				topBucketsToDelete = append(topBucketsToDelete, topBucketName)
				return nil
			})
			if iterBucketsErr != nil {
				return iterBucketsErr
			}
			for _, topBucketName := range topBucketsToDelete {
				if err := tx.DeleteBucket(topBucketName); err != nil {
					return err
				}
				lgr.Debugw("cache_periodic_bucket_removed", "periodic", topBucketName)

			}

			if maybeTopBucket, err := tx.CreateBucket(currentTop); err != nil {
				return err
			} else {
				lgr.Debugw("cache_periodic_bucket_created", "periodic", currentTop)
				topBucket = maybeTopBucket
			}
		}

		bucket := topBucket.Bucket(c.name)
		if bucket == nil {
			if newBucket, err := topBucket.CreateBucket(c.name); err != nil {
				return err
			} else {
				lgr.Infow("cache_bucket_created", "periodic", currentTop, "cache", string(c.name))
				bucket = newBucket
			}
		}
		return bucket.Put(key, value)
	})
}

func (s *Storage) currentAndPreviousTopBuckets() ([]byte, []byte) {
	var now = time.Now().Unix()
	currentTs := (now / s.bucketPeriod) * s.bucketPeriod
	previousTs := currentTs - s.bucketPeriod

	current := make([]byte, 8)
	binary.BigEndian.PutUint64(current, uint64(currentTs))

	previous := make([]byte, 8)
	binary.BigEndian.PutUint64(previous, uint64(previousTs))
	return current, previous
}

var (
	getHitsVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "cache",
		Name:      "get_hits_total",
		Help:      "Number of cache gets that were hits.",
	}, []string{"cache"})
	getMissesVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "cache",
		Name:      "get_misses_total",
		Help:      "Number of cache gets that were misses.",
	}, []string{"cache"})
	getPromotionsVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "cache",
		Name:      "promotions_total",
		Help:      "Number of cache gets that in promoting a value from the previous bucket.",
	}, []string{"cache"})
	putsVec = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "cache",
		Name:      "puts_total",
		Help:      "Number of cache put requests.",
	}, []string{"cache"})
)

func init() {
	prometheus.MustRegister(getHitsVec)
	prometheus.MustRegister(getMissesVec)
	prometheus.MustRegister(getPromotionsVec)
	prometheus.MustRegister(putsVec)
}
