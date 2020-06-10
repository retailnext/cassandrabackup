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

package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
)

type cache struct {
	getHitsVec       *prometheus.CounterVec
	getMissesVec     *prometheus.CounterVec
	getPromotionsVec *prometheus.CounterVec
	putsVec          *prometheus.CounterVec
}

type bucket struct {
	SkippedBytes  prometheus.Counter
	SkippedFiles  prometheus.Counter
	UploadedBytes prometheus.Counter
	UploadedFiles prometheus.Counter
	UploadErrors  prometheus.Counter
}

type existsCache struct {
	ExistsCacheLockTimeMisses prometheus.Counter
}

type digest struct {
	HitBytesTotal    prometheus.Counter
	HitFilesTotal    prometheus.Counter
	MissBytesTotal   prometheus.Counter
	MissFilesTotal   prometheus.Counter
	MissSecondsTotal prometheus.Counter
}

type CacheCounters struct {
	Hits       prometheus.Counter
	Misses     prometheus.Counter
	Promotions prometheus.Counter
	Puts       prometheus.Counter
}

func NewCacheCounters(name string) *CacheCounters {
	return &CacheCounters{
		Hits:       Cache.getHitsVec.WithLabelValues(name),
		Misses:     Cache.getMissesVec.WithLabelValues(name),
		Promotions: Cache.getPromotionsVec.WithLabelValues(name),
		Puts:       Cache.putsVec.WithLabelValues(name),
	}
}

var (
	Bucket = bucket{
		SkippedBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "bucket",
			Name:      "skipped_bytes_total",
			Help:      "Total bytes not uploaded due to them already existing in the bucket.",
		}),
		SkippedFiles: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "bucket",
			Name:      "skipped_files_total",
			Help:      "Number of files not uploaded due to them already existing in the bucket.",
		}),
		UploadedBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "bucket",
			Name:      "upload_bytes_total",
			Help:      "Total bytes uploaded to the bucket.",
		}),
		UploadedFiles: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "bucket",
			Name:      "upload_files_total",
			Help:      "Number of files uploaded to the bucket.",
		}),
		UploadErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "bucket",
			Name:      "upload_errors_total",
			Help:      "Number of failed file uploads.",
		}),
	}

	Cache = cache{
		getHitsVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "cache",
			Name:      "get_hits_total",
			Help:      "Number of cache gets that were hits.",
		}, []string{"cache"}),
		getMissesVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "cache",
			Name:      "get_misses_total",
			Help:      "Number of cache gets that were misses.",
		}, []string{"cache"}),
		getPromotionsVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "cache",
			Name:      "promotions_total",
			Help:      "Number of cache gets that in promoting a value from the previous bucket.",
		}, []string{"cache"}),
		putsVec: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "cache",
			Name:      "puts_total",
			Help:      "Number of cache put requests.",
		}, []string{"cache"}),
	}

	ExistsCache = existsCache{
		ExistsCacheLockTimeMisses: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "bucket_exists_cache",
			Name:      "lock_time_misses_total",
			Help:      "Number of exists cache misses due to expired/future lock time.",
		}),
	}

	Digest = digest{
		HitBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "digestcache",
			Name:      "hit_bytes_total",
			Help:      "Total file size of digest requests processed that were a cache hit.",
		}),
		HitFilesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "digestcache",
			Name:      "hit_files_total",
			Help:      "Number of digest requests that were a cache hit.",
		}),
		MissBytesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "digestcache",
			Name:      "miss_bytes_total",
			Help:      "Total file size of digest requests processed that were a cache miss.",
		}),
		MissFilesTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "digestcache",
			Name:      "miss_files_total",
			Help:      "Number of digest requests that were a cache miss.",
		}),
		MissSecondsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "digestcache",
			Name:      "miss_seconds_total",
			Help:      "Total time spent calculating new digests.",
		}),
	}
)

func SetupPrometheus(metricsListenAddress, metricsPath *string) {
	if metricsListenAddress == nil || *metricsListenAddress == "" {
		return
	}
	go func() {
		http.Handle(*metricsPath, promhttp.Handler())
		err := http.ListenAndServe(*metricsListenAddress, nil)
		zap.S().Fatalw("metrics_listen_error", "err", err)
	}()
}

func init() {
	prometheus.MustRegister(Cache.getHitsVec)
	prometheus.MustRegister(Cache.getMissesVec)
	prometheus.MustRegister(Cache.getPromotionsVec)
	prometheus.MustRegister(Cache.putsVec)

	prometheus.MustRegister(Bucket.SkippedBytes)
	prometheus.MustRegister(Bucket.SkippedFiles)
	prometheus.MustRegister(Bucket.UploadedBytes)
	prometheus.MustRegister(Bucket.UploadedFiles)
	prometheus.MustRegister(Bucket.UploadErrors)

	prometheus.MustRegister(ExistsCache.ExistsCacheLockTimeMisses)

	prometheus.MustRegister(Digest.HitBytesTotal)
	prometheus.MustRegister(Digest.HitFilesTotal)
	prometheus.MustRegister(Digest.MissBytesTotal)
	prometheus.MustRegister(Digest.MissFilesTotal)
	prometheus.MustRegister(Digest.MissSecondsTotal)
}
