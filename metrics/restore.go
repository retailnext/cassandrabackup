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
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

type restore struct {
	SkippedFiles    prometheus.Counter
	SkippedBytes    prometheus.Counter
	DownloadSeconds prometheus.Counter
	DownloadFiles   prometheus.Counter
	DownloadBytes   prometheus.Counter
	DownloadErrors  prometheus.Counter
	registerOnce    sync.Once
}

var (
	Restore = restore{
		SkippedFiles: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "restore",
			Name:      "skipped_files_total",
			Help:      "Number of files skipped during restore due to already being on disk.",
		}),
		SkippedBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "restore",
			Name:      "skipped_bytes_total",
			Help:      "Number of bytes skipped during restore due to already being on disk.",
		}),
		DownloadFiles: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "restore",
			Name:      "download_files_total",
			Help:      "Number of files downloaded during the restore.",
		}),
		DownloadBytes: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "restore",
			Name:      "download_bytes_total",
			Help:      "Number of bytes downloaded during the restore.",
		}),
		DownloadErrors: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "restore",
			Name:      "download_errors_total",
			Help:      "Number files that failed to download during the restore.",
		}),
		DownloadSeconds: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "restore",
			Name:      "download_seconds_total",
			Help:      "Time spent downloading files during the restore.",
		}),
	}
)

func (c *restore) RegisterMetrics() {
	c.registerOnce.Do(func() {
		prometheus.MustRegister(c.SkippedFiles)
		prometheus.MustRegister(c.SkippedBytes)
		prometheus.MustRegister(c.DownloadSeconds)
		prometheus.MustRegister(c.DownloadFiles)
		prometheus.MustRegister(c.DownloadBytes)
		prometheus.MustRegister(c.DownloadErrors)
	})
}
