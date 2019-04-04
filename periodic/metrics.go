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

package periodic

import (
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	lastBackupAtGauges = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cassandrabackup",
		Subsystem: "periodic",
		Name:      "last_at_seconds",
		Help:      "Time the last backup successfully completed.",
	}, []string{"type"})
	lastBackupOkGauges = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cassandrabackup",
		Subsystem: "periodic",
		Name:      "last_ok",
		Help:      "1 if the last backup completed successfully.",
	}, []string{"type"})
	backupInProgressGauges = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "cassandrabackup",
		Subsystem: "periodic",
		Name:      "in_progress",
		Help:      "1 if a backup is in progress.",
	}, []string{"type"})
	backupErrorCounters = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "periodic",
		Name:      "errors_total",
		Help:      "Number of failed backups.",
	}, []string{"type"})
	backupCompletedCounters = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "cassandrabackup",
		Subsystem: "periodic",
		Name:      "completed_total",
		Help:      "Number of completed backups.",
	}, []string{"type"})

	registerOnce sync.Once
)

func registerMetrics() {
	registerOnce.Do(func() {
		prometheus.MustRegister(backupCompletedCounters)
		prometheus.MustRegister(backupErrorCounters)
		prometheus.MustRegister(backupInProgressGauges)
		prometheus.MustRegister(lastBackupAtGauges)
		prometheus.MustRegister(lastBackupOkGauges)

		// reify everything
		backupErrorCounters.WithLabelValues("incremental")
		backupErrorCounters.WithLabelValues("snapshot")
		backupCompletedCounters.WithLabelValues("incremental")
		backupCompletedCounters.WithLabelValues("snapshot")
		backupInProgressGauges.WithLabelValues("incremental").Set(0)
		backupInProgressGauges.WithLabelValues("snapshot").Set(0)
		lastBackupAtGauges.WithLabelValues("incremental").Set(0)
		lastBackupAtGauges.WithLabelValues("snapshot").Set(0)
		lastBackupOkGauges.WithLabelValues("incremental").Set(0)
		lastBackupOkGauges.WithLabelValues("snapshot").Set(0)
	})
}
