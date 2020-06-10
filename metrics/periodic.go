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

type periodic struct {
	LastBackupOkGauges      *prometheus.GaugeVec
	LastBackupAtGauges      *prometheus.GaugeVec
	BackupInProgressGauges  *prometheus.GaugeVec
	BackupErrorCounters     *prometheus.CounterVec
	BackupCompletedCounters *prometheus.CounterVec
	registerOnce            sync.Once
}

var (
	Periodic = periodic{
		LastBackupAtGauges: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cassandrabackup",
			Subsystem: "periodic",
			Name:      "last_at_seconds",
			Help:      "Time the last backup successfully completed.",
		}, []string{"type"}),
		LastBackupOkGauges: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cassandrabackup",
			Subsystem: "periodic",
			Name:      "last_ok",
			Help:      "1 if the last backup completed successfully.",
		}, []string{"type"}),
		BackupInProgressGauges: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: "cassandrabackup",
			Subsystem: "periodic",
			Name:      "in_progress",
			Help:      "1 if a backup is in progress.",
		}, []string{"type"}),
		BackupErrorCounters: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "periodic",
			Name:      "errors_total",
			Help:      "Number of failed backups.",
		}, []string{"type"}),
		BackupCompletedCounters: prometheus.NewCounterVec(prometheus.CounterOpts{
			Namespace: "cassandrabackup",
			Subsystem: "periodic",
			Name:      "completed_total",
			Help:      "Number of completed backups.",
		}, []string{"type"}),
	}
)

func (c *periodic) RegisterMetrics() {
	c.registerOnce.Do(func() {
		prometheus.MustRegister(c.BackupCompletedCounters)
		prometheus.MustRegister(c.BackupErrorCounters)
		prometheus.MustRegister(c.BackupInProgressGauges)
		prometheus.MustRegister(c.LastBackupAtGauges)
		prometheus.MustRegister(c.LastBackupOkGauges)

		// reify everything
		c.BackupErrorCounters.WithLabelValues("incremental")
		c.BackupErrorCounters.WithLabelValues("snapshot")
		c.BackupCompletedCounters.WithLabelValues("incremental")
		c.BackupCompletedCounters.WithLabelValues("snapshot")
		c.BackupInProgressGauges.WithLabelValues("incremental").Set(0)
		c.BackupInProgressGauges.WithLabelValues("snapshot").Set(0)
		c.LastBackupAtGauges.WithLabelValues("incremental").Set(0)
		c.LastBackupAtGauges.WithLabelValues("snapshot").Set(0)
		c.LastBackupOkGauges.WithLabelValues("incremental").Set(0)
		c.LastBackupOkGauges.WithLabelValues("snapshot").Set(0)
	})
}
