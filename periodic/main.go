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

package periodic

import (
	"context"
	"time"

	"github.com/retailnext/cassandrabackup/backup"
	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/metrics"
	"go.uber.org/zap"
)

const snapshotEvery = 1 * time.Hour
const incrementalEvery = 5 * time.Minute

func Main(ctx context.Context, cfg config.Config) error {
	metrics.Periodic.RegisterMetrics()
	lgr := zap.S()

	var lastSnapshotAt time.Time
	var lastIncrementalAt time.Time
	everyMinute := time.NewTicker(time.Minute)
	defer everyMinute.Stop()
	doneCh := ctx.Done()

	var err error
DONE:
	for {
		select {
		case <-doneCh:
			err = ctx.Err()
			break DONE
		case <-everyMinute.C:
		}

		now := time.Now()
		if lastIncrementalAt.Before(now.Add(-incrementalEvery)) {
			metrics.Periodic.BackupInProgressGauges.WithLabelValues("incremental").Set(1)
			lgr.Infow("starting_backup", "type", "incremental")
			err = backup.DoIncremental(ctx, cfg)
			metrics.Periodic.BackupInProgressGauges.WithLabelValues("incremental").Set(0)
			now = time.Now()
			if err == nil {
				lastIncrementalAt = now
				metrics.Periodic.LastBackupAtGauges.WithLabelValues("incremental").Set(float64(now.Unix()))
				metrics.Periodic.LastBackupOkGauges.WithLabelValues("incremental").Set(1)
				metrics.Periodic.BackupCompletedCounters.WithLabelValues("incremental").Inc()
				lgr.Infow("backup_complete", "type", "incremental")

			} else {
				metrics.Periodic.LastBackupOkGauges.WithLabelValues("incremental").Set(0)
				lgr.Errorw("backup_error", "type", "incremental", "err", err)
				metrics.Periodic.BackupErrorCounters.WithLabelValues("incremental").Inc()
			}
		} else if lastSnapshotAt.Before(now.Add(-snapshotEvery)) {
			metrics.Periodic.BackupInProgressGauges.WithLabelValues("snapshot").Set(1)
			lgr.Infow("starting_backup", "type", "snapshot")
			err = backup.DoSnapshotBackup(ctx, cfg)
			metrics.Periodic.BackupInProgressGauges.WithLabelValues("snapshot").Set(0)
			now = time.Now()
			if err == nil {
				lastSnapshotAt = now
				metrics.Periodic.LastBackupAtGauges.WithLabelValues("snapshot").Set(float64(now.Unix()))
				metrics.Periodic.LastBackupOkGauges.WithLabelValues("snapshot").Set(1)
				metrics.Periodic.BackupCompletedCounters.WithLabelValues("snapshot").Inc()
				lgr.Infow("backup_complete", "type", "snapshot")
			} else {
				metrics.Periodic.LastBackupOkGauges.WithLabelValues("snapshot").Set(0)
				metrics.Periodic.BackupErrorCounters.WithLabelValues("snapshot").Inc()
				lgr.Errorw("backup_error", "type", "snapshot", "err", err)
			}
		}
	}
	return err
}
