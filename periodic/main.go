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
	"cassandrabackup/backup"
	"context"
	"time"

	"go.uber.org/zap"
)

const snapshotEvery = 1 * time.Hour
const incrementalEvery = 5 * time.Minute

func Main(ctx context.Context, cluster string, cleanIncremental bool) error {
	registerMetrics()
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
		case _ = <-doneCh:
			err = ctx.Err()
			break DONE
		case _ = <-everyMinute.C:
		}

		now := time.Now()
		if lastIncrementalAt.Before(now.Add(-incrementalEvery)) {
			backupInProgressGauges.WithLabelValues("incremental").Set(1)
			lgr.Infow("starting_backup", "type", "incremental")
			err = backup.DoIncremental(ctx, cleanIncremental, cluster)
			backupInProgressGauges.WithLabelValues("incremental").Set(0)
			now = time.Now()
			if err == nil {
				lastIncrementalAt = now
				lastBackupAtGauges.WithLabelValues("incremental").Set(float64(now.Unix()))
				lastBackupOkGauges.WithLabelValues("incremental").Set(1)
				backupCompletedCounters.WithLabelValues("incremental").Inc()
				lgr.Infow("backup_complete", "type", "incremental")

			} else {
				lastBackupOkGauges.WithLabelValues("incremental").Set(0)
				lgr.Errorw("backup_error", "type", "incremental", "err", err)
				backupErrorCounters.WithLabelValues("incremental").Inc()
			}
		} else if lastSnapshotAt.Before(now.Add(-snapshotEvery)) {
			backupInProgressGauges.WithLabelValues("snapshot").Set(1)
			lgr.Infow("starting_backup", "type", "snapshot")
			err = backup.DoSnapshotBackup(ctx, cluster)
			backupInProgressGauges.WithLabelValues("snapshot").Set(0)
			now = time.Now()
			if err == nil {
				lastSnapshotAt = now
				lastBackupAtGauges.WithLabelValues("snapshot").Set(float64(now.Unix()))
				lastBackupOkGauges.WithLabelValues("snapshot").Set(1)
				backupCompletedCounters.WithLabelValues("snapshot").Inc()
				lgr.Infow("backup_complete", "type", "snapshot")
			} else {
				lastBackupOkGauges.WithLabelValues("snapshot").Set(0)
				backupErrorCounters.WithLabelValues("snapshot").Inc()
				lgr.Errorw("backup_error", "type", "snapshot", "err", err)
			}
		}
	}
	return err
}
