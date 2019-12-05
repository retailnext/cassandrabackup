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

package restore

import (
	"context"
	"errors"

	"github.com/retailnext/cassandrabackup/manifests"
	"github.com/retailnext/cassandrabackup/nodeidentity"
	"github.com/retailnext/cassandrabackup/restore/plan"
	"github.com/retailnext/cassandrabackup/unixtime"
	"go.uber.org/zap"
)

var NoSnapshotsFound = errors.New("no snapshots found for host")
var NoBackupsFound = errors.New("no backups found for host")
var ChangesDetected = errors.New("file changes detected")

func RestoreHost(ctx context.Context) error {
	identity := nodeidentity.ForRestore(ctx, hostCmdCluster, hostCmdHostname, hostCmdHostnamePattern)
	lgr := zap.S().With("identity", identity)

	nodePlan, err := plan.Create(ctx, identity, unixtime.Seconds(*hostCmdNotBefore), unixtime.Seconds(*hostCmdNotAfter))
	if err != nil {
		return err
	}

	if len(nodePlan.SelectedManifests) == 0 {
		return NoBackupsFound
	}
	if nodePlan.SelectedManifests[0].ManifestType != manifests.ManifestTypeSnapshot {
		return NoSnapshotsFound
	}

	lgr.Infow("selected_manifests", "base", nodePlan.SelectedManifests[0], "additional", nodePlan.SelectedManifests[1:])

	if len(nodePlan.ChangedFiles) > 0 {
		for name, history := range nodePlan.ChangedFiles {
			for _, entry := range history {
				lgr.Infow("file_changed", "name", name, "digest", entry.Digest, "manifest", entry.Manifest)
			}
		}
		if !*hostCmdAllowChangedFiles {
			return ChangesDetected
		}
	}

	if *hostCmdDryRun {
		for name, file := range nodePlan.Files {
			lgr.Infow("would_download", "name", name, "digest", file)
		}
		return nil
	}

	w := newWorker("/var/lib/cassandra/data", true)
	return w.restoreFiles(ctx, nodePlan.Files)
}
