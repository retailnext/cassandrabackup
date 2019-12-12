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
	"regexp"

	"github.com/retailnext/cassandrabackup/manifests"
	"github.com/retailnext/cassandrabackup/nodeidentity"
	"github.com/retailnext/cassandrabackup/restore/plan"
	"github.com/retailnext/cassandrabackup/unixtime"
	"go.uber.org/zap"
)

func RestoreCluster(ctx context.Context) error {
	lgr := zap.S()

	filter := plan.Filter{
		IncludeIndexes: !*clusterCmdSkipIndexes,
	}
	filter.Build(*clusterCmdTables)

	identities := nodeIdentitiesForCluster(ctx, clusterCmdCluster, clusterCmdHostnamePattern)
	lgr.Infow("selected_hosts", "identities", identities)

	var dp downloadPlan
	for _, hostIdentity := range identities {
		hostLgr := lgr.With("identity", hostIdentity)

		nodePlan, err := plan.Create(ctx, hostIdentity, unixtime.Seconds(*clusterCmdNotBefore), unixtime.Seconds(*clusterCmdNotAfter))
		if err != nil {
			return err
		}
		if len(nodePlan.SelectedManifests) == 0 {
			hostLgr.Warnw("no_backups_found")
			continue
		}
		if nodePlan.SelectedManifests[0].ManifestType != manifests.ManifestTypeSnapshot {
			hostLgr.Warnw("no_snapshots_found")
			continue
		}
		hostLgr.Infow("selected_manifests", "base", nodePlan.SelectedManifests[0], "additional", nodePlan.SelectedManifests[1:])

		nodePlan.Filter(filter)

		dp.addHost(hostIdentity.Hostname, nodePlan)
	}

	files := dp.includeChanged("PREVIOUS_VERSIONS")

	if *clusterCmdDryRun {
		for name, file := range files {
			lgr.Infow("would_download", "name", name, "digest", file)
		}
		return nil
	}

	w := newWorker(*clusterCmdTargetDirectory, false)
	return w.restoreFiles(ctx, files)
}

func nodeIdentitiesForCluster(ctx context.Context, cluster, prefix *string) []manifests.NodeIdentity {
	expr := regexp.MustCompile("^" + regexp.QuoteMeta(*prefix) + ".+$")
	return nodeidentity.ForRestoreMatchingRegexp(ctx, *cluster, expr)
}
