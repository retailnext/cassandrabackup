// Copyright 2023 RetailNext, Inc.
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

import "github.com/alecthomas/kingpin/v2"

var (
	Cmd = kingpin.Command("restore", "")

	HostCmd    = Cmd.Command("host", "Restore this host from backup")
	ClusterCmd = Cmd.Command("cluster", "Download from multiple hosts' backups")

	hostCmdDryRun            = HostCmd.Flag("dry-run", "Don't actually download files").Bool()
	hostCmdAllowChangedFiles = HostCmd.Flag("allow-changed", "Allow restoration of files that changed between manifests").Bool()
	hostCmdNotBefore         = HostCmd.Flag("not-before", "Ignore manifests before this time (unix seconds)").Int64()
	hostCmdNotAfter          = HostCmd.Flag("not-after", "Ignore manifests after this time (unix seconds)").Int64()
	hostCmdCluster           = HostCmd.Flag("cluster", "Use a different cluster name when selecting a backup to restore.").String()
	hostCmdHostname          = HostCmd.Flag("hostname", "Use a specific hostname when selecting a backup to restore.").String()
	hostCmdHostnamePattern   = HostCmd.Flag("hostname-pattern", "Use a prefix pattern when selecting a backup to restore.").String()

	clusterCmdDryRun          = ClusterCmd.Flag("dry-run", "Don't actually download files").Bool()
	clusterCmdTargetDirectory = ClusterCmd.Flag("target", "A subdirectory will be created under this for each host.").Required().String()
	clusterCmdNotBefore       = ClusterCmd.Flag("not-before", "Ignore manifests before this time (unix seconds)").Int64()
	clusterCmdNotAfter        = ClusterCmd.Flag("not-after", "Ignore manifests after this time (unix seconds)").Int64()
	clusterCmdCluster         = ClusterCmd.Flag("cluster", "Download files for hosts in this cluster").Required().String()
	clusterCmdHostnamePattern = ClusterCmd.Flag("hostname-pattern", "Download for hosts matching this prefix.").Required().String()
	clusterCmdTables          = ClusterCmd.Flag("table", "Download files for these tables (keyspace.table)").Required().Strings()
	clusterCmdSkipIndexes     = ClusterCmd.Flag("skip-indexes", "Skip downloading indexes").Default("True").Bool()
)
