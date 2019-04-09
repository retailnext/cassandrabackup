// Copyright (c) 2019, RetailNext, Inc.
// This material contains trade secrets and confidential information of
// RetailNext, Inc.  Any use, reproduction, disclosure or dissemination
// is strictly prohibited without the explicit written permission
// of RetailNext, Inc.
// All rights reserved.

package restore

import "gopkg.in/alecthomas/kingpin.v2"

var (
	Cmd = kingpin.Command("restore", "Restore from backup")

	restoreCmdDryRun            = Cmd.Flag("dry-run", "Don't actually download files").Bool()
	restoreCmdAllowChangedFiles = Cmd.Flag("allow-changed", "Allow restoration of files that changed between manifests").Bool()
	restoreCmdNotBefore         = Cmd.Flag("not-before", "Ignore manifests before this time (unix seconds)").Int64()
	restoreCmdNotAfter          = Cmd.Flag("not-after", "Ignore manifests after this time (unix seconds)").Int64()
	restoreCluster              = Cmd.Flag("cluster", "Use a different cluster name when selecting a backup to restore.").String()
	restoreHostname             = Cmd.Flag("hostname", "Use a specific hostname when selecting a backup to restore.").String()
	restoreHostnamePattern      = Cmd.Flag("hostname-pattern", "Use a prefix pattern when selecting a backup to restore.").String()
)
