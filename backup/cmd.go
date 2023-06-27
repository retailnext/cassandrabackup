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

package backup

import "github.com/alecthomas/kingpin/v2"

var (
	Cmd = kingpin.Command("backup", "")

	_ = Cmd.Command("incremental", "Make an incremental backup.")
	_ = Cmd.Command("snapshot", "Make a snapshot backup.")
	_ = Cmd.Command("run", "Make incremental and snapshot backups on a schedule. (Foreground Daemon)")

	overrideCluster    = Cmd.Flag("cluster", "Override cluster name when storing backups.").String()
	overrideHostname   = Cmd.Flag("hostname", "Override hostname when storing backups.").String()
	noCleanIncremental = Cmd.Flag("no-clean-incremental", "Do not clean up incremental backup files.").Bool()
	verboseClean       = Cmd.Flag("verbose-clean", "Log incremental backup files that are or would be removed.").Bool()
)
