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

package systemlocal

import (
	"cassandrabackup/manifests"
	"os"
	"sort"
	"strings"

	"go.uber.org/zap"
)

func GetIdentityAndSkeletonManifest(cluster string, offline bool) (manifests.NodeIdentity, manifests.Manifest, error) {
	lgr := zap.S()

	cfg, err := LoadCassandraConfig()
	if err != nil {
		lgr.Panicw("read_cassandra_config_error", "err", err)
	}

	fqdn, err := os.Hostname()
	if err != nil {
		lgr.Panicw("os_hostname_error", "err", err)
	}
	hostnameParts := strings.Split(fqdn, ".")
	hostname := hostnameParts[0]

	identity := manifests.NodeIdentity{
		Cluster:  cfg.ClusterName,
		Hostname: hostname,
	}
	manifest := manifests.Manifest{
		Address:     cfg.ListenAddress,
		Partitioner: cfg.Partitioner,
		Tokens:      cfg.Tokens,
	}
	if cluster != identity.Cluster {
		lgr.Warnw("cluster_name_mismatch", "flag", cluster, "actual", identity.Cluster)
	}

	if offline {
		return identity, manifest, nil
	}

	info, err := GetNodeInfo(cfg.ListenAddress)
	if err != nil {
		lgr.Errorw("get_node_info_error", "err", err)
		return manifests.NodeIdentity{}, manifests.Manifest{}, err
	}
	manifest.HostID = info.HostID
	manifest.Tokens = info.Tokens
	sort.Strings(manifest.Tokens)

	if identity.Cluster != info.ClusterName {
		lgr.Warnw("cluster_name_mismatch", "env", cfg.ClusterName, "cassandra", identity.Cluster, "live", info.ClusterName)
	}

	return identity, manifest, nil
}
