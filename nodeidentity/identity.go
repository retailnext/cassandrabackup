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

package nodeidentity

import (
	"cassandrabackup/cassandraconfig"
	"cassandrabackup/manifests"
	"cassandrabackup/systemlocal"
	"cassandrabackup/unixtime"
	"os"

	"go.uber.org/zap"
)

var ()

// GetIdentityAndManifestTemplateOffline reads the cassandra config file, system hostname, and overrides.
// GetIdentityAndManifestTemplate should be used when cassandra is expected to be running.
func GetIdentityAndManifestTemplateOffline(overrideCluster, overrideHostname *string) (manifests.NodeIdentity, manifests.Manifest, error) {
	lgr := zap.S()

	var identity manifests.NodeIdentity

	if overrideCluster != nil {
		identity.Cluster = *overrideCluster
	}
	if overrideHostname != nil {
		identity.Hostname = *overrideHostname
	}

	cfg, err := cassandraconfig.Load()
	if err != nil {
		lgr.Errorw("load_cassandra_config_error", "err", err)
		return manifests.NodeIdentity{}, manifests.Manifest{}, err
	}

	if identity.Cluster == "" {
		identity.Cluster = cfg.ClusterName
	} else {
		if identity.Cluster != cfg.ClusterName {
			lgr.Warnw("backup_cluster_overridden", "actual", cfg.ClusterName, "override", identity.Cluster)
		}
	}

	hostname := getHostname()
	if identity.Hostname == "" {
		identity.Hostname = hostname
	} else {
		if identity.Hostname != hostname {
			lgr.Warnw("backup_hostname_overridden", "actual", hostname, "override", identity.Hostname)
		}
	}

	template := manifests.Manifest{
		Time:        unixtime.Now(),
		Address:     cfg.IPForClients(),
		Partitioner: cfg.Partitioner,
		Tokens:      cfg.Tokens(),
	}
	return identity, template, nil
}

// GetIdentityAndManifestTemplate returns a template manifest suitable for uploading, incorporating runtime token info.
// Panics on critical differences between the cassandra.yaml config and the running daemon.
func GetIdentityAndManifestTemplate(overrideCluster, overrideHostname *string) (manifests.NodeIdentity, manifests.Manifest, error) {
	lgr := zap.S()
	identity, template, err := GetIdentityAndManifestTemplateOffline(overrideCluster, overrideHostname)
	if err != nil {
		return identity, template, err
	}

	info, err := systemlocal.GetNodeInfo(template.Address)
	if err != nil {
		lgr.Errorw("get_node_info_error", "addr", template.Address, "err", err)
		return identity, template, err
	}

	if template.Partitioner != info.Partitioner {
		lgr.Panicw("partitioner", "config", template.Partitioner, "actual", info.Partitioner)
	}

	template.HostID = info.HostID
	if len(template.Tokens) == 0 {
		template.Tokens = info.Tokens
	} else {
		if len(template.Tokens) != len(info.Tokens) {
			lgr.Panicw("tokens_mismatch", "config", template.Tokens, "actual", info.Tokens)
		}
		for i := range template.Tokens {
			if template.Tokens[i] != info.Tokens[i] {
				lgr.Panicw("tokens_mismatch", "config", template.Tokens, "actual", info.Tokens)
			}
		}
	}

	return identity, template, nil
}

var getHostname = func() string {
	name, err := os.Hostname()
	if err != nil {
		panic(err)
	}
	for i, r := range name {
		if r == '.' {
			return name[:i]
		}
	}
	return name
}
