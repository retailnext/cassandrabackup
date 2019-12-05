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
	"context"
	"regexp"
	"strings"

	"github.com/retailnext/cassandrabackup/bucket"
	"github.com/retailnext/cassandrabackup/cassandraconfig"
	"github.com/retailnext/cassandrabackup/manifests"
	"go.uber.org/zap"
)

func ForRestore(ctx context.Context, overrideCluster, overrideHostname, overridePattern *string) manifests.NodeIdentity {
	var result manifests.NodeIdentity
	if overrideCluster != nil {
		result.Cluster = *overrideCluster
	}
	if overrideHostname != nil {
		result.Hostname = *overrideHostname
	}
	if result.Hostname != "" && result.Cluster != "" {
		return result
	}

	if result.Cluster == "" {
		result.Cluster = getCluster()
	}
	if result.Hostname != "" && result.Cluster != "" {
		return result
	}

	lgr := zap.S()
	pattern := getHostnamePattern(overridePattern)
	client := bucket.OpenShared()

	nodes, err := client.ListHostNames(ctx, result.Cluster)
	if err != nil {
		lgr.Panicw("list_hosts_error", "cluster", result.Cluster, "err", err)
	}
	filtered := nodes[:0]
	for _, ni := range nodes {
		if pattern.MatchString(ni.Hostname) {
			filtered = append(filtered, ni)
		}
	}
	if len(filtered) != 1 {
		lgr.Panicw("failed_to_find_host", "pattern", pattern, "found", filtered)
	}
	lgr.Infow("selected_host", "pattern", pattern, "found", filtered[0])

	return filtered[0]
}

func getCluster() string {
	raw, err := cassandraconfig.Load()
	if err != nil {
		zap.S().Panicw("config_load_error", "err", err)
	}
	return raw.ClusterName
}

func getHostnamePattern(overridePattern *string) *regexp.Regexp {
	var pattern string
	if overridePattern != nil {
		pattern = *overridePattern
	}
	numTail := regexp.MustCompile(`\d+$`)
	hostname := getHostname()
	myNumTail := numTail.FindString(hostname)
	if myNumTail == "" {
		return regexp.MustCompile("^" + regexp.QuoteMeta(hostname) + "$")
	}
	if pattern == "" {
		pattern = hostname[0 : len(hostname)-len(myNumTail)]
	}
	myNumTail = strings.TrimLeft(myNumTail, "0")
	if myNumTail == "" {
		myNumTail = "0"
	}
	return regexp.MustCompile("^" + regexp.QuoteMeta(pattern) + "0*" + myNumTail + "$")
}
