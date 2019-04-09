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
	"sort"

	"github.com/gocql/gocql"
)

type NodeInfo struct {
	BootstrapState string
	ClusterName    string
	DataCenter     string
	HostID         string
	Partitioner    string
	Rack           string
	Tokens         []string
}

func GetNodeInfo(addr string) (NodeInfo, error) {
	var result NodeInfo

	cluster := gocql.NewCluster(addr)
	cluster.NumConns = 1
	cluster.DisableInitialHostLookup = true
	cluster.Consistency = gocql.LocalOne

	session, err := cluster.CreateSession()
	if err != nil {
		return result, err
	}
	defer session.Close()

	q := session.Query(`SELECT bootstrapped, cluster_name, data_center, host_id, partitioner, rack, tokens FROM system.local`)
	err = q.Scan(&result.BootstrapState, &result.ClusterName, &result.DataCenter, &result.HostID, &result.Partitioner, &result.Rack, &result.Tokens)
	sort.Strings(result.Tokens)

	return result, err
}
