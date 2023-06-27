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

package cassandraconfig

import (
	"sort"
	"strings"
	"sync"

	"github.com/retailnext/cassandrabackup/paranoid"
	"gopkg.in/yaml.v3"
)

var (
	ConfigFileName = "/etc/cassandra/cassandra.yaml"

	cacheLock sync.Mutex
	cachedRef paranoid.File
	cachedRaw Raw
)

func Load() (Raw, error) {
	cacheLock.Lock()
	defer cacheLock.Unlock()

	if cachedRef.Name() == ConfigFileName && cachedRef.Check() == nil {
		return cachedRaw, nil
	}

	var newRaw Raw

	newRef, err := paranoid.NewFile(ConfigFileName)
	if err != nil {
		return newRaw, err
	}
	f, err := newRef.Open()
	defer func() {
		if f != nil {
			if closeErr := f.Close(); closeErr != nil {
				panic(closeErr)
			}
		}
	}()
	if err != nil {
		return newRaw, err
	}

	decoder := yaml.NewDecoder(f)
	err = decoder.Decode(&newRaw)

	if err == nil {
		cachedRef = newRef
		cachedRaw = newRaw
	}
	return newRaw, err
}

type Raw struct {
	BroadcastAddress    string `yaml:"broadcast_address"`
	BroadcastRPCAddress string `yaml:"broadcast_rpc_address"`
	ClusterName         string `yaml:"cluster_name"`
	InitialToken        string `yaml:"initial_token"`
	ListenAddress       string `yaml:"listen_address"`
	ListenInterface     string `yaml:"listen_interface"`
	Partitioner         string `yaml:"partitioner"`
	RPCAddress          string `yaml:"rpc_address"`
	RPCInterface        string `yaml:"rpc_interface"`
}

func (r Raw) Tokens() []string {
	tokens := strings.Split(r.InitialToken, ",")
	result := tokens[:0]
	for _, token := range tokens {
		s := strings.TrimSpace(token)
		if s != "" {
			result = append(result, s)
		}
	}
	sort.Strings(result)
	return result
}
