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
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v2"
)

type CassandraConfig struct {
	ClusterName   string
	Tokens        []string
	ListenAddress string
	Partitioner   string
}

var cassandraConfigFile = "/etc/cassandra/cassandra.yaml"

func LoadCassandraConfig() (CassandraConfig, error) {
	file, err := os.Open(cassandraConfigFile)
	if err != nil {
		return CassandraConfig{}, err
	}
	defer func() {
		if err := file.Close(); err != nil {
			panic(err)
		}
	}()

	decoder := yaml.NewDecoder(file)
	cfg := rawCassandraYaml{}
	if err = decoder.Decode(&cfg); err != nil {
		return CassandraConfig{}, err
	}

	result := CassandraConfig{
		ClusterName:   cfg.ClusterName,
		ListenAddress: cfg.ListenAddress,
		Partitioner:   cfg.Partitioner,
	}

	tokens := strings.Split(cfg.InitialToken, ",")
	for _, t := range tokens {
		t = strings.TrimSpace(t)
		if t != "" {
			result.Tokens = append(result.Tokens, t)
		}
	}
	sort.Strings(result.Tokens)

	return result, nil
}

type rawCassandraYaml struct {
	ClusterName   string `yaml:"cluster_name"`
	InitialToken  string `yaml:"initial_token"`
	ListenAddress string `yaml:"listen_address"`
	Partitioner   string `yaml:"partitioner"`
}
