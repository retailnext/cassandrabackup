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

//go:generate go run github.com/mailru/easyjson/easyjson -disallow_unknown_fields $GOFILE

package manifests

import (
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/unixtime"
)

type ManifestType int

const (
	ManifestTypeInvalid     ManifestType = 0
	ManifestTypeSnapshot    ManifestType = 1
	ManifestTypeIncomplete  ManifestType = 2
	ManifestTypeIncremental ManifestType = 3
)

//easyjson:json
type Manifest struct {
	Time         unixtime.Seconds             `json:"time"`
	ManifestType ManifestType                 `json:"manifest_type"`
	HostID       string                       `json:"host_id"`
	Address      string                       `json:"address"`
	Partitioner  string                       `json:"partitioner"`
	Tokens       []string                     `json:"tokens"`
	DataFiles    map[string]digest.ForRestore `json:"data_files"`
}

func (m Manifest) Key() ManifestKey {
	return ManifestKey{
		Time:         m.Time,
		ManifestType: m.ManifestType,
	}
}
