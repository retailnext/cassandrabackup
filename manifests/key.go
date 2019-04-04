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

package manifests

import (
	"cassandrabackup/unixtime"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

var InvalidManifestKey = errors.New("invalid manifest key")

type ManifestKey struct {
	Time         unixtime.Seconds
	ManifestType ManifestType
}

func (k ManifestKey) FileName() string {
	return fmt.Sprintf("%020d.%d.json", k.Time, k.ManifestType)
}

func (k *ManifestKey) PopulateFromFileName(name string) error {
	parts := strings.Split(name, ".")
	if len(parts) != 3 {
		return InvalidManifestKey
	}
	if len(parts[0]) != 20 {
		return InvalidManifestKey
	}
	if len(parts[1]) != 1 {
		return InvalidManifestKey
	}
	if parts[2] != "json" {
		return InvalidManifestKey
	}
	var seconds unixtime.Seconds
	if err := seconds.ParseDecimal(parts[0]); err != nil {
		return InvalidManifestKey
	}
	typeCode, err := strconv.Atoi(parts[1])
	if err != nil {
		return InvalidManifestKey
	}
	k.Time = seconds
	k.ManifestType = ManifestType(typeCode)
	return nil
}

type ManifestKeys []ManifestKey

func (s ManifestKeys) Len() int {
	return len(s)
}

func (s ManifestKeys) Less(i, j int) bool {
	if s[i].Time < s[j].Time {
		return true
	}
	if s[i].Time > s[j].Time {
		return false
	}
	return s[i].ManifestType < s[j].ManifestType
}

func (s ManifestKeys) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
