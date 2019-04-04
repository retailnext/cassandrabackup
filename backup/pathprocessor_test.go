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

package backup

import "testing"

func TestIncrementalPathProcessor(t *testing.T) {
	cases := map[string]string{
		// Live tables and indexes
		"luneta/site-bcfbb16bdd5b36ac9db83d20236eb7ee/.site_subscription_uuid_index/md-462-big-Summary.db": "",
		"system_schema/indexes-0feb57ac311f382fba6d9024d305702f/md-1-big-Data.db":                          "",

		// Incrementals
		"luneta/site-bcfbb16bdd5b36ac9db83d20236eb7ee/backups/.site_subscription_uuid_index/md-462-big-Filter.db": "luneta/site-bcfbb16bdd5b36ac9db83d20236eb7ee/.site_subscription_uuid_index/md-462-big-Filter.db",
		"system_schema/indexes-0feb57ac311f382fba6d9024d305702f/backups/md-2-big-Data.db":                         "system_schema/indexes-0feb57ac311f382fba6d9024d305702f/md-2-big-Data.db",

		// Snapshot: my-test
		"luneta/site-bcfbb16bdd5b36ac9db83d20236eb7ee/snapshots/my-test/.site_subscription_uuid_index/md-462-big-Data.db": "",
		"system_schema/indexes-0feb57ac311f382fba6d9024d305702f/snapshots/my-test/md-3-big-Data.db":                       "",

		// Snapshot: other-snapshot
		"luneta/site-bcfbb16bdd5b36ac9db83d20236eb7ee/snapshots/other-snapshot/.site_subscription_uuid_index/md-462-big-Data.db": "",
		"system_schema/indexes-0feb57ac311f382fba6d9024d305702f/snapshots/other-snapshot/md-3-big-Data.db":                       "",
	}

	pr := &incrementalPathProcessor{}
	for input, expected := range cases {
		if pr.ManifestPath(input) != expected {
			t.Fatalf("input=%q expected=%q actual=%q", input, expected, pr.ManifestPath(input))
		}
	}
}

func TestSnapshotPathProcessor(t *testing.T) {
	cases := map[string]string{
		// Live tables and indexes
		"luneta/site-bcfbb16bdd5b36ac9db83d20236eb7ee/.site_subscription_uuid_index/md-462-big-Summary.db": "",
		"system_schema/indexes-0feb57ac311f382fba6d9024d305702f/md-1-big-Data.db":                          "",

		// Incrementals
		"luneta/site-bcfbb16bdd5b36ac9db83d20236eb7ee/backups/.site_subscription_uuid_index/md-462-big-Filter.db": "",
		"system_schema/indexes-0feb57ac311f382fba6d9024d305702f/backups/md-2-big-Data.db":                         "",

		// Snapshot: my-test
		"luneta/site-bcfbb16bdd5b36ac9db83d20236eb7ee/snapshots/my-test/.site_subscription_uuid_index/md-462-big-Data.db": "luneta/site-bcfbb16bdd5b36ac9db83d20236eb7ee/.site_subscription_uuid_index/md-462-big-Data.db",
		"system_schema/indexes-0feb57ac311f382fba6d9024d305702f/snapshots/my-test/md-3-big-Data.db":                       "system_schema/indexes-0feb57ac311f382fba6d9024d305702f/md-3-big-Data.db",

		// Snapshot: other-snapshot
		"luneta/site-bcfbb16bdd5b36ac9db83d20236eb7ee/snapshots/other-snapshot/.site_subscription_uuid_index/md-462-big-Data.db": "",
		"system_schema/indexes-0feb57ac311f382fba6d9024d305702f/snapshots/other-snapshot/md-3-big-Data.db":                       "",
	}

	pr := &snapshotPathProcessor{
		name: "my-test",
	}
	for input, expected := range cases {
		if pr.ManifestPath(input) != expected {
			t.Fatalf("input=%q expected=%q actual=%q", input, expected, pr.ManifestPath(input))
		}
	}
}
