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

package plan

import (
	"context"

	"github.com/retailnext/cassandrabackup/bucket"
	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/manifests"
	"github.com/retailnext/cassandrabackup/unixtime"
	"go.uber.org/zap"
)

type HistoryEntry struct {
	Manifest manifests.ManifestKey
	Digest   digest.ForRestore
}

type NodePlan struct {
	Files             map[string]digest.ForRestore
	ChangedFiles      map[string][]HistoryEntry
	SelectedManifests manifests.ManifestKeys
}

func Create(ctx context.Context, cfg config.Config, identity manifests.NodeIdentity, startAfter, notAfter unixtime.Seconds) (NodePlan, error) {
	lgr := zap.S().With("identity", identity)

	nodeManifests, err := getManifests(ctx, cfg, identity, startAfter, notAfter)
	if err != nil {
		lgr.Errorw("get_manifests_error", "err", err)
		return NodePlan{}, err
	}

	return assemble(nodeManifests), nil
}

func getManifests(ctx context.Context, cfg config.Config, identity manifests.NodeIdentity, startAfter, notAfter unixtime.Seconds) ([]manifests.Manifest, error) {
	client := bucket.OpenShared(cfg)

	keys, err := client.ListManifests(ctx, identity, startAfter, notAfter)
	if err != nil {
		return nil, err
	}

	snapshotIndex := -1
	for i := len(keys) - 1; i >= 0; i-- {
		if keys[i].ManifestType == manifests.ManifestTypeSnapshot {
			snapshotIndex = i
			break
		}
	}
	if snapshotIndex >= 0 {
		keys = keys[snapshotIndex:]
	}

	if len(keys) == 0 {
		return nil, nil
	}
	return bucket.GetManifests(ctx, client, identity, keys)
}

func assemble(nodeManifests []manifests.Manifest) NodePlan {
	nodePlan := NodePlan{
		SelectedManifests: make(manifests.ManifestKeys, 0, len(nodeManifests)),
	}

	fileHistories := make(map[string][]HistoryEntry)
	for _, manifest := range nodeManifests {
		nodePlan.SelectedManifests = append(nodePlan.SelectedManifests, manifest.Key())

		for name, file := range manifest.DataFiles {
			history := fileHistories[name]
			entry := HistoryEntry{
				Manifest: manifest.Key(),
				Digest:   file,
			}
			fileHistories[name] = append(history, entry)
		}
	}

	if len(fileHistories) > 0 {
		nodePlan.Files = make(map[string]digest.ForRestore, len(fileHistories))
		for name, history := range fileHistories {
			for i := range history {
				nodePlan.Files[name] = history[i].Digest
				if i > 0 {
					if history[i].Digest != history[i-1].Digest {
						if nodePlan.ChangedFiles == nil {
							nodePlan.ChangedFiles = make(map[string][]HistoryEntry)
						}
						chHistory := nodePlan.ChangedFiles[name]
						if len(chHistory) == 0 {
							chHistory = append(chHistory, history[i-1])
						}
						chHistory = append(chHistory, history[i])
						nodePlan.ChangedFiles[name] = chHistory
					}
				}
			}
		}
	}

	return nodePlan
}
