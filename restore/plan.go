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

package restore

import (
	"cassandrabackup/bucket"
	"cassandrabackup/digest"
	"cassandrabackup/manifests"
	"cassandrabackup/unixtime"
	"context"
	"errors"

	"go.uber.org/zap"
)

var client *bucket.Client

var NoSnapshotsFound = errors.New("no snapshots found")
var ChangesDetected = errors.New("file changes detected")

func Main(ctx context.Context, cluster, hostname string, dryRun, allowChangedFiles bool, notBefore, notAfter int64) error {
	lgr := zap.S()

	identity := manifests.NodeIdentity{
		Cluster:  cluster,
		Hostname: hostname,
	}

	keys, err := selectManifests(ctx, identity, unixtime.Seconds(notBefore), unixtime.Seconds(notAfter))
	if err != nil {
		return err
	}
	lgr.Infow("selected_manifests", "keys", keys)

	p, err := assemble(ctx, identity, keys)
	if err != nil {
		return err
	}

	if len(p.changedHistories) > 0 {
		for name, history := range p.changedHistories {
			for _, entry := range history {
				lgr.Infow("file_changed", "name", name, "digest", entry.file, "manifest", entry.manifest)
			}
		}
		if !allowChangedFiles {
			return ChangesDetected
		}
	}

	if dryRun {
		for name, file := range p.files {
			lgr.Infow("would_restore", "name", name, "digest", file)
		}
		return nil
	}

	w := newWorker(identity)
	return w.restoreFiles(ctx, p.files)
}

func selectManifests(ctx context.Context, identity manifests.NodeIdentity, startAfter, notAfter unixtime.Seconds) (manifests.ManifestKeys, error) {
	if client == nil {
		client = bucket.NewClient()
	}

	keys, err := client.ListManifests(ctx, identity, startAfter, notAfter)
	if err != nil {
		return nil, err
	}

	var snapshotIndex = -1
	for i := len(keys) - 1; i >= 0; i-- {
		if keys[i].ManifestType == manifests.ManifestTypeSnapshot {
			snapshotIndex = i
			break
		}
	}
	if snapshotIndex < 0 {
		return nil, NoSnapshotsFound
	}
	return keys[snapshotIndex:], nil
}

func assemble(ctx context.Context, identity manifests.NodeIdentity, keys manifests.ManifestKeys) (*plan, error) {
	selectedManifests, err := client.GetManifests(ctx, identity, keys)
	if err != nil {
		return nil, err
	}

	var plan plan
	for _, manifest := range selectedManifests {
		plan.addManifest(manifest)
	}
	plan.finish()

	return &plan, nil
}

type historyEntry struct {
	file     digest.ForRestore
	manifest manifests.ManifestKey
}

type fileHistory []historyEntry

type plan struct {
	fileHistories    map[string]fileHistory
	changedHistories map[string]fileHistory
	files            map[string]digest.ForRestore
}

func (p *plan) addManifest(manifest manifests.Manifest) {
	if p.fileHistories == nil {
		p.fileHistories = make(map[string]fileHistory)
	}
	key := manifest.Key()
	for name, file := range manifest.DataFiles {
		history := p.fileHistories[name]
		entry := historyEntry{
			file:     file,
			manifest: key,
		}
		p.fileHistories[name] = append(history, entry)
	}
}

func (p *plan) finish() {
	p.files = make(map[string]digest.ForRestore)
	p.changedHistories = make(map[string]fileHistory)

	for name, history := range p.fileHistories {
		for i := range history {
			p.files[name] = history[i].file
			if i > 0 {
				if history[i].file != history[i-1].file {
					chHistory := p.changedHistories[name]
					if len(chHistory) == 0 {
						chHistory = append(chHistory, history[i-1])
					}
					chHistory = append(chHistory, history[i])
					p.changedHistories[name] = chHistory
				}
			}
		}
	}
}
