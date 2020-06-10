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

import (
	"context"
	"fmt"

	"github.com/retailnext/cassandrabackup/bucket"
	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/manifests"
	"github.com/retailnext/cassandrabackup/nodeidentity"
	"github.com/retailnext/cassandrabackup/nodetool"
)

func DoSnapshotBackup(ctx context.Context, config *config.Config) error {
	identity, manifest, err := nodeidentity.GetIdentityAndManifestTemplate(overrideCluster, overrideHostname)
	if err != nil {
		return err
	}

	snapshotName := fmt.Sprintf("auto-%s", manifest.Time.Decimal())
	err = nodetool.TakeSnapshot(snapshotName)
	if err != nil {
		return err
	}

	manifest.ManifestType = manifests.ManifestTypeSnapshot

	pr := &processor{
		ctx: ctx,

		bucketClient: bucket.OpenShared(config),
		digestCache:  digest.OpenShared(config),

		prospectedFiles: make(chan fileRecord),
		uploadedFiles:   make(chan fileRecord),

		identity: identity,
		manifest: manifest,

		cleanupHandler: &snapshotCleanupHandler{
			name: snapshotName,
		},
		pathProcessor: snapshotPathProcessor{
			name: snapshotName,
		},
	}

	go pr.prospect()
	go pr.uploadFiles()
	return pr.finish()
}
