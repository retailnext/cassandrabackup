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
	"cassandrabackup/bucket"
	"cassandrabackup/digest"
	"cassandrabackup/manifests"
	"cassandrabackup/nodetool"
	"cassandrabackup/systemlocal"
	"cassandrabackup/unixtime"
	"context"
	"fmt"
)

func DoSnapshotBackup(ctx context.Context, cluster string) error {
	identity, manifest, err := systemlocal.GetIdentityAndSkeletonManifest(cluster, false)
	if err != nil {
		return err
	}

	now := unixtime.Now()
	snapshotName := fmt.Sprintf("auto-%s", now.Decimal())
	err = nodetool.TakeSnapshot(snapshotName)
	if err != nil {
		return err
	}

	manifest.Time = now
	manifest.ManifestType = manifests.ManifestTypeSnapshot

	pr := &processor{
		ctx: ctx,

		bucketClient: bucket.NewClient(),
		digestCache:  digest.OpenShared(),

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
