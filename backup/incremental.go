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

	"github.com/retailnext/cassandrabackup/bucket"
	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/manifests"
	"github.com/retailnext/cassandrabackup/nodeidentity"
)

func DoIncremental(ctx context.Context, cfg config.Config) error {
	identity, manifest, err := nodeidentity.GetIdentityAndManifestTemplate(overrideCluster, overrideHostname)
	if err != nil {
		return err
	}

	manifest.ManifestType = manifests.ManifestTypeIncremental

	pr := &processor{
		ctx: ctx,

		bucketClient: bucket.OpenShared(cfg),
		digestCache:  digest.OpenShared(cfg),

		prospectedFiles: make(chan fileRecord, 1),
		uploadedFiles:   make(chan fileRecord, 1),

		identity:       identity,
		manifest:       manifest,
		cleanupHandler: &incrementalCleanupHandler{},
		pathProcessor:  incrementalPathProcessor{},
	}

	go pr.prospect()
	go pr.uploadFiles()
	return pr.finish()
}
