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
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/manifests"
	"github.com/retailnext/cassandrabackup/paranoid"
)

type processor struct {
	ctx context.Context

	bucketClient *bucket.Client
	digestCache  *digest.Cache

	prospectedFiles chan fileRecord
	uploadedFiles   chan fileRecord

	identity       manifests.NodeIdentity
	manifest       manifests.Manifest
	cleanupHandler cleanupHandler
	pathProcessor  pathProcessor
}

type fileRecord struct {
	ManifestPath string
	File         paranoid.File
	Digests      digest.ForUpload

	ProspectError error
	UploadError   error
}
