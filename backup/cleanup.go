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
	"cassandrabackup/nodetool"
	"cassandrabackup/paranoid"

	"go.uber.org/zap"
)

type cleanupHandler interface {
	MarkUploadSuccess(ref paranoid.File)
	MarkUploadFailure(ref paranoid.File)
	MarkProspectFailure()
	MarkManifestUploadFailure()
	MarkManifestUploadSuccess()

	Execute() error
}

type snapshotCleanupHandler struct {
	name string
}

func (ch *snapshotCleanupHandler) MarkUploadSuccess(ref paranoid.File) {
}

func (ch *snapshotCleanupHandler) MarkUploadFailure(ref paranoid.File) {
}

func (ch *snapshotCleanupHandler) MarkProspectFailure() {
}

func (ch *snapshotCleanupHandler) MarkManifestUploadFailure() {
}

func (ch *snapshotCleanupHandler) MarkManifestUploadSuccess() {
}

func (ch *snapshotCleanupHandler) Execute() error {
	return nodetool.ClearSnapshot(ch.name)
}

type incrementalCleanupHandler struct {
	uploadedFiles            []paranoid.File
	sawProspectFailure       bool
	sawUploadFailure         bool
	sawManifestUploadFailure bool
	manifestUploadOK         bool
}

func (ch *incrementalCleanupHandler) MarkUploadSuccess(ref paranoid.File) {
	ch.uploadedFiles = append(ch.uploadedFiles, ref)
}

func (ch *incrementalCleanupHandler) MarkUploadFailure(ref paranoid.File) {
	ch.sawUploadFailure = true
}

func (ch *incrementalCleanupHandler) MarkProspectFailure() {
	ch.sawProspectFailure = true
}

func (ch *incrementalCleanupHandler) MarkManifestUploadFailure() {
	ch.sawManifestUploadFailure = true
}

func (ch *incrementalCleanupHandler) MarkManifestUploadSuccess() {
	ch.manifestUploadOK = true
}

func (ch *incrementalCleanupHandler) Execute() error {
	lgr := zap.S()
	if ch.sawProspectFailure {
		lgr.Infow("skipping_incremental_cleanup", "reason", "prospect_failure")
		return nil
	}
	if ch.sawUploadFailure {
		lgr.Infow("skipping_incremental_cleanup", "reason", "upload_failure")
		return nil
	}
	if ch.sawManifestUploadFailure {
		lgr.Infow("skipping_incremental_cleanup", "reason", "manifest_upload_failure")
		return nil
	}
	if !ch.manifestUploadOK {
		lgr.Infow("skipping_incremental_cleanup", "reason", "manifest_not_uploaded")
		return nil
	}
	if *noCleanIncremental {
		lgr.Infow("skipping_incremental_cleanup", "reason", "not_enabled", "would_remove", len(ch.uploadedFiles))
		if *verboseClean {
			for _, ref := range ch.uploadedFiles {
				lgr.Infow("cleanup_would_have_removed_file", "name", ref.Name())
			}
		}
		return nil
	}

	var lastErr error
	for _, ref := range ch.uploadedFiles {
		if err := ref.Delete(); err != nil {
			lgr.Errorw("cleanup_failed_to_remove_file", "name", ref.Name(), "err", err)
			lastErr = err
		} else {
			if *verboseClean {
				lgr.Infow("cleanup_removed_file", "name", ref.Name())
			}
		}
	}
	return lastErr
}
