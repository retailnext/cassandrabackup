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
	"os"

	"go.uber.org/zap"
)

type cleanupHandler interface {
	MarkUploadSuccess(path string)
	MarkUploadFailure(path string)
	MarkProspectFailure()
	MarkManifestUploadFailure()
	MarkManifestUploadSuccess()

	Execute() error
}

type dummyCleanupHandler struct {
	sawUploadSuccess      bool
	sawUploadFailure      bool
	sawProspectFailure    bool
	manifestUploadFailure bool
	manifestUploadSuccess bool
}

func (ch *dummyCleanupHandler) MarkUploadSuccess(path string) {
	ch.sawUploadSuccess = true
}

func (ch *dummyCleanupHandler) MarkUploadFailure(path string) {
	ch.sawUploadFailure = true
}

func (ch *dummyCleanupHandler) MarkProspectFailure() {
	ch.sawProspectFailure = true
}

func (ch *dummyCleanupHandler) MarkManifestUploadFailure() {
	ch.manifestUploadFailure = true
}

func (ch *dummyCleanupHandler) MarkManifestUploadSuccess() {
	ch.manifestUploadSuccess = true
}

func (ch *dummyCleanupHandler) Execute() error {
	zap.S().Infow("dummy_cleanup_handler", "saw_upload_success", ch.sawUploadSuccess, "saw_upload_failure", ch.sawUploadFailure, "saw_prospect_failure", ch.sawProspectFailure, "manifest_upload_ok", ch.manifestUploadSuccess, "manifest_upload_err", ch.manifestUploadFailure)
	return nil
}

type snapshotCleanupHandler struct {
	name string
}

func (ch *snapshotCleanupHandler) MarkUploadSuccess(path string) {
}

func (ch *snapshotCleanupHandler) MarkUploadFailure(path string) {
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
	uploadedFiles            map[string]struct{}
	sawProspectFailure       bool
	sawUploadFailure         bool
	sawManifestUploadFailure bool
	manifestUploadOK         bool
}

func (ch *incrementalCleanupHandler) MarkUploadSuccess(path string) {
	if ch.uploadedFiles == nil {
		ch.uploadedFiles = make(map[string]struct{})
	}
	ch.uploadedFiles[path] = struct{}{}
}

func (ch *incrementalCleanupHandler) MarkUploadFailure(path string) {
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
		lgr.Warnw("skipping_incremental_cleanup", "reason", "prospect_failure")
		return nil
	}
	if ch.sawUploadFailure {
		lgr.Warnw("skipping_incremental_cleanup", "reason", "upload_failure")
		return nil
	}
	if ch.sawManifestUploadFailure {
		lgr.Warnw("skipping_incremental_cleanup", "reason", "manifest_upload_failure")
		return nil
	}
	if !ch.manifestUploadOK {
		lgr.Warnw("skipping_incremental_cleanup", "reason", "manifest_not_uploaded")
		return nil
	}

	var lastErr error
	for name := range ch.uploadedFiles {
		err := os.Remove(name)
		if err != nil {
			if os.IsNotExist(err) {
				lgr.Warnw("file_disappeared", "name", name)
			} else {
				lgr.Errorw("cleanup_failed_to_remove_file", "name", name, "err", err)
				lastErr = err
			}
		}
	}
	return lastErr
}
