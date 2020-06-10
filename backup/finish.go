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

	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/manifests"
	"go.uber.org/zap"
)

func (p *processor) finish() error {
	lgr := zap.S()
	defer func() {
		if err := p.cleanupHandler.Execute(); err != nil {
			lgr.Fatalw("cleanup_failed", "err", err)
		}
	}()

	p.manifest.DataFiles = make(map[string]digest.ForRestore)
	var hadFailures bool
	var prospectError, uploadError error
	for {
		record, ok := <-p.uploadedFiles
		if !ok {
			break
		}
		if record.ProspectError != nil {
			p.cleanupHandler.MarkProspectFailure()
			lgr.Errorw("prospect_error", "path", record.File.Name(), "err", record.ProspectError)
			hadFailures = true
			if prospectError == nil {
				prospectError = record.ProspectError
			}
			continue
		}
		if record.UploadError != nil && record.UploadError != config.UploadSkipped {
			p.cleanupHandler.MarkUploadFailure(record.File)
			lgr.Errorw("upload_error", "path", record.File.Name(), "err", record.UploadError)
			hadFailures = true
			if uploadError == nil {
				uploadError = record.UploadError
			}
			continue
		}

		if record.ManifestPath == "" {
			// ManifestPath should only be empty when a general error is being propagated through the channels.
			panic("empty manifest path")
		}
		if _, exists := p.manifest.DataFiles[record.ManifestPath]; exists {
			lgr.Panicw("duplicate_manifest_path", "record", record)
		}
		p.manifest.DataFiles[record.ManifestPath] = record.Digests.ForRestore()
		p.cleanupHandler.MarkUploadSuccess(record.File)
	}

	if hadFailures {
		// Still write a manifest for the stuff we did manage to upload.
		p.manifest.ManifestType = manifests.ManifestTypeIncomplete
	}

	if len(p.manifest.DataFiles) > 0 {

		if p.manifest.ManifestType == manifests.ManifestTypeInvalid {
			panic("invalid manifest type")
		}
		absoluteKey := p.bucketClient.KeyStore().AbsoluteKeyForManifest(p.identity, p.manifest.Key())
		if err := p.bucketClient.PutManifest(context.Background(), absoluteKey, p.manifest); err != nil {
			lgr.Errorw("manifest_put_error", "err", err)
			p.cleanupHandler.MarkManifestUploadFailure()
			return err
		} else {
			lgr.Infow("put_manifest", "type", p.manifest.ManifestType, "files", len(p.manifest.DataFiles))
			p.cleanupHandler.MarkManifestUploadSuccess()
		}
	} else {
		lgr.Infow("not_uploading_manifest", "reason", "no_files")
	}

	if ctxErr := p.ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if uploadError != nil {
		return uploadError
	}
	if prospectError != nil {
		return prospectError
	}
	return nil
}
