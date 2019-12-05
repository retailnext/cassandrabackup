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
	"os"
	"path/filepath"
	"strings"

	"github.com/retailnext/cassandrabackup/paranoid"
	"go.uber.org/zap"
)

const dataPath = "/var/lib/cassandra/data"

func (p *processor) prospect() {
	defer close(p.prospectedFiles)

	records, walkErr := getFiles(dataPath, p.pathProcessor)
	if walkErr != nil {
		p.prospectedFiles <- fileRecord{
			ProspectError: walkErr,
		}
		return
	}

	doneCh := p.ctx.Done()
	for _, record := range records {
		record.Digests, record.ProspectError = p.digestCache.Get(p.ctx, record.File)

		select {
		case <-doneCh:
			p.prospectedFiles <- fileRecord{
				ProspectError: p.ctx.Err(),
			}
			return
		case p.prospectedFiles <- record:
		}
	}
}

func getFiles(root string, pathProcessor pathProcessor) ([]fileRecord, error) {
	lgr := zap.S()

	var records []fileRecord

	walkErr := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if isIgnorableWalkError(dataPath, path, err) {
				// This is something we can ignore, like a non-snapshot non-backup file disappearing mid-walk.
				lgr.Debugw("ignoring_walk_error", "path", path, "err", err)
				return nil
			}

			lgr.Errorw("walk_error", "path", path, "err", err)
			return err
		}

		if info.IsDir() {
			return nil
		}

		record := fileRecord{
			File: paranoid.NewFileFromInfo(path, info),
		}

		relPath, err := filepath.Rel(dataPath, path)
		if err != nil {
			panic(err)
		}
		record.ManifestPath = pathProcessor.ManifestPath(relPath)
		if record.ManifestPath != "" {
			// The processor has indicated that this file should not be backed up.
			records = append(records, record)
		}

		return nil
	})

	return records, walkErr
}

func isIgnorableWalkError(basePath, path string, err error) bool {
	relPath, relErr := filepath.Rel(basePath, path)
	if relErr != nil {
		panic(relErr)
	}
	parts := strings.Split(relPath, string(filepath.Separator))
	if len(parts) == 3 {
		switch filepath.Ext(parts[2]) {
		case "txt", "db", "crc32", "sha1":
			// This may be a live sstable (not in backups or snapshots) that went away mid-scan
			return os.IsNotExist(err)
		}
	}
	if len(parts) == 4 {
		switch parts[2] {
		case "backups", "snapshots":
			// This is in backups or snapshots, we cannot ignore this.
			return false
		}
		if strings.HasSuffix(parts[2], "_index") {
			// This is "live" like above, but is an index in 3.x
			switch filepath.Ext(parts[3]) {
			case "txt", "db", "crc32", "sha1":
				// This may be a live sstable of an index that went away mid-scan
				return os.IsNotExist(err)
			}
		}
	}
	return false
}
