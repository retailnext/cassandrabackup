// Copyright 2023 RetailNext, Inc.
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

package digest

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/retailnext/cassandrabackup/cache"
	"github.com/retailnext/cassandrabackup/paranoid"
)

func TestCacheAWS(t *testing.T) {
	dir, err := os.MkdirTemp("", "")
	if err != nil {
		panic(err)
	}
	defer func() {
		if cleanupErr := os.RemoveAll(dir); cleanupErr != nil {
			panic(cleanupErr)
		}
	}()

	cachePath := filepath.Join(dir, "cache.db")
	testFilePath := filepath.Join(dir, "bigfile")

	storage, err := cache.Open(cachePath, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if closeErr := storage.Close(); closeErr != nil {
			panic(closeErr)
		}
	}()
	bigSize := 1024 * 1024 * 70

	c := &Cache{
		c: storage.Cache(cacheName),
		f: &awsForUploadFactory{},
	}

	if err := os.WriteFile(testFilePath, make([]byte, bigSize), 0o644); err != nil {
		panic(err)
	}

	safeFile, err := paranoid.NewFile(testFilePath)
	if err != nil {
		t.Fatal(err)
	}
	if safeFile.Len() != int64(bigSize) {
		t.Fatalf("wrong len %d != %d", safeFile.Len(), bigSize)
	}

	entry1, err := c.Get(context.Background(), safeFile)
	if err != nil {
		t.Fatal(err)
	}

	pd1 := entry1.PartDigests()

	if pd1.TotalLength() != safeFile.Len() {
		t.Fatalf("wrong entry len %d != %d", pd1.TotalLength(), safeFile.Len())
	}

	if pd1.Parts() != 2 {
		t.Fatalf("expected %d parts got %d", pd1.Parts(), safeFile.Len())
	}

	if pd1.PartLength(1)+pd1.PartLength(2) != safeFile.Len() {
		t.Fatal("lengths don't add up")
	}

	if closeErr := storage.Close(); closeErr != nil {
		panic(closeErr)
	}

	storage, err = cache.Open(cachePath, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	c = &Cache{
		c: storage.Cache(cacheName),
		f: &awsForUploadFactory{},
	}

	entry2, err := c.Get(context.Background(), safeFile)
	if err != nil {
		t.Fatal(err)
	}

	pd2 := entry2.PartDigests()

	if entry1.ForRestore() != entry2.ForRestore() {
		t.Fatalf("restore entry mismatch %+v %+v", entry1, entry2)
	}

	if pd1.TotalLength() != pd2.TotalLength() {
		t.Fatalf("restore entry mismatch %+v %+v", entry1, entry2)
	}
}
