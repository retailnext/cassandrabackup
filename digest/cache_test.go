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

package digest

import (
	"cassandrabackup/cache"
	"cassandrabackup/paranoid"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
)

func TestCache(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
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

	storage, err := cache.Open(cachePath, 0644)
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
	}

	if err := ioutil.WriteFile(testFilePath, make([]byte, bigSize), 0644); err != nil {
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

	if entry1.ContentLength() != safeFile.Len() {
		t.Fatalf("wrong entry len %d != %d", entry1.ContentLength(), safeFile.Len())
	}

	if entry1.Parts() != 2 {
		t.Fatalf("expected %d parts got %d", entry1.Parts(), safeFile.Len())
	}

	if entry1.PartLength(1)+entry1.PartLength(2) != safeFile.Len() {
		t.Fatal("lengths don't add up")
	}

	if closeErr := storage.Close(); closeErr != nil {
		panic(closeErr)
	}

	storage, err = cache.Open(cachePath, 0644)
	if err != nil {
		t.Fatal(err)
	}
	c = &Cache{
		c: storage.Cache(cacheName),
	}

	entry2, err := c.Get(context.Background(), safeFile)
	if err != nil {
		t.Fatal(err)
	}

	if entry1.ForRestore() != entry2.ForRestore() {
		t.Fatalf("restore entry mismatch %+v %+v", entry1, entry2)
	}

	if entry1.ContentLength() != entry2.ContentLength() {
		t.Fatalf("restore entry mismatch %+v %+v", entry1, entry2)
	}
}
