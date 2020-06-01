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

package manifests

import (
	"context"
	"crypto/rand"
	"io/ioutil"
	"os"
	"testing"

	"github.com/go-test/deep"
	"github.com/mailru/easyjson"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/paranoid"
	"github.com/retailnext/cassandrabackup/unixtime"
)

func TestManifest(t *testing.T) {
	tempFile, err := ioutil.TempFile("", "")
	if err != nil {
		panic(err)
	}
	tempFileName := tempFile.Name()
	defer func() {
		closeErr := tempFile.Close()
		cleanupErr := os.Remove(tempFileName)
		if closeErr != nil {
			panic(closeErr)
		}
		if cleanupErr != nil {
			panic(cleanupErr)
		}
	}()
	buf := make([]byte, 1024)
	if _, err := rand.Read(buf); err != nil {
		panic(err)
	}

	parFile, err := paranoid.NewFile(tempFileName)
	if err != nil {
		t.Fatal(err)
	}

	forUploadAWS := func() digest.ForUpload { return &digest.ForUploadAWS{} }
	dgst, err := digest.GetUncached(context.Background(), parFile, forUploadAWS)
	if err != nil {
		t.Fatal(err)
	}

	m1 := Manifest{
		Time:         unixtime.Now(),
		ManifestType: ManifestTypeIncremental,
		HostID:       "foobar",
		Address:      "127.0.0.2",
		Partitioner:  "bazboo",
		Tokens:       []string{"-1", "1"},
		DataFiles: map[string]digest.ForRestore{
			tempFileName: dgst.ForRestore(),
		},
	}

	jsonBytes, err := easyjson.Marshal(m1)
	if err != nil {
		t.Fatal(err)
	}

	var m2 Manifest
	if err := easyjson.Unmarshal(jsonBytes, &m2); err != nil {
		t.Fatal(err)
	}

	deep.CompareUnexportedFields = true
	if diff := deep.Equal(m1, m2); diff != nil {
		t.Fatal(diff)
	}
}
