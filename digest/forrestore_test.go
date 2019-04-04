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
	"crypto/rand"
	"testing"

	"github.com/go-test/deep"
	"github.com/mailru/easyjson"
)

func TestForRestoreEasyJSON(t *testing.T) {
	var fr1, fr2 ForRestore

	if _, err := rand.Read(fr1.blake2b[:]); err != nil {
		panic(err)
	}

	jsonBytes, err := easyjson.Marshal(&fr1)
	if err != nil {
		t.Fatal(err)
	}

	if err := easyjson.Unmarshal(jsonBytes, &fr2); err != nil {
		t.Fatal(err)
	}

	deep.CompareUnexportedFields = true
	if diff := deep.Equal(fr1, fr2); diff != nil {
		t.Fatal(diff)
	}
}
