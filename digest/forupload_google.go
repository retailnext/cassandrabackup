// Copyright 2020 RetailNext, Inc.
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
	"encoding/base64"
	"fmt"

	"github.com/retailnext/cassandrabackup/blake"
	"github.com/retailnext/cassandrabackup/digest/parts"
	"github.com/retailnext/cassandrabackup/paranoid"
)

type googleForUpload struct {
	blake2b blake.Blake2bDigest
}

func (u *googleForUpload) URLSafe() string {
	return base64.RawURLEncoding.EncodeToString(u.blake2b[:])
}

func (u *googleForUpload) PartDigests() *parts.PartDigests {
	return nil
}

func (u *googleForUpload) onWrite(buf []byte, len int) {
}

func (u *googleForUpload) ForRestore() ForRestore {
	return NewForRestore(u.blake2b)
}

func (u *googleForUpload) Populate(ctx context.Context, file paranoid.File) error {
	blake2b512Hash, err := blake.MakeHash(ctx, file, u.onWrite)
	if err != nil {
		return err
	}

	u.blake2b.Populate(blake2b512Hash)
	return nil
}

func (u *googleForUpload) MarshalBinary() ([]byte, error) {
	result := make([]byte, 64)
	if copy(result[0:], u.blake2b[:]) != 64 {
		panic("bad copy")
	}
	return result, nil
}

func (u *googleForUpload) UnmarshalBinary(data []byte) error {
	if len(data) < 64 {
		return fmt.Errorf("invalid data")
	}
	if copy(u.blake2b[:], data) != 64 {
		panic("bad copy")
	}
	return nil
}
