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
	"encoding/binary"
	"fmt"

	"github.com/retailnext/cassandrabackup/blake"
	"github.com/retailnext/cassandrabackup/paranoid"
)

type gcsForUpload struct {
	blake2b     blake.Blake2bDigest
	totalLength int64
}

func (u *gcsForUpload) TotalLength() int64 {
	return u.totalLength
}

func (u *gcsForUpload) ForRestore() ForRestore {
	return NewForRestore(u.blake2b)
}

func (u *gcsForUpload) Populate(ctx context.Context, file paranoid.File) error {
	u.totalLength = file.Len()

	blake2b512Hash, err := blake.MakeFileHash(ctx, file, nil)
	if err != nil {
		return err
	}

	u.blake2b.Populate(blake2b512Hash)
	return nil
}

const headerLength = 8

func (u *gcsForUpload) MarshalBinary() ([]byte, error) {
	header := make([]byte, headerLength)
	binary.BigEndian.PutUint64(header[0:], uint64(u.totalLength))

	result := make([]byte, 64+headerLength)
	if copy(result[0:], u.blake2b[:]) != 64 {
		panic("bad copy")
	}
	if copy(result[64:], header) != headerLength {
		panic("bad copy")
	}
	return result, nil
}

func (u *gcsForUpload) UnmarshalBinary(data []byte) error {
	if len(data) < 64 {
		return fmt.Errorf("invalid data")
	}
	if copy(u.blake2b[:], data) != 64 {
		panic("bad copy")
	}
	u.totalLength = int64(binary.BigEndian.Uint64(data[64:]))
	return nil
}
