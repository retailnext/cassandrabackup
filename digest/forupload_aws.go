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
	"io"

	"github.com/retailnext/cassandrabackup/blake"
	"github.com/retailnext/cassandrabackup/digest/parts"
	"github.com/retailnext/cassandrabackup/paranoid"
)

type awsForUpload struct {
	blake2b          blake.Blake2bDigest
	partDigestsMaker parts.PartDigestsMaker
	partDigests      parts.PartDigests
}

func (u *awsForUpload) URLSafe() string {
	return base64.RawURLEncoding.EncodeToString(u.blake2b[:])
}

func (u *awsForUpload) PartDigests() *parts.PartDigests {
	return &u.partDigests
}

func (u *awsForUpload) onWrite(buf []byte, len int) {
	if n, err := u.partDigestsMaker.Write(buf[0:len]); err != nil {
		panic(err)
	} else if n != len {
		panic(io.ErrShortWrite)
	}
}

func (u *awsForUpload) ForRestore() ForRestore {
	return NewForRestore(u.blake2b)
}

const partSize = 1024 * 1024 * 64

func (u *awsForUpload) Populate(ctx context.Context, file paranoid.File) error {
	u.partDigestsMaker.Reset(partSize)

	blake2b512Hash, err := blake.MakeHash(ctx, file, u.onWrite)
	if err != nil {
		return err
	}

	u.blake2b.Populate(blake2b512Hash)
	u.partDigests = u.partDigestsMaker.Finish()
	return nil
}

func (u *awsForUpload) MarshalBinary() ([]byte, error) {
	partDigests, err := u.partDigests.MarshalBinary()
	if err != nil {
		return nil, err
	}
	result := make([]byte, 64+len(partDigests))
	if copy(result[0:], u.blake2b[:]) != 64 {
		panic("bad copy")
	}
	if copy(result[64:], partDigests) != len(partDigests) {
		panic("bad copy")
	}
	return result, nil
}

func (u *awsForUpload) UnmarshalBinary(data []byte) error {
	if len(data) < 64 {
		return fmt.Errorf("invalid data")
	}
	if copy(u.blake2b[:], data) != 64 {
		panic("bad copy")
	}
	return u.partDigests.UnmarshalBinary(data[64:])
}
