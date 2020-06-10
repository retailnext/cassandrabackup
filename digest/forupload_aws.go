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
	"fmt"
	"io"

	"github.com/retailnext/cassandrabackup/blake"
	"github.com/retailnext/cassandrabackup/digest/parts"
	"github.com/retailnext/cassandrabackup/paranoid"
)

type AWSForUpload struct {
	blake2b          blake.Blake2bDigest
	partDigestsMaker parts.PartDigestsMaker
	PartDigests      parts.PartDigests
}

func (u *AWSForUpload) TotalLength() int64 {
	return u.PartDigests.TotalLength()
}

func (u *AWSForUpload) onWrite(buf []byte, len int) {
	if n, err := u.partDigestsMaker.Write(buf[0:len]); err != nil {
		panic(err)
	} else if n != len {
		panic(io.ErrShortWrite)
	}
}

func (u *AWSForUpload) ForRestore() ForRestore {
	return NewForRestore(u.blake2b)
}

const partSize = 1024 * 1024 * 64

func (u *AWSForUpload) Populate(ctx context.Context, file paranoid.File) error {
	u.partDigestsMaker.Reset(partSize)

	blake2b512Hash, err := blake.MakeFileHash(ctx, file, u.onWrite)
	if err != nil {
		return err
	}

	u.blake2b.Populate(blake2b512Hash)
	u.PartDigests = u.partDigestsMaker.Finish()
	return nil
}

func (u *AWSForUpload) MarshalBinary() ([]byte, error) {
	partDigests, err := u.PartDigests.MarshalBinary()
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

func (u *AWSForUpload) UnmarshalBinary(data []byte) error {
	if len(data) < 64 {
		return fmt.Errorf("invalid data")
	}
	if copy(u.blake2b[:], data) != 64 {
		panic("bad copy")
	}
	return u.PartDigests.UnmarshalBinary(data[64:])
}
