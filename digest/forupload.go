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
	"hash"
	"io"

	"github.com/retailnext/cassandrabackup/digest/parts"
	"github.com/retailnext/cassandrabackup/paranoid"
	"golang.org/x/crypto/blake2b"
)

type ForUpload interface {
	UnmarshalBinary(data []byte) error
	MarshalBinary() ([]byte, error)
	populate(ctx context.Context, file paranoid.File) error
	onWrite(buf []byte, len int)
	PartDigests() *parts.PartDigests
	ForRestore() ForRestore
}

type ForUploadAWS struct {
	blake2b          blake2bDigest
	partDigestsMaker parts.PartDigestsMaker
	partDigests      parts.PartDigests
}

func (u *ForUploadAWS) URLSafe() string {
	return base64.RawURLEncoding.EncodeToString(u.blake2b[:])
}

func (u *ForUploadAWS) PartDigests() *parts.PartDigests {
	return &u.partDigests
}

func (u *ForUploadAWS) onWrite(buf []byte, len int) {
	if n, err := u.partDigestsMaker.Write(buf[0:len]); err != nil {
		panic(err)
	} else if n != len {
		panic(io.ErrShortWrite)
	}
}

func (u *ForUploadAWS) ForRestore() ForRestore {
	return ForRestore{
		blake2b: u.blake2b,
	}
}

const checkContextBytesInterval = 1024 * 1024 * 8
const partSize = 1024 * 1024 * 64

func makeHash(ctx context.Context, file paranoid.File, u ForUpload) (hash.Hash, error) {
	osFile, err := file.Open()
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := osFile.Close(); closeErr != nil {
			panic(closeErr)
		}
	}()

	blake2b512Hash, err := blake2b.New512(nil)
	if err != nil {
		panic(err)
	}

	buf := make([]byte, 32*1024)
	var doneCh <-chan struct{}
	var lastCheckedDoneCh int64
	var size int64
	for {
		bytesRead, err := osFile.Read(buf)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if bytesRead > 0 {
			u.onWrite(buf, bytesRead)
			if n, err := blake2b512Hash.Write(buf[0:bytesRead]); err != nil {
				panic(err)
			} else if n != bytesRead {
				panic(io.ErrShortWrite)
			}
		}
		size += int64(bytesRead)
		if err == io.EOF {
			break
		}

		if size-lastCheckedDoneCh > checkContextBytesInterval {
			if doneCh == nil {
				doneCh = ctx.Done()
			}

			select {
			case <-doneCh:
				return nil, ctx.Err()
			default:
				lastCheckedDoneCh = size
			}
		}
	}

	err = file.CheckFile(osFile)
	return blake2b512Hash, err
}

func (u *ForUploadAWS) populate(ctx context.Context, file paranoid.File) error {
	u.partDigestsMaker.Reset(partSize)

	blake2b512Hash, err := makeHash(ctx, file, u)
	if err != nil {
		return err
	}

	u.blake2b.populate(blake2b512Hash)
	u.partDigests = u.partDigestsMaker.Finish()
	return nil
}

func (u *ForUploadAWS) MarshalBinary() ([]byte, error) {
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

func (u *ForUploadAWS) UnmarshalBinary(data []byte) error {
	if len(data) < 64 {
		return fmt.Errorf("invalid data")
	}
	if copy(u.blake2b[:], data) != 64 {
		panic("bad copy")
	}
	return u.partDigests.UnmarshalBinary(data[64:])
}
