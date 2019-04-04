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
	"cassandrabackup/digest/parts"
	"cassandrabackup/paranoid"
	"context"
	"encoding/base64"
	"fmt"
	"io"

	"golang.org/x/crypto/blake2b"
)

type ForUpload struct {
	blake2b     blake2bDigest
	partDigests parts.PartDigests
}

func (u *ForUpload) URLSafe() string {
	return base64.RawURLEncoding.EncodeToString(u.blake2b[:])
}

func (u *ForUpload) Parts() int64 {
	return u.partDigests.Parts()
}

func (u ForUpload) PartOffset(partNumber int64) int64 {
	return u.partDigests.PartOffset(partNumber)
}

func (u ForUpload) PartLength(partNumber int64) int64 {
	return u.partDigests.PartLength(partNumber)
}

func (u ForUpload) ContentLength() int64 {
	return int64(u.partDigests.TotalLength())
}

func (u ForUpload) PartContentMD5(partNumber int64) string {
	return u.partDigests.PartContentMD5(partNumber)
}

func (u ForUpload) PartContentSHA256(partNumber int64) string {
	return u.partDigests.PartContentSHA256(partNumber)
}

func (u *ForUpload) ForRestore() ForRestore {
	return ForRestore{
		blake2b: u.blake2b,
	}
}

const checkContextBytesInterval = 1024 * 1024 * 8

func (u *ForUpload) populate(ctx context.Context, file paranoid.File) error {
	osFile, err := file.Open()
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := osFile.Close(); closeErr != nil {
			panic(closeErr)
		}
	}()

	var partDigestsMaker parts.PartDigestsMaker
	partDigestsMaker.Reset(1024 * 1024 * 64)

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
			return err
		}
		if bytesRead > 0 {
			if n, err := partDigestsMaker.Write(buf[0:bytesRead]); err != nil {
				panic(err)
			} else if n != bytesRead {
				panic(io.ErrShortWrite)
			}
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
				return ctx.Err()
			default:
				lastCheckedDoneCh = size
			}
		}
	}

	if err := file.CheckFile(osFile); err != nil {
		return err
	}

	u.blake2b.populate(blake2b512Hash)
	u.partDigests = partDigestsMaker.Finish()
	return nil
}

func (u *ForUpload) MarshalBinary() ([]byte, error) {
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

func (u *ForUpload) UnmarshalBinary(data []byte) error {
	if len(data) < 64 {
		return fmt.Errorf("invalid data")
	}
	if copy(u.blake2b[:], data) != 64 {
		panic("bad copy")
	}
	return u.partDigests.UnmarshalBinary(data[64:])
}
