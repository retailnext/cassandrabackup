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
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"hash"
	"io"

	"github.com/retailnext/cassandrabackup/bucket/config"
	"github.com/retailnext/cassandrabackup/digest/parts"
	"github.com/retailnext/cassandrabackup/paranoid"
	"golang.org/x/crypto/blake2b"
)

type ForUpload struct {
	blake2b     blake2bDigest
	partDigests *parts.PartDigests
	md5         md5Digest
	totalLength int64
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
	return u.totalLength
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

func (u *ForUpload) MD5() []byte {
	return u.md5[:]
}

const checkContextBytesInterval = 1024 * 1024 * 8
const partSize = 1024 * 1024 * 64
const bufferSize = 4 * 1024 * 8

func computeHash(ctx context.Context, reader io.Reader, md5Hash hash.Hash, partDigestsMaker *parts.PartDigestsMaker) (hash.Hash, error) {
	blake2b512Hash, err := blake2b.New512(nil)
	if err != nil {
		panic(err)
	}

	buf := make([]byte, bufferSize)
	var doneCh <-chan struct{}
	var lastCheckedDoneCh int64
	var size int64
	for {
		bytesRead, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			return nil, err
		}
		if bytesRead > 0 {
			if n, err := blake2b512Hash.Write(buf[0:bytesRead]); err != nil {
				panic(err)
			} else if n != bytesRead {
				panic(io.ErrShortWrite)
			}
			if partDigestsMaker != nil {
				if n, err := partDigestsMaker.Write(buf[0:bytesRead]); err != nil {
					panic(err)
				} else if n != bytesRead {
					panic(io.ErrShortWrite)
				}
			}
			if md5Hash != nil {
				if n, err := md5Hash.Write(buf[0:bytesRead]); err != nil {
					panic(err)
				} else if n != bytesRead {
					panic(io.ErrShortWrite)
				}
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

	return blake2b512Hash, nil
}

func (u *ForUpload) populate(ctx context.Context, file paranoid.File, provider config.CloudProvider) error {
	var md5Hash hash.Hash
	var partDigestsMaker *parts.PartDigestsMaker

	if provider.IsAWS() {
		partDigestsMaker = &parts.PartDigestsMaker{}
		partDigestsMaker.Reset(partSize)
	} else {
		md5Hash = md5.New()
	}

	u.totalLength = file.Len()
	osFile, err := file.Open()
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := osFile.Close(); closeErr != nil {
			panic(closeErr)
		}
	}()

	blake2b512Hash, err := computeHash(ctx, osFile, md5Hash, partDigestsMaker)
	if err != nil {
		return err
	}

	if err := file.CheckFile(osFile); err != nil {
		return err
	}

	u.blake2b.populate(blake2b512Hash)

	if partDigestsMaker != nil {
		pd := partDigestsMaker.Finish()
		u.partDigests = &pd
	}

	if md5Hash != nil {
		u.md5.populate(md5Hash)
	}

	return nil
}

func (u *ForUpload) MarshalBinary() ([]byte, error) {
	if u.partDigests != nil {
		// AWS cache format
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

	// GCP cache format
	result := make([]byte, 64+md5.Size+8)
	if copy(result[0:], u.blake2b[:]) != 64 {
		panic("bad copy")
	}
	if copy(result[64:], u.md5[:]) != md5.Size {
		panic("bad copy")
	}
	binary.BigEndian.PutUint64(result[64+md5.Size:], uint64(u.totalLength))
	return result, nil
}

func (u *ForUpload) UnmarshalBinary(data []byte) error {
	if len(data) < 64 {
		return fmt.Errorf("invalid data")
	}
	if copy(u.blake2b[:], data) != 64 {
		panic("bad copy")
	}

	// the signature to identify the AWS cache format, is when the first 31 bits
	// after the blake2b digest are zero since MD5 will never have 31 zero bits
	// and an AWS partSize can never be larger than 5GB in the AWS platform
	if binary.BigEndian.Uint64(data[64:])>>33 == 0 {
		// AWS cache format
		u.partDigests = &parts.PartDigests{}
		err := u.partDigests.UnmarshalBinary(data[64:])
		if err != nil {
			return err
		}
		u.totalLength = u.partDigests.TotalLength()
		return nil
	}

	// GCP cache format
	if copy(u.md5[:], data[64:]) != md5.Size {
		panic("bad copy")
	}
	u.totalLength = int64(binary.BigEndian.Uint64(data[64+md5.Size:]))
	return nil
}
