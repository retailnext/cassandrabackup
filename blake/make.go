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

package blake

import (
	"context"
	"hash"
	"io"

	"github.com/retailnext/cassandrabackup/paranoid"
	"golang.org/x/crypto/blake2b"
)

const checkContextBytesInterval = 1024 * 1024 * 8
const bufferSize = 4 * 1024 * 8

func computeHash(ctx context.Context, reader io.ReadSeeker, onWrite func(buf []byte, len int)) (hash.Hash, error) {
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
			if onWrite != nil {
				onWrite(buf, bytesRead)
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
				return nil, ctx.Err()
			default:
				lastCheckedDoneCh = size
			}
		}
	}

	return blake2b512Hash, nil
}

func MakeHash(ctx context.Context, reader io.ReadSeeker) (hash.Hash, error) {
	return computeHash(ctx, reader, nil)
}

func MakeFileHash(ctx context.Context, file paranoid.File, onWrite func(buf []byte, len int)) (hash.Hash, error) {
	osFile, err := file.Open()
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := osFile.Close(); closeErr != nil {
			panic(closeErr)
		}
	}()

	blake2b512Hash, err := computeHash(ctx, osFile, onWrite)
	if err != nil {
		return nil, err
	}

	return blake2b512Hash, file.CheckFile(osFile)
}
