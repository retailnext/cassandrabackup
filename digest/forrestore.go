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

	"github.com/mailru/easyjson/jlexer"
	"github.com/mailru/easyjson/jwriter"
	"golang.org/x/crypto/blake2b"
)

type ForRestore struct {
	blake2b blake2bDigest
}

func (r ForRestore) URLSafe() string {
	return r.blake2b.URLSafe()
}

func (r ForRestore) Verify(ctx context.Context, reader io.ReadSeeker) error {
	_, err := reader.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	blake2b512Hash, err := blake2b.New512(nil)
	if err != nil {
		panic(err)
	}

	buf := make([]byte, 64*128*4)
	var doneCh <-chan struct{}
	var lastCheckedDoneCh int64
	var size int64
	for {
		bytesRead, err := reader.Read(buf)
		if err != nil && err != io.EOF {
			return err
		}
		if bytesRead > 0 {
			if _, err := blake2b512Hash.Write(buf[0:bytesRead]); err != nil {
				panic(err)
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

	var actual blake2bDigest
	actual.populate(blake2b512Hash)
	if r.blake2b != actual {
		return MismatchError{
			expected: r.blake2b,
			actual:   actual,
		}
	}
	return nil
}

func (r ForRestore) MarshalText() ([]byte, error) {
	return r.blake2b.MarshalText()
}

func (r *ForRestore) UnmarshalText(text []byte) error {
	return r.blake2b.UnmarshalText(text)
}

func (r *ForRestore) MarshalEasyJSON(w *jwriter.Writer) {
	r.blake2b.MarshalEasyJSON(w)
}

func (r *ForRestore) UnmarshalEasyJSON(l *jlexer.Lexer) {
	r.blake2b.UnmarshalEasyJSON(l)
}

func (r *ForRestore) MarshalBinary() ([]byte, error) {
	return r.blake2b.MarshalBinary()
}

func (r *ForRestore) UnmarshalBinary(data []byte) error {
	return r.blake2b.UnmarshalBinary(data)
}

type MismatchError struct {
	expected blake2bDigest
	actual   blake2bDigest
}

func (e MismatchError) Error() string {
	expected, err := e.expected.MarshalText()
	if err != nil {
		panic(err)
	}
	actual, err := e.actual.MarshalText()
	if err != nil {
		panic(err)
	}
	return fmt.Sprintf("digest mismatch: expected=%s actual=%s", string(expected), string(actual))
}
