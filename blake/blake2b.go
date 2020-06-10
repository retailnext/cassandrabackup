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
	"encoding/base64"
	"errors"
	"fmt"
	"hash"

	"github.com/mailru/easyjson/jlexer"
	"github.com/mailru/easyjson/jwriter"
)

const blake2bDigestLength = 64

type Blake2bDigest [blake2bDigestLength]byte

func (d Blake2bDigest) URLSafe() string {
	return base64.URLEncoding.EncodeToString(d[:])
}

func (d Blake2bDigest) MarshalText() ([]byte, error) {
	text := make([]byte, base64.StdEncoding.EncodedLen(blake2bDigestLength))
	base64.StdEncoding.Encode(text, d[:])
	return text, nil
}

func (d *Blake2bDigest) UnmarshalText(text []byte) error {
	if len(text) != base64.StdEncoding.EncodedLen(blake2bDigestLength) {
		return fmt.Errorf("invalid text")
	}
	data := make([]byte, base64.StdEncoding.DecodedLen(len(text)))
	if n, err := base64.StdEncoding.Decode(data, text); err != nil {
		return err
	} else if n != blake2bDigestLength {
		return fmt.Errorf("invalid text")
	}
	copy(d[:], data[0:blake2bDigestLength])
	return nil
}

func (d Blake2bDigest) MarshalEasyJSON(w *jwriter.Writer) {
	w.Base64Bytes(d[:])
}

func (d *Blake2bDigest) UnmarshalEasyJSON(r *jlexer.Lexer) {
	b := r.Bytes()
	if len(b) != blake2bDigestLength {
		r.AddNonFatalError(errors.New("invalid length"))
	} else {
		copy(d[:], b)
	}
}

func (d Blake2bDigest) MarshalBinary() ([]byte, error) {
	result := make([]byte, blake2bDigestLength)
	copy(result, d[:])
	return result, nil
}

func (d *Blake2bDigest) UnmarshalBinary(data []byte) error {
	if len(data) != blake2bDigestLength {
		return blake2bDigestInvalidLength
	}
	copy(d[:], data)
	return nil
}

var blake2bDigestInvalidLength = errors.New("blake2bDigest: invalid length")

func (d *Blake2bDigest) Populate(h hash.Hash) {
	sum := h.Sum(nil)
	if len(sum) != blake2bDigestLength {
		panic("bad hash.Sum() length")
	}
	copy(d[:], sum)
}
