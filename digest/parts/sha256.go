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

package parts

import (
	"encoding/hex"
	"hash"
)

const sha256DigestLength = 32

type sha256Digest [sha256DigestLength]byte

type sha256PartDigests []sha256Digest

func (d sha256Digest) String() string {
	return hex.EncodeToString(d[:])
}

func (d *sha256Digest) populate(h hash.Hash) {
	digest := h.Sum(nil)
	if len(digest) != sha256DigestLength {
		panic("bad hash.Sum() length")
	}
	copy(d[:], digest)
}

func (s sha256PartDigests) size() int {
	return sha256DigestLength * len(s)
}

func (s sha256PartDigests) marshalTo(data []byte) {
	for i := range s {
		copy(data[sha256DigestLength*i:], s[i][:])
	}
}

func (s sha256PartDigests) unmarshal(data []byte) {
	for i := range s {
		copy(s[i][:], data[sha256DigestLength*i:])
	}
}
