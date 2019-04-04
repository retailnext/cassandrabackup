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
	"encoding/base64"
	"hash"
)

const md5DigestLength = 16

type md5Digest [md5DigestLength]byte

type md5PartDigests []md5Digest

func (d *md5Digest) populate(h hash.Hash) {
	digest := h.Sum(nil)
	if len(digest) != md5DigestLength {
		panic("bad hash.Sum() length")
	}
	copy(d[:], digest)
}

func (d md5Digest) String() string {
	return base64.StdEncoding.EncodeToString(d[:])
}

func (s md5PartDigests) size() int {
	return md5DigestLength * len(s)
}

func (s md5PartDigests) marshalTo(data []byte) {
	for i := range s {
		copy(data[md5DigestLength*i:], s[i][:])
	}
}

func (s md5PartDigests) unmarshal(data []byte) {
	for i := range s {
		copy(s[i][:], data[md5DigestLength*i:])
	}
}
