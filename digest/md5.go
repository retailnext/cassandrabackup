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
	"crypto/md5"
	"hash"
)

type md5Digest [md5.Size]byte

func (d *md5Digest) populate(h hash.Hash) {
	sum := h.Sum(nil)
	if len(sum) != md5.Size {
		panic("bad md5 length")
	}
	copy(d[:], sum)
}
