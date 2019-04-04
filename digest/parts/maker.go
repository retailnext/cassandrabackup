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
	"crypto/md5"
	"crypto/sha256"
	"hash"
	"io"
)

type PartDigestsMaker struct {
	result PartDigests

	md5Hash    hash.Hash
	sha256Hash hash.Hash
	pending    uint64
}

func (pd *PartDigestsMaker) Reset(partSize uint64) {
	if partSize <= 0 {
		panic("Reset: partSize must be >= 0")
	}
	pd.result.partSize = partSize
	pd.result.md5Parts = nil
	pd.result.sha256Parts = nil
	pd.result.totalLength = 0
	if pd.md5Hash == nil {
		pd.md5Hash = md5.New()
	} else {
		pd.md5Hash.Reset()
	}
	if pd.sha256Hash == nil {
		pd.sha256Hash = sha256.New()
	} else {
		pd.sha256Hash.Reset()
	}
	pd.pending = 0
}

func (pd *PartDigestsMaker) Finish() PartDigests {
	if pd.pending > 0 || len(pd.result.md5Parts) == 0 {
		pd.flushPart()
	}
	pd.md5Hash = nil
	return pd.result
}

func (pd *PartDigestsMaker) Write(p []byte) (int, error) {
	var n int

	for len(p) > 0 {
		if pd.pending == pd.result.partSize {
			pd.flushPart()
		}

		var p0, p1 []byte
		remaining := pd.result.partSize - pd.pending
		if remaining < uint64(len(p)) {
			p0 = p[0:remaining]
			p1 = p[remaining:]
		} else {
			p0 = p
		}
		thisWriteLen := len(p0)
		if n0, err := pd.md5Hash.Write(p0); err != nil {
			panic(err)
		} else if n0 != thisWriteLen {
			panic(io.ErrShortWrite)
		}
		if n0, err := pd.sha256Hash.Write(p0); err != nil {
			panic(err)
		} else if n0 != thisWriteLen {
			panic(io.ErrShortWrite)
		}
		pd.pending += uint64(thisWriteLen)
		n += thisWriteLen
		p = p1
	}

	return n, nil
}

func (pd *PartDigestsMaker) flushPart() {
	var thisMd5Part md5Digest
	thisMd5Part.populate(pd.md5Hash)
	pd.md5Hash.Reset()
	pd.result.md5Parts = append(pd.result.md5Parts, thisMd5Part)

	var thisSha256Part sha256Digest
	thisSha256Part.populate(pd.sha256Hash)
	pd.sha256Hash.Reset()
	pd.result.sha256Parts = append(pd.result.sha256Parts, thisSha256Part)

	pd.result.totalLength += pd.pending
	pd.pending = 0
}
