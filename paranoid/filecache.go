// Copyright 2023 RetailNext, Inc.
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

package paranoid

import (
	"bytes"
	"encoding/binary"
)

const (
	cacheKeyLen         = 8  // inode
	cacheValueHeaderLen = 24 // sec + nsec + size
)

func (f File) CacheKey() []byte {
	var key [cacheKeyLen]byte
	binary.BigEndian.PutUint64(key[:], f.fingerprint.identity.inode)
	return key[:]
}

func (f File) cacheValueHeader() [cacheValueHeaderLen]byte {
	var header [cacheValueHeaderLen]byte
	binary.BigEndian.PutUint64(header[0:], uint64(f.fingerprint.mtime.Sec))
	binary.BigEndian.PutUint64(header[8:], uint64(f.fingerprint.mtime.Nsec))
	binary.BigEndian.PutUint64(header[16:], uint64(f.fingerprint.size))
	return header
}

func (f File) UnwrapCacheEntry(cacheKey, cacheValue []byte) []byte {
	if !bytes.Equal(f.CacheKey(), cacheKey) {
		return nil
	}
	if len(cacheValue) < cacheValueHeaderLen {
		return nil
	}
	cacheValueHeader := f.cacheValueHeader()
	if !bytes.Equal(cacheValueHeader[:], cacheValue[0:cacheValueHeaderLen]) {
		return nil
	}
	return cacheValue[cacheValueHeaderLen:]
}

func (f File) WrapCacheEntry(data []byte) []byte {
	r := make([]byte, 0, cacheValueHeaderLen+len(data))
	cacheValueHeader := f.cacheValueHeader()
	r = append(r, cacheValueHeader[:]...)
	r = append(r, data...)
	return r
}
