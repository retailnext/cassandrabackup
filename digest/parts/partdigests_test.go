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

package parts

import (
	"crypto/md5"
	"crypto/sha256"
	"fmt"
	"testing"
)

type partsDigestTestCase struct {
	partSize    uint64
	writes      [][]byte
	md5Parts    []md5Digest
	sha256Parts []sha256Digest
}

func (tc *partsDigestTestCase) exec() string {
	var maker PartDigestsMaker
	maker.Reset(tc.partSize)
	var t uint64
	for writeNum, writeBytes := range tc.writes {
		n, err := maker.Write(writeBytes)
		if n != len(writeBytes) || err != nil {
			return fmt.Sprintf("write %d failed pending=%d err=%s", writeNum, n, err)
		} else {
			t += uint64(n)
		}
	}
	pd := maker.Finish()

	expected := PartDigests{
		partSize:    tc.partSize,
		totalLength: t,
		md5Parts:    tc.md5Parts,
		sha256Parts: tc.sha256Parts,
	}

	if msg := comparePartsDigests(expected, pd); msg != "" {
		return msg
	}

	marshalled, err := pd.MarshalBinary()
	if err != nil {
		return fmt.Sprintf("MarshalBinary failed: %s", err)
	}
	pd = PartDigests{}
	err = pd.UnmarshalBinary(marshalled)
	if err != nil {
		return fmt.Sprintf("UnmarshalBinary failed: %s", err)
	}
	if msg := comparePartsDigests(expected, pd); msg != "" {
		return fmt.Sprintf("MarshalBinary round trip failed: %s", msg)
	}

	marshalled, err = pd.MarshalText()
	if err != nil {
		return fmt.Sprintf("MarshalText failed: %s", err)
	}
	pd = PartDigests{}
	err = pd.UnmarshalText(marshalled)
	if err != nil {
		return fmt.Sprintf("UnmarshalText failed: %s", err)
	}
	if msg := comparePartsDigests(expected, pd); msg != "" {
		return fmt.Sprintf("MarshalText round trip failed: %s", msg)
	}

	return ""
}

func comparePartsDigests(expected, actual PartDigests) string {
	if expected.partSize != actual.partSize {
		return fmt.Sprintf("wrong partSize expected=%d actual=%d", expected.partSize, actual.partSize)
	}

	if expected.totalLength != actual.totalLength {
		return fmt.Sprintf("wrong totalLength expected=%d actual=%d", expected.totalLength, actual.totalLength)
	}

	if len(expected.md5Parts) != len(actual.md5Parts) {
		return fmt.Sprintf("wrong number of md5Parts expected=%d actual=%d", len(expected.md5Parts), len(actual.md5Parts))
	}

	if len(expected.sha256Parts) != len(actual.sha256Parts) {
		return fmt.Sprintf("wrong number of sha256Parts expected=%d actual=%d", len(expected.sha256Parts), len(actual.sha256Parts))
	}

	for partNum := range expected.md5Parts {
		if expected.md5Parts[partNum] != actual.md5Parts[partNum] {
			return fmt.Sprintf("wrong md5 digest part=%d expected=%v actual=%v", partNum, expected.md5Parts[partNum], actual.md5Parts[partNum])
		}
	}

	for partNum := range expected.sha256Parts {
		if expected.sha256Parts[partNum] != actual.sha256Parts[partNum] {
			return fmt.Sprintf("wrong sha256 digest part=%d expected=%v actual=%v", partNum, expected.sha256Parts[partNum], actual.sha256Parts[partNum])
		}
	}

	return ""
}

func md5Of(data []byte) md5Digest {
	var result md5Digest
	h := md5.New()
	_, err := h.Write(data)
	if err != nil {
		panic(err)
	}
	copy(result[:], h.Sum(nil))
	return result
}

func sha256Of(data []byte) sha256Digest {
	var result sha256Digest
	h := sha256.New()
	_, err := h.Write(data)
	if err != nil {
		panic(err)
	}
	copy(result[:], h.Sum(nil))
	return result
}

var partsDigestTestCases = []partsDigestTestCase{
	{
		partSize: 1024,
		writes: [][]byte{
			make([]byte, 0),
			make([]byte, 0),
		},
		md5Parts: []md5Digest{
			md5Of(make([]byte, 0)),
		},
		sha256Parts: []sha256Digest{
			sha256Of(make([]byte, 0)),
		},
	},
	{
		partSize: 1024,
		writes: [][]byte{
			nil,
			make([]byte, 0),
			make([]byte, 1),
			{1, 2},
			make([]byte, 1021),
			make([]byte, 3072),
		},
		md5Parts: []md5Digest{
			md5Of(func() (part []byte) {
				part = make([]byte, 1024)
				part[1] = 1
				part[2] = 2
				return
			}()),
			md5Of(make([]byte, 1024)),
			md5Of(make([]byte, 1024)),
			md5Of(make([]byte, 1024)),
		},
		sha256Parts: []sha256Digest{
			sha256Of(func() (part []byte) {
				part = make([]byte, 1024)
				part[1] = 1
				part[2] = 2
				return
			}()),
			sha256Of(make([]byte, 1024)),
			sha256Of(make([]byte, 1024)),
			sha256Of(make([]byte, 1024)),
		},
	},
	{
		partSize: 1024,
		writes: [][]byte{
			make([]byte, 1024),
		},
		md5Parts: []md5Digest{
			md5Of(make([]byte, 1024)),
		},
		sha256Parts: []sha256Digest{
			sha256Of(make([]byte, 1024)),
		},
	},
	{
		partSize: 1024,
		writes: [][]byte{
			make([]byte, 1024),
			make([]byte, 512),
		},
		md5Parts: []md5Digest{
			md5Of(make([]byte, 1024)),
			md5Of(make([]byte, 512)),
		},
		sha256Parts: []sha256Digest{
			sha256Of(make([]byte, 1024)),
			sha256Of(make([]byte, 512)),
		},
	},
	{
		partSize: 1024,
		writes: [][]byte{
			make([]byte, 1025),
			make([]byte, 511),
		},
		md5Parts: []md5Digest{
			md5Of(make([]byte, 1024)),
			md5Of(make([]byte, 512)),
		},
		sha256Parts: []sha256Digest{
			sha256Of(make([]byte, 1024)),
			sha256Of(make([]byte, 512)),
		},
	},
	{
		partSize: 1024,
		writes: [][]byte{
			make([]byte, 1023),
			make([]byte, 513),
		},
		md5Parts: []md5Digest{
			md5Of(make([]byte, 1024)),
			md5Of(make([]byte, 512)),
		},
		sha256Parts: []sha256Digest{
			sha256Of(make([]byte, 1024)),
			sha256Of(make([]byte, 512)),
		},
	},
}

func TestPartsDigests(t *testing.T) {
	for testCaseNum, testCase := range partsDigestTestCases {
		msg := testCase.exec()
		if msg != "" {
			t.Errorf("case %d failed: %s", testCaseNum, msg)
		}
	}
}
