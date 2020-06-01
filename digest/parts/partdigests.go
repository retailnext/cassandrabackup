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
	"encoding/binary"
	"fmt"
)

type PartDigests struct {
	partSize    uint64
	md5Parts    md5PartDigests
	sha256Parts sha256PartDigests
	totalLength uint64
}

func (pd *PartDigests) TotalLength() int64 {
	return int64(pd.totalLength)
}

func (pd *PartDigests) Parts() int64 {
	numParts := int64(pd.totalLength) / int64(pd.partSize)
	if pd.totalLength%pd.partSize > 0 || pd.totalLength == 0 {
		numParts += 1
	}
	return numParts
}

func (pd *PartDigests) PartOffset(partNumber int64) int64 {
	if partNumber < 0 || partNumber > int64(len(pd.md5Parts)) {
		panic("PartOffset: invalid partNumber")
	}
	return int64(pd.partSize) * (partNumber - 1)
}

func (pd *PartDigests) PartLength(partNumber int64) int64 {
	if partNumber < 1 || partNumber > pd.Parts() {
		panic("PartLength: invalid partNumber")
	}
	if partNumber == pd.Parts() {
		return int64(pd.totalLength % pd.partSize)
	}
	return int64(pd.partSize)
}

func (pd *PartDigests) PartContentMD5(partNumber int64) string {
	if partNumber < 1 || partNumber > pd.Parts() {
		panic("PartContentMD5: invalid partNumber")
	}
	i := int(partNumber) - 1
	return pd.md5Parts[i].String()
}

func (pd *PartDigests) PartContentSHA256(partNumber int64) string {
	if partNumber < 1 || partNumber > pd.Parts() {
		panic("PartContentSHA256: invalid partNumber")
	}
	i := int(partNumber) - 1
	return pd.sha256Parts[i].String()
}

const headerLength = 16
const partSizeOffset = 0
const totalLengthOffset = 8

func (pd *PartDigests) MarshalBinary() (data []byte, err error) {
	output := make([]byte, headerLength+pd.md5Parts.size()+pd.sha256Parts.size())
	binary.BigEndian.PutUint64(output[partSizeOffset:], pd.partSize)
	binary.BigEndian.PutUint64(output[totalLengthOffset:], pd.totalLength)
	pd.md5Parts.marshalTo(output[headerLength:])
	pd.sha256Parts.marshalTo(output[headerLength+pd.md5Parts.size():])
	return output, nil
}

func (pd *PartDigests) UnmarshalBinary(data []byte) error {
	if len(data) < headerLength {
		return fmt.Errorf("invalid data")
	}
	pd.partSize = binary.BigEndian.Uint64(data[partSizeOffset:])
	pd.totalLength = binary.BigEndian.Uint64(data[totalLengthOffset:])
	pd.md5Parts = make(md5PartDigests, pd.Parts())
	pd.sha256Parts = make(sha256PartDigests, pd.Parts())
	if len(data) != headerLength+pd.md5Parts.size()+pd.sha256Parts.size() {
		return fmt.Errorf("invalid data")
	}
	pd.md5Parts.unmarshal(data[headerLength:])
	pd.sha256Parts.unmarshal(data[headerLength+pd.md5Parts.size():])
	return nil
}

func (pd *PartDigests) MarshalText() ([]byte, error) {
	binaryEncoded, err := pd.MarshalBinary()
	if err != nil {
		return nil, err
	}
	text := make([]byte, base64.StdEncoding.EncodedLen(len(binaryEncoded)))
	base64.StdEncoding.Encode(text, binaryEncoded)
	return text, nil
}

func (pd *PartDigests) UnmarshalText(text []byte) error {
	data := make([]byte, base64.StdEncoding.DecodedLen(len(text)))
	if n, err := base64.StdEncoding.Decode(data, text); err != nil {
		return err
	} else {
		data = data[0:n]
	}
	return pd.UnmarshalBinary(data)
}
