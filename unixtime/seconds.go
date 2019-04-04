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

package unixtime

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/mailru/easyjson/jlexer"
	"github.com/mailru/easyjson/jwriter"
)

func Now() Seconds {
	return Seconds(time.Now().Unix())
}

type Seconds int64

func (t Seconds) Decimal() string {
	return fmt.Sprintf("%020d", t)
}

func (t Seconds) String() string {
	return time.Unix(int64(t), 0).UTC().Format(time.RFC3339)
}

func (t *Seconds) ParseDecimal(value string) error {
	v, err := strconv.ParseInt(value, 10, 64)
	if err == nil {
		*t = Seconds(v)
	}
	return err
}

var NonZeroNanoseconds = errors.New("time has nonzero nanoseconds")

func (t *Seconds) ParseString(value string) error {
	gotime, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return err
	}
	if gotime.Nanosecond() != 0 {
		return NonZeroNanoseconds
	}
	*t = Seconds(gotime.Unix())
	return nil
}

func (t *Seconds) ParseStringIgnoreNanoseconds(value string) error {
	gotime, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return err
	}
	*t = Seconds(gotime.Unix())
	return nil
}

func (t Seconds) MarshalEasyJSON(w *jwriter.Writer) {
	w.String(t.String())
}

func (t *Seconds) UnmarshalEasyJSON(l *jlexer.Lexer) {
	if err := t.ParseStringIgnoreNanoseconds(l.String()); err != nil {
		l.AddNonFatalError(err)
	}
}

const secondsBinaryLength = 8

var secondsBinaryInvalidLength = errors.New("unixtime.Seconds: invalid length")

func (t Seconds) MarshalBinary() ([]byte, error) {
	result := make([]byte, secondsBinaryLength)
	binary.BigEndian.PutUint64(result, uint64(t))
	return result, nil
}

func (t *Seconds) UnmarshalBinary(data []byte) error {
	if len(data) != secondsBinaryLength {
		return secondsBinaryInvalidLength
	}
	*t = Seconds(int64(binary.BigEndian.Uint64(data)))
	return nil
}
