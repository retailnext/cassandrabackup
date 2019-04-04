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
	"fmt"
	"testing"
)

type testCase struct {
	t       Seconds
	decimal string
	string  string
}

func (tc testCase) test() string {
	decimalString := tc.t.Decimal()
	if decimalString != tc.decimal {
		return fmt.Sprintf("wrong Decimal() expected=%q actual=%q", tc.decimal, decimalString)
	}
	var decimalParse Seconds
	if err := decimalParse.ParseDecimal(decimalString); err != nil {
		return err.Error()
	}
	if decimalParse != tc.t {
		return fmt.Sprintf("wrong ParseDecimal() result expected=%d actual=%d", int64(tc.t), int64(decimalParse))
	}

	stringString := tc.t.String()
	if stringString != tc.string {
		return fmt.Sprintf("wrong String() expected=%q actual=%q", tc.string, stringString)
	}
	var stringParse Seconds
	if err := stringParse.ParseString(stringString); err != nil {
		return err.Error()
	}
	if stringParse != tc.t {
		return fmt.Sprintf("wrong ParseString() result expected=%d actual=%d", int64(tc.t), int64(stringParse))
	}

	byteBytes, err := stringParse.MarshalBinary()
	if err != nil {
		return err.Error()
	}
	var byteParse Seconds
	if err := byteParse.UnmarshalBinary(byteBytes); err != nil {
		return err.Error()
	}
	if byteParse != tc.t {
		return fmt.Sprintf("wrong ParseString() result expected=%d actual=%d", int64(tc.t), int64(byteParse))
	}

	return ""
}

var cases = []testCase{
	{
		t:       -1,
		decimal: "-0000000000000000001",
		string:  "1969-12-31T23:59:59Z",
	},
	{
		t:       0,
		decimal: "00000000000000000000",
		string:  "1970-01-01T00:00:00Z",
	},
	{
		t:       1,
		decimal: "00000000000000000001",
		string:  "1970-01-01T00:00:01Z",
	},
	{
		t:       1 << 31,
		decimal: "00000000002147483648",
		string:  "2038-01-19T03:14:08Z",
	},
	{
		t:       1 << 32,
		decimal: "00000000004294967296",
		string:  "2106-02-07T06:28:16Z",
	},
	{
		t:       1 << 33,
		decimal: "00000000008589934592",
		string:  "2242-03-16T12:56:32Z",
	},
}

func TestSeconds(t *testing.T) {
	for i, tc := range cases {
		msg := tc.test()
		if msg != "" {
			t.Errorf("case %d: %s", i, msg)
		}
	}
}
