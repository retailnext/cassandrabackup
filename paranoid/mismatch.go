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

package paranoid

import "fmt"

type FingerprintMismatch struct {
	name     string
	expected fingerprint
	actual   fingerprint
}

func (e FingerprintMismatch) Error() string {
	return fmt.Sprintf("osFile modified: name=%q expected=%+v actual=%+v", e.name, e.expected, e.actual)
}

func IsFingerprintMismatch(err error) bool {
	switch err.(type) {
	case *FingerprintMismatch:
		return true
	case FingerprintMismatch:
		return true
	default:
		return false
	}
}
