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

package restore

import (
	"fmt"

	"go.uber.org/zap/zapcore"
)

type FileErrors map[string]error

func (e FileErrors) Error() string {
	return fmt.Sprintf("%d files failed", len(e))
}

func (e FileErrors) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	for path, err := range e {
		enc.AddString(path, err.Error())
	}
	return nil
}
