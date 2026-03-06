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

package bucket

import (
	"errors"

	"github.com/aws/smithy-go"
	"go.uber.org/zap"
)

func IsNoSuchKey(err error) bool {
	if err != nil {
		var apiErr smithy.APIError
		if errors.As(err, &apiErr) {
			switch apiErr.ErrorCode() {
			case "NoSuchKey", "NotFound":
				return true
			case "RequestCanceled":
				return false
			default:
				zap.S().Infow("other_aws_error", "code", apiErr.ErrorCode(), "error", apiErr.Error(), "message", apiErr.ErrorMessage())
				return false
			}
		}
	}
	return false
}
