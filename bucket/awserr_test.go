// Copyright 2024 RetailNext, Inc.
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
	"testing"

	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/go-test/deep"
)

func TestIsNoSuchKey(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected bool
	}{
		{
			name:     "nil error",
			err:      nil,
			expected: false,
		},
		{
			name:     "non-aws error",
			err:      errors.New("some error"),
			expected: false,
		},
		{
			name: "NoSuchKey error",
			err: &smithy.OperationError{
				ServiceID:     "S3",
				OperationName: "GetObject",
				Err: &smithyhttp.ResponseError{
					Response: &smithyhttp.Response{},
					Err: &smithy.GenericAPIError{
						Code:    "NoSuchKey",
						Message: "The specified key does not exist.",
					},
				},
			},
			expected: true,
		},
		{
			name: "NotFound error",
			err: &smithy.OperationError{
				ServiceID:     "S3",
				OperationName: "HeadObject",
				Err: &smithyhttp.ResponseError{
					Response: &smithyhttp.Response{},
					Err: &smithy.GenericAPIError{
						Code:    "NotFound",
						Message: "Not Found",
					},
				},
			},
			expected: true,
		},
		{
			name: "other AWS error",
			err: &smithy.OperationError{
				ServiceID:     "S3",
				OperationName: "GetObject",
				Err: &smithyhttp.ResponseError{
					Response: &smithyhttp.Response{},
					Err: &smithy.GenericAPIError{
						Code:    "AccessDenied",
						Message: "Access Denied",
					},
				},
			},
			expected: false,
		},
		{
			name:     "context canceled",
			err:      errors.New("RequestCanceled"),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := IsNoSuchKey(tt.err)
			if diff := deep.Equal(result, tt.expected); diff != nil {
				t.Errorf("IsNoSuchKey() %v", diff)
			}
		})
	}
}
