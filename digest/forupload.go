// Copyright 2020 RetailNext, Inc.
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

package digest

import (
	"context"

	"github.com/retailnext/cassandrabackup/digest/parts"
	"github.com/retailnext/cassandrabackup/paranoid"
)

type ForUploadFactory interface {
	CreateForUpload() ForUpload
}

type awsForUploadFactory struct{}

func (f *awsForUploadFactory) CreateForUpload() ForUpload {
	return &awsForUpload{}
}

type googleForUploadFactory struct{}

func (f *googleForUploadFactory) CreateForUpload() ForUpload {
	return &googleForUpload{}
}

type ForUpload interface {
	UnmarshalBinary(data []byte) error
	MarshalBinary() ([]byte, error)
	Populate(ctx context.Context, file paranoid.File) error
	PartDigests() *parts.PartDigests
	ForRestore() ForRestore
}
