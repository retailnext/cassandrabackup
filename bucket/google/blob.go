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

package google

import (
	"context"
	"os"

	"github.com/retailnext/cassandrabackup/bucket/keystore"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/paranoid"
)

func (c *gcsClient) DownloadBlob(ctx context.Context, digests digest.ForRestore, file *os.File) error {
	return nil
}

func (c *gcsClient) PutBlob(ctx context.Context, file paranoid.File, digests digest.ForUpload) error {
	return nil
}

func (c *gcsClient) KeyStore() *keystore.KeyStore {
	return &c.keyStore
}
