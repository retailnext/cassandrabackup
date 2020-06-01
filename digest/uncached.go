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

package digest

import (
	"context"

	"github.com/retailnext/cassandrabackup/paranoid"
)

func GetUncached(ctx context.Context, file paranoid.File, forUpload func() ForUpload) (ForUpload, error) {
	result := forUpload()
	err := result.populate(ctx, file)
	return result, err
}
