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

package plan

import (
	"strings"

	"go.uber.org/zap"
)

type Filter struct {
	Tables         map[string]struct{}
	IncludeIndexes bool
}

func (f *Filter) Build(tables []string) {
	f.Tables = make(map[string]struct{}, len(tables))

	for _, tableSpec := range tables {
		parts := strings.Split(tableSpec, ".")
		if len(parts) != 2 {
			zap.S().Panicw("invalid_table", "table", tableSpec)
		}
		f.Tables[tableSpec] = struct{}{}
	}
}

func (f Filter) match(name string) bool {
	parts := strings.Split(name, "/")
	if len(parts) < 3 {
		zap.S().Panicw("unexpected_name", "name", name)
		return false
	}
	if !f.IncludeIndexes {
		if parts[2] == "" {
			zap.S().Panicw("unexpected_empty_part", "name", name)
			return false
		}
		if parts[2][0] == '.' {
			return false
		}
	}
	suffixIndex := strings.LastIndex(parts[1], "-")
	if suffixIndex < 0 {
		zap.S().Panicw("unexpected_suffix_index", "name", name)
	}
	keyspace := parts[0]
	table := parts[1][:suffixIndex]
	_, ok := f.Tables[keyspace+"."+table]
	return ok
}

func (p *NodePlan) Filter(f Filter) {
	for fileName := range p.Files {
		if !f.match(fileName) {
			delete(p.Files, fileName)
		}
	}
	for fileName := range p.ChangedFiles {
		if !f.match(fileName) {
			delete(p.ChangedFiles, fileName)
		}
	}
}
