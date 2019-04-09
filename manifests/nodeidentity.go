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

package manifests

import "go.uber.org/zap/zapcore"

type NodeIdentity struct {
	Cluster  string
	Hostname string
}

func (ni NodeIdentity) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("cluster", ni.Cluster)
	enc.AddString("hostname", ni.Hostname)
	return nil
}
