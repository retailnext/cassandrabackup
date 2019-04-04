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

package nodetool

import (
	"os/exec"

	"go.uber.org/zap"
)

var Tool = "/usr/bin/nodetool"

func TakeSnapshot(name string) error {
	lgr := zap.S()
	cmd := exec.Command(Tool, "-h", "localhost", "snapshot", "-t", name)
	output, err := cmd.CombinedOutput()
	if err != nil {
		lgr.Errorw("take_snapshot_fail", "err", err, "output", output)
		return err
	}
	lgr.Infow("created_snapshot", "name", name)
	return nil
}

func ClearSnapshot(name string) error {
	lgr := zap.S()
	cmd := exec.Command(Tool, "-h", "localhost", "clearsnapshot", "-t", name)
	output, err := cmd.CombinedOutput()
	if err != nil {
		lgr.Errorw("clearsnapshot_fail", "err", err, "output", output)
		return err
	}
	lgr.Infow("cleared_snapshot", "name", name)
	return nil
}
