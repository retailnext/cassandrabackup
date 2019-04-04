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

import (
	"os"
	"syscall"
)

type fingerprint struct {
	identity identity
	size     int64
	mtime    syscall.Timespec
}

func (fp *fingerprint) fromInfo(info os.FileInfo) {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		fp.identity.populate(stat)
		fp.size = stat.Size
		fp.mtime = MtimeFromStat(stat)
	} else {
		panic("paranoid: unsupported FileInfo.Sys()")
	}
}

func MtimeFromInfo(info os.FileInfo) syscall.Timespec {
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		MtimeFromStat(stat)
	}
	panic("paranoid: unsupported FileInfo.Sys()")
}
