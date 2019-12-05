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

import "os"

func NewFileFromInfo(name string, info os.FileInfo) File {
	file := File{
		name: name,
	}
	file.fingerprint.fromInfo(info)
	return file
}

func NewFile(name string) (File, error) {
	info, err := os.Stat(name)
	if err != nil {
		return File{}, err
	}
	return NewFileFromInfo(name, info), nil
}

type File struct {
	name        string
	fingerprint fingerprint
}

func (f File) Name() string {
	return f.name
}

func (f File) Len() int64 {
	return f.fingerprint.size
}

// Remove a file only if it matches.
// Returns a non-nil error if the file exits and doesn't match, or if os.Remove fails for a non-NotExist reason.
func (f File) Delete() error {
	err := f.Check()
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	err = os.Remove(f.name)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

func (f File) Check() error {
	info, err := os.Stat(f.name)
	if err != nil {
		return err
	}
	return f.check(info)
}

func (f File) CheckFile(file *os.File) error {
	info, err := file.Stat()
	if err != nil {
		return err
	}
	return f.check(info)
}

func (f File) check(info os.FileInfo) error {
	var current fingerprint
	current.fromInfo(info)
	if f.fingerprint != current {
		return FingerprintMismatch{
			name:     f.name,
			expected: f.fingerprint,
			actual:   current,
		}
	}
	return nil
}

func (f File) Open() (*os.File, error) {
	file, err := os.Open(f.name)
	if err != nil {
		return nil, err
	}
	err = f.CheckFile(file)
	if err != nil {
		if closeErr := file.Close(); closeErr != nil {
			panic(closeErr)
		}
		return nil, err
	}
	return file, nil
}
