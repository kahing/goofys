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

package main

import (
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

type Inode struct {
	Name *string
	FullName *string
	Id fuseops.InodeID
	Attributes fuseops.InodeAttributes
}

type DirHandle struct {
	Name *string
	FullName *string
	Entries []fuseutil.Dirent
	NameToEntry map[string]fuseops.InodeAttributes
	Marker *string
	BaseOffset int
}

func (dh *DirHandle) IsDir(name *string) bool {
	en, ok := dh.NameToEntry[*name]
	if !ok {
		return false
	}

	return en.Nlink == 2
}
