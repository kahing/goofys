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
	"fmt"
	"log"

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

type FileHandle struct {
	Name *string
	FullName *string
	Dirty bool
	// TODO replace with something else when we do MPU for flushing
	Buf []byte
}

func NewFileHandle(in *Inode) *FileHandle {
	return &FileHandle{ Name: in.Name, FullName: in.FullName, Buf: make([]byte, 0, 4096) };
}

func (parent *Inode) LookUp(fs *Goofys, name *string) (inode *Inode, err error) {
	var fullName string
	if parent.Id == fuseops.RootInodeID {
		fullName = *name
	} else {
		fullName = fmt.Sprintf("%v/%v", *parent.FullName, *name)
	}

	log.Printf("> LookUpInode: %v\n", fullName)

/*
	id, ok := fs.nameToID[fullName]
	if ok {
		defer fs.mu.Unlock()

		inode = fs.inodes[id]
		op.Entry.Child = inode.Id
		op.Entry.Attributes = inode.Attributes
		op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)

		log.Printf("< LookUpInode: %v from nameToID\n", fullName)
		return
	}
*/

	dh, ok := fs.nameToDir[*parent.Name]
	if ok {
		inode = &Inode{ Name: name, FullName: &fullName }

		if dh.IsDir(name) {
			log.Printf("< LookUpInode: %v dir\n", fullName)

			inode.Attributes = fs.rootAttrs
		} else {
			log.Printf("< LookUpInode: %v from dh.NameToEntry\n", fullName)

			inode.Attributes = dh.NameToEntry[*name]
		}
	} else {
		inode, err = fs.LookUpInodeMaybeDir(*name, fullName)
		if err != nil {
			return nil, err
		}
		log.Printf("< LookUpInode: %v from LookUpInodeMaybeDir\n", fullName)
	}

	return
}
