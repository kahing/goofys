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
	"bytes"
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

type Inode struct {
	Id fuseops.InodeID
	Name *string
	FullName *string
	Attributes *fuseops.InodeAttributes

	mu sync.Mutex // everything below is protected by mu
	handles map[*DirHandle]bool // value is ignored
	refcnt uint64
}


func NewInode(name *string, fullName *string) (inode *Inode) {
	inode = &Inode{ Name: name, FullName: fullName }
	inode.handles = make(map[*DirHandle]bool)
	inode.refcnt = 1
	return
}

func (inode *Inode) Ref() {
	inode.mu.Lock()
	defer inode.mu.Unlock()
	inode.refcnt++
}

type DirHandle struct {
	inode *Inode

	mu sync.Mutex // everything below is protected by mu
	Entries []fuseutil.Dirent
	NameToEntry map[string]fuseops.InodeAttributes // XXX use a smaller struct
	Marker *string
	BaseOffset int
}

func NewDirHandle(inode *Inode) (dh *DirHandle) {
	dh = &DirHandle{ inode: inode }
	dh.NameToEntry = make(map[string]fuseops.InodeAttributes)
	return
}

func (dh *DirHandle) IsDir(name *string) bool {
	en, ok := dh.NameToEntry[*name]
	if !ok {
		return false
	}

	return en.Nlink == 2
}

type FileHandle struct {
	inode *Inode

	Dirty bool
	// TODO replace with something else when we do MPU for flushing
	Buf []byte
}

func NewFileHandle(in *Inode) *FileHandle {
	return &FileHandle{ inode: in, Buf: make([]byte, 0, 4096) };
}

func (inode *Inode) logFuse(op string, args ...interface{}) {
	log.Printf("%v: [%v] %v", op, *inode.FullName, args)
}

// LOCKS_REQUIRED(parent.mu)
func (parent *Inode) lookupFromDirHandles(name *string) (inode *Inode) {
	parent.mu.Lock()
	defer parent.mu.Unlock()

	defer func() {
		if inode != nil {
			inode.Ref()
		}
	}()

	for dh := range parent.handles {
		attr, ok := dh.NameToEntry[*name]
		if ok {
			inode = NewInode(name, parent.getChildName(name))
			inode.Attributes = &attr
			return
		}
	}

	return
}

func (parent *Inode) LookUp(fs *Goofys, name *string) (inode *Inode, err error) {
	parent.logFuse("Inode.LookUp", *name)

	inode = parent.lookupFromDirHandles(name)
	if inode != nil {
		return
	}

	inode, err = fs.LookUpInodeMaybeDir(name, parent.getChildName(name))
	if err != nil {
		return nil, err
	}

	return
}

func (parent *Inode) getChildName(name *string) *string {
	if parent.Id == fuseops.RootInodeID {
		return name
	} else {
		fullName := fmt.Sprintf("%v/%v", *parent.FullName, *name)
		return &fullName
	}
}

func (inode *Inode) DeRef(n uint64) (stale bool) {
	inode.logFuse("ForgetInode", n)

	inode.mu.Lock()
	defer inode.mu.Unlock()

	if inode.refcnt < n {
		panic(fmt.Sprintf("deref %v from %v", n, inode.refcnt))
	}

	inode.refcnt -= n

	return inode.refcnt == 0
}

func (parent *Inode) Unlink(fs *Goofys, name *string) (err error) {
	fullName := parent.getChildName(name)

	params := &s3.DeleteObjectInput{
		Bucket:       &fs.bucket,
		Key:          fullName,
	}

	resp, err := fs.s3.DeleteObject(params)
	if err != nil {
		return mapAwsError(err)
	}

	log.Println(resp)
	return
}

func (parent *Inode) Create(
	fs *Goofys,
	name *string) (inode *Inode, fh *FileHandle) {

	parent.logFuse("Inode.Create", *name)

	parent.mu.Lock()
	defer parent.mu.Unlock()

	fullName := parent.getChildName(name)

	now := time.Now()
	inode = NewInode(name, fullName)
	inode.Attributes = &fuseops.InodeAttributes{
		Size: 0,
		Nlink: 1,
		Mode: 0644,
		Atime: now,
		Mtime: now,
		Ctime: now,
		Crtime: now,
		Uid:  fs.uid,
		Gid:  fs.gid,
		Blocks: 0,
		BlockSize: 512,
	}

	fh = NewFileHandle(inode)
	fh.Dirty = true

	return
}

func (inode *Inode) GetAttributes(fs *Goofys) (*fuseops.InodeAttributes, error) {
	// XXX refresh attributes
	inode.logFuse("GetAttributes")
	return inode.Attributes, nil
}

func (inode *Inode) OpenFile(fs *Goofys) *FileHandle {
	return NewFileHandle(inode)
}

func (fh *FileHandle) WriteFile(fs *Goofys, offset int64, data []byte) (err error) {
	fh.inode.logFuse("WriteFile", offset, len(data))

	const MaxInt = int(^uint(0) >> 1)

	if (offset + int64(len(data)) > int64(MaxInt)) {
		return fuse.EINVAL
	}

	last := int(offset) + len(data)
	if last > cap(fh.Buf) {
		//log.Printf(">> WriteFile: enlarging buffer: %v -> %v", cap(fh.Buf), last * 2)
		tmp := make([]byte, len(data), last * 2)
		copy(tmp, fh.Buf)
		fh.Buf = tmp
	}

	if last > len(fh.Buf) {
		fh.Buf = fh.Buf[0 : last]
		fh.inode.Attributes.Size = uint64(last)
	}

	copy(fh.Buf[offset : last], data)
	fh.Dirty = true

	return
}

func (fh *FileHandle) ReadFile(fs *Goofys, offset int64, buf []byte) (bytesRead int, err error) {
	end := offset + int64(len(buf)) - 1
	bytes := fmt.Sprintf("bytes=%v-%v", offset, end)
	toRead := len(buf)

	fh.inode.logFuse("ReadFile", bytes)

	params := &s3.GetObjectInput{
		Bucket:                     &fs.bucket,
		Key:                        fh.inode.FullName,
		Range:                      &bytes,
	}

	resp, err := fs.s3.GetObject(params)
	if err != nil {
		return 0, mapAwsError(err)
	}

	toRead = int(*resp.ContentLength)

	for toRead > 0 {
		buf := buf[bytesRead : bytesRead + int(toRead)]

		nread, err := resp.Body.Read(buf)
		bytesRead += nread
		toRead -= nread

		if err != nil {
			if err != io.EOF {
				return bytesRead, err
			} else {
				break;
			}
		}
	}

	err = nil
	return
}

func (fh *FileHandle) FlushFile(fs *Goofys) (err error) {
	fh.inode.logFuse("FlushFile")

	// XXX lock the file handle
	if fh.Dirty {
		params := &s3.PutObjectInput{
			Bucket: &fs.bucket,
			Key: fh.inode.FullName,
			Body: bytes.NewReader(fh.Buf),
		}

		resp, err := fs.s3.PutObject(params)

		log.Println(resp)

		if err == nil {
			fh.Dirty = false
		} else {
			return mapAwsError(err)
		}
	}

	return
}

func (inode *Inode) OpenDir() (dh *DirHandle) {
	inode.logFuse("OpenDir")

	dh = NewDirHandle(inode)

	inode.mu.Lock()
	defer inode.mu.Unlock()

	inode.handles[dh] = true

	return
}

// Dirents, sorted by name.
type sortedDirents []fuseutil.Dirent

func (p sortedDirents) Len() int           { return len(p) }
func (p sortedDirents) Less(i, j int) bool { return p[i].Name < p[j].Name }
func (p sortedDirents) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func makeDirEntry(name *string, t fuseutil.DirentType) fuseutil.Dirent {
	return fuseutil.Dirent{ Name: *name, Type: t, Inode: fuseops.RootInodeID + 1 }
}

func (dh *DirHandle) ReadDir(fs *Goofys, offset fuseops.DirOffset) (*fuseutil.Dirent, error) {
	// If the request is for offset zero, we assume that either this is the first
	// call or rewinddir has been called. Reset state.
	if offset == 0 {
		dh.Entries = nil
	}

	i := int(offset) - dh.BaseOffset
	if i < 0 {
		panic(fmt.Sprintf("invalid offset %v, base=%v", offset, dh.BaseOffset))
	}

	if i >= len(dh.Entries) {
		if dh.Marker != nil {
			dh.Entries = nil
			dh.BaseOffset += i
			i = 0
		}
	}

	if dh.BaseOffset > 5000 {
		// XXX prevent infinite loop, raise the limit later
		panic("too many results")
	}

	if dh.Entries == nil {
		prefix := *dh.inode.FullName
		if len(prefix) != 0 {
			prefix += "/"
		}

		params := &s3.ListObjectsInput{
			Bucket:       &fs.bucket,
			Delimiter:    aws.String("/"),
			Marker:       dh.Marker,
			Prefix:       &prefix,
			//MaxKeys:      aws.Int64(3),
		}

		resp, err := fs.s3.ListObjects(params)
		if err != nil {
			return nil, mapAwsError(err)
		}

		log.Println(resp)

		dh.Entries = make([]fuseutil.Dirent, 0, len(resp.CommonPrefixes) + len(resp.Contents))
		
		for _, dir := range resp.CommonPrefixes {
			// strip trailing /
			dirName := (*dir.Prefix)[0:len(*dir.Prefix)-1]
			// strip previous prefix
			dirName = dirName[len(*params.Prefix):]
			dh.Entries = append(dh.Entries, makeDirEntry(&dirName, fuseutil.DT_Directory))
			dh.NameToEntry[dirName] = fs.rootAttrs
		}

		for _, obj := range resp.Contents {
			baseName := (*obj.Key)[len(prefix):]
			if len(baseName) == 0 {
				// this is a directory blob
				continue
			}
			dh.Entries = append(dh.Entries, makeDirEntry(&baseName, fuseutil.DT_File))
			dh.NameToEntry[baseName] = fuseops.InodeAttributes{
				Size: uint64(*obj.Size),
				Nlink: 1,
				Mode: 0644,
				Atime: *obj.LastModified,
				Mtime: *obj.LastModified,
				Ctime: *obj.LastModified,
				Crtime: *obj.LastModified,
				Uid:  fs.uid,
				Gid:  fs.gid,
				Blocks: uint64(*obj.Size) / 512,
				BlockSize: 512,
			}
		}

		sort.Sort(sortedDirents(dh.Entries))

		// Fix up offset fields.
		for i := 0; i < len(dh.Entries); i++ {
			en := &dh.Entries[i]
			en.Offset = fuseops.DirOffset(i + dh.BaseOffset) + 1
		}


		if *resp.IsTruncated {
			dh.Marker = resp.NextMarker
		} else {
			dh.Marker = nil
		}
	}

	if i == len(dh.Entries) {
		// we've reached the end
		return nil, nil
	} else if i > len(dh.Entries) {
		return nil, fuse.EINVAL
	}

	return &dh.Entries[i], nil
}

func (dh *DirHandle) CloseDir() error {
	inode := dh.inode

	inode.mu.Lock()
	defer inode.mu.Unlock()
	delete(inode.handles, dh)

	return nil
}
