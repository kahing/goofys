// Copyright 2015 - 2017 Ka-Hing Cheung
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

package internal

import (
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"

	"github.com/sirupsen/logrus"
)

type InodeAttributes struct {
	Size  uint64
	Mtime time.Time
}

type Inode struct {
	Id         fuseops.InodeID
	Name       *string
	fs         *Goofys
	Attributes InodeAttributes
	KnownSize  *uint64
	// It is generally safe to read `AttrTime` without locking because if some other
	// operation is modifying `AttrTime`, in most cases the reader is okay with working with
	// stale data. But Time is a struct and modifying it is not atomic. However
	// in practice (until the year 2157) we should be okay because
	// - Almost all uses of AttrTime will be about comparisions (AttrTime < x, AttrTime > x)
	// - Time object will have Time::monotonic bit set (until the year 2157) => the time
	//   comparision just compares Time::ext field
	// Ref: https://github.com/golang/go/blob/e42ae65a8507/src/time/time.go#L12:L56
	AttrTime time.Time

	mu sync.Mutex // everything below is protected by mu

	// We are not very consistent about enforcing locks for `Parent` because, the
	// parent field very very rarely changes and it is generally fine to operate on
	// stale parent informaiton
	Parent *Inode

	dir *DirInodeData

	Invalid     bool
	ImplicitDir bool

	fileHandles uint32

	userMetadata map[string][]byte
	s3Metadata   map[string][]byte

	// the refcnt is an exception, it's protected by the global lock
	// Goofys.mu
	refcnt uint64
}

func NewInode(fs *Goofys, parent *Inode, name *string) (inode *Inode) {
	if strings.Index(*name, "/") != -1 {
		fuseLog.Errorf("%v is not a valid name", *name)
	}

	inode = &Inode{
		Name:       name,
		fs:         fs,
		AttrTime:   time.Now(),
		Parent:     parent,
		s3Metadata: make(map[string][]byte),
		refcnt:     1,
	}

	return
}

func (inode *Inode) SetFromBlobItem(item *BlobItemOutput) {
	inode.mu.Lock()
	defer inode.mu.Unlock()

	inode.Attributes.Size = item.Size
	size := item.Size
	inode.KnownSize = &size
	if item.LastModified != nil {
		inode.Attributes.Mtime = *item.LastModified
	} else {
		inode.Attributes.Mtime = inode.fs.rootAttrs.Mtime
	}
	if item.ETag != nil {
		inode.s3Metadata["etag"] = []byte(*item.ETag)
	} else {
		delete(inode.s3Metadata, "etag")
	}
	if item.StorageClass != nil {
		inode.s3Metadata["storage-class"] = []byte(*item.StorageClass)
	} else {
		delete(inode.s3Metadata, "storage-class")
	}
	now := time.Now()
	// don't want to update time if this inode is setup to never expire
	if inode.AttrTime.Before(now) {
		inode.AttrTime = now
	}
}

func (inode *Inode) cloud() (cloud StorageBackend, path string) {
	var prefix string
	var dir *Inode

	if inode.dir == nil {
		path = *inode.Name
		dir = inode.Parent
	} else {
		dir = inode
	}

	for p := dir; p != nil; p = p.Parent {
		if p.dir.cloud != nil {
			cloud = p.dir.cloud
			// the error backend produces a mount.err file
			// at the root and is not aware of prefix
			_, isErr := cloud.(StorageBackendInitError)
			if !isErr {
				// we call init here instead of
				// relying on the wrapper to call init
				// because we want to return the right
				// prefix
				if c, ok := cloud.(*StorageBackendInitWrapper); ok {
					err := c.Init("")
					isErr = err != nil
				}
			}

			if !isErr {
				prefix = p.dir.mountPrefix
			}
			break
		}

		if path == "" {
			path = *p.Name
		} else if p.Parent != nil {
			// don't prepend if I am already the root node
			path = *p.Name + "/" + path
		}
	}

	if path == "" {
		path = strings.TrimRight(prefix, "/")
	} else {
		path = prefix + path
	}
	return
}

func (inode *Inode) FullName() *string {
	if inode.Parent == nil {
		return inode.Name
	} else {
		s := inode.Parent.getChildName(*inode.Name)
		return &s
	}
}

func (inode *Inode) touch() {
	inode.Attributes.Mtime = time.Now()
}

func (inode *Inode) InflateAttributes() (attr fuseops.InodeAttributes) {
	mtime := inode.Attributes.Mtime
	if mtime.IsZero() {
		mtime = inode.fs.rootAttrs.Mtime
	}

	attr = fuseops.InodeAttributes{
		Size:   inode.Attributes.Size,
		Atime:  mtime,
		Mtime:  mtime,
		Ctime:  mtime,
		Crtime: mtime,
		Uid:    inode.fs.flags.Uid,
		Gid:    inode.fs.flags.Gid,
	}

	if inode.dir != nil {
		attr.Nlink = 2
		attr.Mode = inode.fs.flags.DirMode | os.ModeDir
	} else {
		attr.Nlink = 1
		attr.Mode = inode.fs.flags.FileMode
	}
	return
}

func (inode *Inode) logFuse(op string, args ...interface{}) {
	if fuseLog.Level >= logrus.DebugLevel {
		fuseLog.Debugln(op, inode.Id, *inode.FullName(), args)
	}
}

func (inode *Inode) errFuse(op string, args ...interface{}) {
	fuseLog.Errorln(op, inode.Id, *inode.FullName(), args)
}

func (inode *Inode) ToDir() {
	if inode.dir == nil {
		inode.Attributes = InodeAttributes{
			Size: 4096,
			// Mtime intentionally not initialized
		}
		inode.dir = &DirInodeData{}
		inode.KnownSize = &inode.fs.rootAttrs.Size
	}
}

// LOCKS_REQUIRED(fs.mu)
// XXX why did I put lock required? This used to return a resurrect bool
// which no long does anything, need to look into that to see if
// that was legacy
func (inode *Inode) Ref() {
	inode.logFuse("Ref", inode.refcnt)

	inode.refcnt++
	return
}

func (inode *Inode) DeRef(n uint64) (stale bool) {
	inode.logFuse("DeRef", n, inode.refcnt)

	if inode.refcnt < n {
		panic(fmt.Sprintf("deref %v from %v", n, inode.refcnt))
	}

	inode.refcnt -= n

	stale = (inode.refcnt == 0)
	return
}

func (inode *Inode) GetAttributes() (*fuseops.InodeAttributes, error) {
	// XXX refresh attributes
	inode.logFuse("GetAttributes")
	if inode.Invalid {
		return nil, fuse.ENOENT
	}
	attr := inode.InflateAttributes()
	return &attr, nil
}

func (inode *Inode) isDir() bool {
	return inode.dir != nil
}

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) fillXattrFromHead(resp *HeadBlobOutput) {
	inode.userMetadata = make(map[string][]byte)

	if resp.ETag != nil {
		inode.s3Metadata["etag"] = []byte(*resp.ETag)
	}
	if resp.StorageClass != nil {
		inode.s3Metadata["storage-class"] = []byte(*resp.StorageClass)
	} else {
		inode.s3Metadata["storage-class"] = []byte("STANDARD")
	}

	for k, v := range resp.Metadata {
		k = strings.ToLower(k)
		value, err := url.PathUnescape(*v)
		if err != nil {
			value = *v
		}
		inode.userMetadata[k] = []byte(value)
	}
}

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) fillXattr() (err error) {
	if !inode.ImplicitDir && inode.userMetadata == nil {

		fullName := *inode.FullName()
		if inode.isDir() {
			fullName += "/"
		}

		cloud, key := inode.cloud()
		params := &HeadBlobInput{Key: key}
		resp, err := cloud.HeadBlob(params)
		if err != nil {
			err = mapAwsError(err)
			if err == fuse.ENOENT {
				err = nil
				if inode.isDir() {
					inode.ImplicitDir = true
				}
			}
			return err
		} else {
			inode.fillXattrFromHead(resp)
		}
	}

	return
}

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) getXattrMap(name string, userOnly bool) (
	meta map[string][]byte, newName string, err error) {

	if strings.HasPrefix(name, "s3.") {
		if userOnly {
			return nil, "", syscall.EACCES
		}

		newName = name[3:]
		meta = inode.s3Metadata
	} else if strings.HasPrefix(name, "user.") {
		err = inode.fillXattr()
		if err != nil {
			return nil, "", err
		}

		newName = name[5:]
		meta = inode.userMetadata
	} else {
		if userOnly {
			return nil, "", syscall.EACCES
		} else {
			return nil, "", syscall.ENODATA
		}
	}

	if meta == nil {
		return nil, "", syscall.ENODATA
	}

	return
}

func convertMetadata(meta map[string][]byte) (metadata map[string]*string) {
	metadata = make(map[string]*string)
	for k, v := range meta {
		k = strings.ToLower(k)
		metadata[k] = aws.String(xattrEscape(v))
	}
	return
}

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) updateXattr() (err error) {
	cloud, key := inode.cloud()
	_, err = cloud.CopyBlob(&CopyBlobInput{
		Source:      key,
		Destination: key,
		Size:        &inode.Attributes.Size,
		ETag:        aws.String(string(inode.s3Metadata["etag"])),
		Metadata:    convertMetadata(inode.userMetadata),
	})
	return
}

func (inode *Inode) SetXattr(name string, value []byte, flags uint32) error {
	inode.logFuse("RemoveXattr", name)

	inode.mu.Lock()
	defer inode.mu.Unlock()

	meta, name, err := inode.getXattrMap(name, true)
	if err != nil {
		return err
	}

	if flags != 0x0 {
		_, ok := meta[name]
		if flags == 0x1 {
			if ok {
				return syscall.EEXIST
			}
		} else if flags == 0x2 {
			if !ok {
				return syscall.ENODATA
			}
		}
	}

	meta[name] = Dup(value)
	err = inode.updateXattr()
	return err
}

func (inode *Inode) RemoveXattr(name string) error {
	inode.logFuse("RemoveXattr", name)

	inode.mu.Lock()
	defer inode.mu.Unlock()

	meta, name, err := inode.getXattrMap(name, true)
	if err != nil {
		return err
	}

	if _, ok := meta[name]; ok {
		delete(meta, name)
		err = inode.updateXattr()
		return err
	} else {
		return syscall.ENODATA
	}
}

func (inode *Inode) GetXattr(name string) ([]byte, error) {
	inode.logFuse("GetXattr", name)

	inode.mu.Lock()
	defer inode.mu.Unlock()

	meta, name, err := inode.getXattrMap(name, false)
	if err != nil {
		return nil, err
	}

	value, ok := meta[name]
	if ok {
		return value, nil
	} else {
		return nil, syscall.ENODATA
	}
}

func (inode *Inode) ListXattr() ([]string, error) {
	inode.logFuse("ListXattr")

	inode.mu.Lock()
	defer inode.mu.Unlock()

	var xattrs []string

	err := inode.fillXattr()
	if err != nil {
		return nil, err
	}

	for k, _ := range inode.s3Metadata {
		xattrs = append(xattrs, "s3."+k)
	}

	for k, _ := range inode.userMetadata {
		xattrs = append(xattrs, "user."+k)
	}

	sort.Strings(xattrs)

	return xattrs, nil
}

func (inode *Inode) OpenFile(metadata fuseops.OpMetadata) (fh *FileHandle, err error) {
	inode.logFuse("OpenFile")

	inode.mu.Lock()
	defer inode.mu.Unlock()

	fh = NewFileHandle(inode, metadata)
	inode.fileHandles += 1
	return
}
