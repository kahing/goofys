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
	"github.com/jacobsa/fuse/fuseutil"

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
	AttrTime   time.Time

	mu sync.Mutex // everything below is protected by mu

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

// Recursively resets the DirTime for child directories.
// ACQUIRES_LOCK(inode.mu)
func (inode *Inode) resetDirTimeRec() {
	inode.mu.Lock()
	if inode.dir == nil {
		inode.mu.Unlock()
		return
	}
	inode.dir.DirTime = time.Time{}
	// Make a copy of the child nodes before giving up the lock.
	// This protects us from any addition/removal of child nodes
	// under this node.
	children := make([]*Inode, len(inode.dir.Children))
	copy(children, inode.dir.Children)
	inode.mu.Unlock()
	for _, child := range children {
		child.resetDirTimeRec()
	}
}

// ResetForUnmount resets the Inode as part of unmounting a storage backend
// mounted at the given inode.
// ACQUIRES_LOCK(inode.mu)
func (inode *Inode) ResetForUnmount() {
	if inode.dir == nil {
		panic(fmt.Sprintf("ResetForUnmount called on a non-directory. name:%v",
			inode.Name))
	}

	inode.mu.Lock()
	// First reset the cloud info for this directory. After that, any read and
	// write operations under this directory will not know about this cloud.
	inode.dir.cloud = nil
	inode.dir.mountPrefix = ""

	// Clear metadata.
	// Set the metadata values to nil instead of deleting them so that
	// we know to fetch them again next time instead of thinking there's
	// no metadata
	inode.userMetadata = nil
	inode.s3Metadata = nil
	inode.Attributes = InodeAttributes{}
	inode.Invalid, inode.ImplicitDir = false, false
	inode.mu.Unlock()
	// Reset DirTime for recursively for this node and all its child nodes.
	// Note: resetDirTimeRec should be called without holding the lock.
	inode.resetDirTimeRec()

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

func (parent *Inode) findPath(path string) (inode *Inode) {
	dir := parent

	for dir != nil {
		if !dir.isDir() {
			return nil
		}

		idx := strings.Index(path, "/")
		if idx == -1 {
			return dir.findChild(path)
		}
		dirName := path[0:idx]
		path = path[idx+1:]

		dir = dir.findChild(dirName)
	}

	return nil
}

func (parent *Inode) findChild(name string) (inode *Inode) {
	parent.mu.Lock()
	defer parent.mu.Unlock()

	inode = parent.findChildUnlockedFull(name)
	return
}

func (parent *Inode) findInodeFunc(name string, isDir bool) func(i int) bool {
	// sort dirs first, then by name
	return func(i int) bool {
		if parent.dir.Children[i].isDir() != isDir {
			return isDir
		}
		return (*parent.dir.Children[i].Name) >= name
	}
}

func (parent *Inode) findChildUnlockedFull(name string) (inode *Inode) {
	inode = parent.findChildUnlocked(name, false)
	if inode == nil {
		inode = parent.findChildUnlocked(name, true)
	}
	return
}

func (parent *Inode) findChildUnlocked(name string, isDir bool) (inode *Inode) {
	l := len(parent.dir.Children)
	if l == 0 {
		return
	}
	i := sort.Search(l, parent.findInodeFunc(name, isDir))
	if i < l {
		// found
		if *parent.dir.Children[i].Name == name {
			inode = parent.dir.Children[i]
		}
	}
	return
}

func (parent *Inode) findChildIdxUnlocked(name string) int {
	l := len(parent.dir.Children)
	if l == 0 {
		return -1
	}
	i := sort.Search(l, parent.findInodeFunc(name, true))
	if i < l {
		// found
		if *parent.dir.Children[i].Name == name {
			return i
		}
	}
	return -1
}

func (parent *Inode) removeChildUnlocked(inode *Inode) {
	l := len(parent.dir.Children)
	if l == 0 {
		return
	}
	i := sort.Search(l, parent.findInodeFunc(*inode.Name, inode.isDir()))
	if i >= l || *parent.dir.Children[i].Name != *inode.Name {
		panic(fmt.Sprintf("%v.removeName(%v) but child not found: %v",
			*parent.FullName(), *inode.Name, i))
	}

	copy(parent.dir.Children[i:], parent.dir.Children[i+1:])
	parent.dir.Children[l-1] = nil
	parent.dir.Children = parent.dir.Children[:l-1]

	if cap(parent.dir.Children)-len(parent.dir.Children) > 20 {
		tmp := make([]*Inode, len(parent.dir.Children))
		copy(tmp, parent.dir.Children)
		parent.dir.Children = tmp
	}
}

func (parent *Inode) removeChild(inode *Inode) {
	parent.mu.Lock()
	defer parent.mu.Unlock()

	parent.removeChildUnlocked(inode)
	return
}

func (parent *Inode) insertChild(inode *Inode) {
	parent.mu.Lock()
	defer parent.mu.Unlock()

	parent.insertChildUnlocked(inode)
}

func (parent *Inode) insertChildUnlocked(inode *Inode) {
	l := len(parent.dir.Children)
	if l == 0 {
		parent.dir.Children = []*Inode{inode}
		return
	}

	i := sort.Search(l, parent.findInodeFunc(*inode.Name, inode.isDir()))
	if i == l {
		// not found = new value is the biggest
		parent.dir.Children = append(parent.dir.Children, inode)
	} else {
		if *parent.dir.Children[i].Name == *inode.Name {
			panic(fmt.Sprintf("double insert of %v", parent.getChildName(*inode.Name)))
		}

		parent.dir.Children = append(parent.dir.Children, nil)
		copy(parent.dir.Children[i+1:], parent.dir.Children[i:])
		parent.dir.Children[i] = inode
	}
}

func (parent *Inode) LookUp(name string) (inode *Inode, err error) {
	parent.logFuse("Inode.LookUp", name)

	inode, err = parent.LookUpInodeMaybeDir(name, parent.getChildName(name))
	if err != nil {
		return nil, err
	}

	return
}

func (parent *Inode) getChildName(name string) string {
	if parent.Id == fuseops.RootInodeID {
		return name
	} else {
		return fmt.Sprintf("%v/%v", *parent.FullName(), name)
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

func (parent *Inode) Unlink(name string) (err error) {
	parent.logFuse("Unlink", name)

	cloud, key := parent.cloud()
	key = appendChildName(key, name)

	_, err = cloud.DeleteBlob(&DeleteBlobInput{
		Key: key,
	})
	if err == fuse.ENOENT {
		// this might have been deleted out of band
		err = nil
	}
	if err != nil {
		return
	}

	parent.mu.Lock()
	defer parent.mu.Unlock()

	inode := parent.findChildUnlocked(name, false)
	if inode != nil {
		parent.removeChildUnlocked(inode)
		inode.Parent = nil
	}

	return
}

func (parent *Inode) Create(
	name string, metadata fuseops.OpMetadata) (inode *Inode, fh *FileHandle) {

	parent.logFuse("Create", name)

	fs := parent.fs

	parent.mu.Lock()
	defer parent.mu.Unlock()

	now := time.Now()
	inode = NewInode(fs, parent, &name)
	inode.Attributes = InodeAttributes{
		Size:  0,
		Mtime: now,
	}

	fh = NewFileHandle(inode, metadata)
	fh.poolHandle = fs.bufferPool
	fh.dirty = true
	inode.fileHandles = 1

	parent.touch()

	return
}

func (parent *Inode) MkDir(
	name string) (inode *Inode, err error) {

	parent.logFuse("MkDir", name)

	fs := parent.fs

	cloud, key := parent.cloud()
	key = appendChildName(key, name)
	if !cloud.Capabilities().DirBlob {
		key += "/"
	}
	params := &PutBlobInput{
		Key:     key,
		Body:    nil,
		DirBlob: true,
	}

	_, err = cloud.PutBlob(params)
	if err != nil {
		return
	}

	parent.mu.Lock()
	defer parent.mu.Unlock()

	inode = NewInode(fs, parent, &name)
	inode.ToDir()
	inode.touch()
	if parent.Attributes.Mtime.Before(inode.Attributes.Mtime) {
		parent.Attributes.Mtime = inode.Attributes.Mtime
	}

	return
}

func appendChildName(parent, child string) string {
	if len(parent) != 0 {
		parent += "/"
	}
	return parent + child
}

func (parent *Inode) isEmptyDir(fs *Goofys, name string) (isDir bool, err error) {
	cloud, key := parent.cloud()
	key = appendChildName(key, name) + "/"

	params := &ListBlobsInput{
		Delimiter: aws.String("/"),
		MaxKeys:   PUInt32(2),
		Prefix:    &key,
	}

	resp, err := cloud.ListBlobs(params)
	if err != nil {
		return false, mapAwsError(err)
	}

	if len(resp.Prefixes) > 0 || len(resp.Items) > 1 {
		err = fuse.ENOTEMPTY
		isDir = true
		return
	}

	if len(resp.Items) == 1 {
		isDir = true

		if *resp.Items[0].Key != key {
			err = fuse.ENOTEMPTY
		}
	}

	return
}

func (parent *Inode) RmDir(name string) (err error) {
	parent.logFuse("Rmdir", name)

	isDir, err := parent.isEmptyDir(parent.fs, name)
	if err != nil {
		return
	}
	// if this was an implicit dir, isEmptyDir would have returned
	// isDir = false
	if isDir {
		cloud, key := parent.cloud()
		key = appendChildName(key, name) + "/"

		params := DeleteBlobInput{
			Key: key,
		}

		_, err = cloud.DeleteBlob(&params)
		if err != nil {
			return
		}
	}

	// we know this entry is gone
	parent.mu.Lock()
	defer parent.mu.Unlock()

	inode := parent.findChildUnlocked(name, true)
	if inode != nil {
		parent.removeChildUnlocked(inode)
		inode.Parent = nil
	}

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

// semantic of rename:
// rename("any", "not_exists") = ok
// rename("file1", "file2") = ok
// rename("empty_dir1", "empty_dir2") = ok
// rename("nonempty_dir1", "empty_dir2") = ok
// rename("nonempty_dir1", "nonempty_dir2") = ENOTEMPTY
// rename("file", "dir") = EISDIR
// rename("dir", "file") = ENOTDIR
func (parent *Inode) Rename(from string, newParent *Inode, to string) (err error) {
	parent.logFuse("Rename", from, newParent.getChildName(to))

	fromCloud, fromPath := parent.cloud()
	toCloud, toPath := newParent.cloud()
	if fromCloud != toCloud {
		// cannot rename across cloud backend
		err = fuse.EINVAL
		return
	}

	fromFullName := appendChildName(fromPath, from)
	fs := parent.fs

	var size *uint64
	var fromIsDir bool
	var toIsDir bool
	var renameChildren bool

	fromIsDir, err = parent.isEmptyDir(fs, from)
	if err != nil {
		if err == fuse.ENOTEMPTY {
			renameChildren = true
		} else {
			return
		}
	}

	toFullName := appendChildName(toPath, to)

	toIsDir, err = parent.isEmptyDir(fs, to)
	if err != nil {
		return
	}

	if fromIsDir && !toIsDir {
		_, err = fromCloud.HeadBlob(&HeadBlobInput{
			Key: toFullName,
		})
		if err == nil {
			return fuse.ENOTDIR
		} else {
			err = mapAwsError(err)
			if err != fuse.ENOENT {
				return
			}
		}
	} else if !fromIsDir && toIsDir {
		return syscall.EISDIR
	}

	if fromIsDir {
		fromFullName += "/"
		toFullName += "/"
		size = PUInt64(0)
	}

	if renameChildren && !fromCloud.Capabilities().DirBlob {
		err = parent.renameChildren(fromCloud, fromFullName,
			newParent, toFullName)
		if err != nil {
			return
		}
	} else {
		err = parent.renameObject(fs, size, fromFullName, toFullName)
	}
	return
}

func (parent *Inode) renameObject(fs *Goofys, size *uint64, fromFullName string, toFullName string) (err error) {
	cloud, _ := parent.cloud()

	_, err = cloud.RenameBlob(&RenameBlobInput{
		Source:      fromFullName,
		Destination: toFullName,
	})
	if err == nil || err != syscall.ENOTSUP {
		return
	}

	_, err = cloud.CopyBlob(&CopyBlobInput{
		Source:      fromFullName,
		Destination: toFullName,
		Size:        size,
	})
	if err != nil {
		return
	}

	_, err = cloud.DeleteBlob(&DeleteBlobInput{
		Key: fromFullName,
	})
	if err != nil {
		return
	}
	s3Log.Debugf("Deleted %v", fromFullName)

	return
}

// if I had seen a/ and a/b, and now I get a/c, that means a/b is
// done, but not a/
func (parent *Inode) isParentOf(inode *Inode) bool {
	return inode.Parent != nil && (parent == inode.Parent || parent.isParentOf(inode.Parent))
}

func sealPastDirs(dirs map[*Inode]bool, d *Inode) {
	for p, sealed := range dirs {
		if p != d && !sealed && !p.isParentOf(d) {
			dirs[p] = true
		}
	}
	// I just read something in d, obviously it's not done yet
	dirs[d] = false
}

// LOCKS_REQUIRED(fs.mu)
// LOCKS_REQUIRED(parent.mu)
// LOCKS_REQUIRED(parent.fs.mu)
func (parent *Inode) insertSubTree(path string, obj *BlobItemOutput, dirs map[*Inode]bool) {
	fs := parent.fs
	slash := strings.Index(path, "/")
	if slash == -1 {
		inode := parent.findChildUnlockedFull(path)
		if inode == nil {
			inode = NewInode(fs, parent, &path)
			inode.refcnt = 0
			fs.insertInode(parent, inode)
			inode.SetFromBlobItem(obj)
		} else {
			// our locking order is most specific lock
			// first, ie: lock a/b before a/. But here we
			// already have a/ and also global lock. For
			// new inode we don't care about that
			// violation because no one else will take
			// that lock anyway
			fs.mu.Unlock()
			parent.mu.Unlock()
			inode.SetFromBlobItem(obj)
			parent.mu.Lock()
			fs.mu.Lock()
		}
		sealPastDirs(dirs, parent)
	} else {
		dir := path[:slash]
		path = path[slash+1:]

		if len(path) == 0 {
			inode := parent.findChildUnlockedFull(dir)
			if inode == nil {
				inode = NewInode(fs, parent, &dir)
				inode.ToDir()
				inode.refcnt = 0
				fs.insertInode(parent, inode)
				inode.SetFromBlobItem(obj)
			} else if !inode.isDir() {
				inode.ToDir()
				fs.addDotAndDotDot(inode)
			} else {
				fs.mu.Unlock()
				parent.mu.Unlock()
				inode.SetFromBlobItem(obj)
				parent.mu.Lock()
				fs.mu.Lock()
			}
			sealPastDirs(dirs, inode)
		} else {
			// ensure that the potentially implicit dir is added
			inode := parent.findChildUnlockedFull(dir)
			if inode == nil {
				inode = NewInode(fs, parent, &dir)
				inode.ToDir()
				inode.refcnt = 0
				fs.insertInode(parent, inode)
			} else if !inode.isDir() {
				inode.ToDir()
				fs.addDotAndDotDot(inode)
			}

			// mark this dir but don't seal anything else
			// until we get to the leaf
			dirs[inode] = false

			fs.mu.Unlock()
			parent.mu.Unlock()
			inode.mu.Lock()
			fs.mu.Lock()
			inode.insertSubTree(path, obj, dirs)
			inode.mu.Unlock()
			fs.mu.Unlock()
			parent.mu.Lock()
			fs.mu.Lock()
		}
	}
}

func (parent *Inode) findChildMaxTime() time.Time {
	maxTime := parent.Attributes.Mtime

	for i, c := range parent.dir.Children {
		if i < 2 {
			// skip . and ..
			continue
		}
		if c.Attributes.Mtime.After(maxTime) {
			maxTime = c.Attributes.Mtime
		}
	}

	return maxTime
}

func (parent *Inode) readDirFromCache(offset fuseops.DirOffset) (en *DirHandleEntry, ok bool) {
	parent.mu.Lock()
	defer parent.mu.Unlock()

	if parent.dir == nil {
		panic(*parent.FullName())
	}
	if !expired(parent.dir.DirTime, parent.fs.flags.TypeCacheTTL) {
		ok = true

		if int(offset) >= len(parent.dir.Children) {
			return
		}
		child := parent.dir.Children[offset]

		en = &DirHandleEntry{
			Name:   *child.Name,
			Inode:  child.Id,
			Offset: offset + 1,
		}
		if child.isDir() {
			en.Type = fuseutil.DT_Directory
		} else {
			en.Type = fuseutil.DT_File
		}

	}
	return
}

func (parent *Inode) LookUpInodeNotDir(name string, c chan HeadBlobOutput, errc chan error) {
	cloud, key := parent.cloud()
	key = appendChildName(key, name)
	params := &HeadBlobInput{Key: key}
	resp, err := cloud.HeadBlob(params)
	if err != nil {
		errc <- mapAwsError(err)
		return
	}

	s3Log.Debug(resp)
	c <- *resp
}

func (parent *Inode) LookUpInodeDir(name string, c chan ListBlobsOutput, errc chan error) {
	cloud, key := parent.cloud()
	key = appendChildName(key, name) + "/"

	params := &ListBlobsInput{
		Delimiter: aws.String("/"),
		MaxKeys:   PUInt32(1),
		Prefix:    &key,
	}

	resp, err := cloud.ListBlobs(params)
	if err != nil {
		errc <- err
		return
	}

	s3Log.Debug(resp)
	c <- *resp
}

// returned inode has nil Id
func (parent *Inode) LookUpInodeMaybeDir(name string, fullName string) (inode *Inode, err error) {
	errObjectChan := make(chan error, 1)
	objectChan := make(chan HeadBlobOutput, 2)
	errDirBlobChan := make(chan error, 1)
	var errDirChan chan error
	var dirChan chan ListBlobsOutput

	checking := 3
	var checkErr [3]error

	cloud, _ := parent.cloud()
	if cloud == nil {
		panic("s3 disabled")
	}

	go parent.LookUpInodeNotDir(name, objectChan, errObjectChan)
	if !cloud.Capabilities().DirBlob && !parent.fs.flags.Cheap {
		go parent.LookUpInodeNotDir(name+"/", objectChan, errDirBlobChan)
		if !parent.fs.flags.ExplicitDir {
			errDirChan = make(chan error, 1)
			dirChan = make(chan ListBlobsOutput, 1)
			go parent.LookUpInodeDir(name, dirChan, errDirChan)
		}
	}

	for {
		select {
		case resp := <-objectChan:
			err = nil
			inode = NewInode(parent.fs, parent, &name)
			if !resp.IsDirBlob {
				// XXX/TODO if both object and object/ exists, return dir
				inode.Attributes = InodeAttributes{
					Size:  resp.Size,
					Mtime: *resp.LastModified,
				}

				// don't want to point to the attribute because that
				// can get updated
				size := inode.Attributes.Size
				inode.KnownSize = &size

			} else {
				inode.ToDir()
				if resp.LastModified != nil {
					inode.Attributes.Mtime = *resp.LastModified
				}
			}
			inode.fillXattrFromHead(&resp)
			return
		case err = <-errObjectChan:
			checking--
			checkErr[0] = err
			s3Log.Debugf("HEAD %v = %v", fullName, err)
		case resp := <-dirChan:
			err = nil
			if len(resp.Prefixes) != 0 || len(resp.Items) != 0 {
				inode = NewInode(parent.fs, parent, &name)
				inode.ToDir()
				if len(resp.Items) != 0 && *resp.Items[0].Key == name+"/" {
					// it's actually a dir blob
					entry := resp.Items[0]
					if entry.ETag != nil {
						inode.s3Metadata["etag"] = []byte(*entry.ETag)
					}
					if entry.StorageClass != nil {
						inode.s3Metadata["storage-class"] = []byte(*entry.StorageClass)
					}

				}
				// if cheap is not on, the dir blob
				// could exist but this returned first
				if inode.fs.flags.Cheap {
					inode.ImplicitDir = true
				}
				return
			} else {
				checkErr[2] = fuse.ENOENT
				checking--
			}
		case err = <-errDirChan:
			checking--
			checkErr[2] = err
			s3Log.Debugf("LIST %v/ = %v", fullName, err)
		case err = <-errDirBlobChan:
			checking--
			checkErr[1] = err
			s3Log.Debugf("HEAD %v/ = %v", fullName, err)
		}

		if cloud.Capabilities().DirBlob {
			return
		}

		switch checking {
		case 2:
			if parent.fs.flags.Cheap {
				go parent.LookUpInodeNotDir(name+"/", objectChan, errDirBlobChan)
			}
		case 1:
			if parent.fs.flags.ExplicitDir {
				checkErr[2] = fuse.ENOENT
				goto doneCase
			} else if parent.fs.flags.Cheap {
				errDirChan = make(chan error, 1)
				dirChan = make(chan ListBlobsOutput, 1)
				go parent.LookUpInodeDir(name, dirChan, errDirChan)
			}
			break
		doneCase:
			fallthrough
		case 0:
			for _, e := range checkErr {
				if e != fuse.ENOENT {
					err = e
					return
				}
			}

			err = fuse.ENOENT
			return
		}
	}
}
