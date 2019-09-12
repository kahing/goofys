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
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

type DirInodeData struct {
	cloud       StorageBackend
	mountPrefix string

	// these 2 refer to readdir of the Children
	lastOpenDir     *DirInodeData
	lastOpenDirIdx  int
	seqOpenDirScore uint8
	DirTime         time.Time

	Children []*Inode
}

type DirHandleEntry struct {
	Name   string
	Inode  fuseops.InodeID
	Type   fuseutil.DirentType
	Offset fuseops.DirOffset
}

type DirHandle struct {
	inode *Inode

	mu sync.Mutex // everything below is protected by mu

	Marker        *string
	lastFromCloud *string
	done          bool
	// Time at which we started fetching child entries
	// from cloud for this handle.
	refreshStartTime time.Time
}

func NewDirHandle(inode *Inode) (dh *DirHandle) {
	dh = &DirHandle{inode: inode}
	return
}

func (inode *Inode) OpenDir() (dh *DirHandle) {
	inode.logFuse("OpenDir")

	parent := inode.Parent
	cloud, _ := inode.cloud()
	_, isS3 := cloud.(*S3Backend)
	dir := inode.dir
	if dir == nil {
		panic(fmt.Sprintf("%v is not a directory", inode.FullName()))
	}

	if isS3 && parent != nil && inode.fs.flags.TypeCacheTTL != 0 {
		parent.mu.Lock()
		defer parent.mu.Unlock()

		numChildren := len(parent.dir.Children)
		dirIdx := -1
		seqMode := false
		firstDir := false

		if parent.dir.lastOpenDir == nil {
			// check if we are opening the first child
			// (after . and ..)  cap the search to 1000
			// peers to bound the time. If the next dir is
			// more than 1000 away, slurping isn't going
			// to be helpful anyway
			for i := 2; i < MinInt(numChildren, 1000); i++ {
				c := parent.dir.Children[i]
				if c.isDir() {
					if *c.Name == *inode.Name {
						dirIdx = i
						seqMode = true
						firstDir = true
					}
					break
				}
			}
		} else {
			// check if we are reading the next one as expected
			for i := parent.dir.lastOpenDirIdx + 1; i < MinInt(numChildren, 1000); i++ {
				c := parent.dir.Children[i]
				if c.isDir() {
					if *c.Name == *inode.Name {
						dirIdx = i
						seqMode = true
					}
					break
				}
			}
		}

		if seqMode {
			if parent.dir.seqOpenDirScore < 255 {
				parent.dir.seqOpenDirScore++
			}
			if parent.dir.seqOpenDirScore == 2 {
				fuseLog.Debugf("%v in readdir mode", *parent.FullName())
			}
			parent.dir.lastOpenDir = dir
			parent.dir.lastOpenDirIdx = dirIdx
			if firstDir {
				// 1) if I open a/, root's score = 1
				// (a is the first dir), so make a/'s
				// count at 1 too this allows us to
				// propagate down the score for
				// depth-first search case
				wasSeqMode := dir.seqOpenDirScore >= 2
				dir.seqOpenDirScore = parent.dir.seqOpenDirScore
				if !wasSeqMode && dir.seqOpenDirScore >= 2 {
					fuseLog.Debugf("%v in readdir mode", *inode.FullName())
				}
			}
		} else {
			parent.dir.seqOpenDirScore = 0
			if dirIdx == -1 {
				dirIdx = parent.findChildIdxUnlocked(*inode.Name)
			}
			if dirIdx != -1 {
				parent.dir.lastOpenDir = dir
				parent.dir.lastOpenDirIdx = dirIdx
			}
		}
	}

	dh = NewDirHandle(inode)
	return
}

func (dh *DirHandle) listObjectsSlurp(prefix string) (resp *ListBlobsOutput, err error) {
	var marker *string
	reqPrefix := prefix
	inode := dh.inode

	cloud, key := inode.cloud()

	if dh.inode.Parent != nil {
		inode = dh.inode.Parent
		var parentCloud StorageBackend
		parentCloud, reqPrefix = inode.cloud()
		if parentCloud != cloud {
			err = fmt.Errorf("cannot slurp across cloud provider")
			return
		}

		if len(reqPrefix) != 0 {
			reqPrefix += "/"
		}
		marker = &key
		if len(*marker) != 0 {
			*marker += "/"
		}
	}

	params := &ListBlobsInput{
		Prefix:     &reqPrefix,
		StartAfter: marker,
	}

	resp, err = cloud.ListBlobs(params)
	if err != nil {
		s3Log.Errorf("ListObjects %v = %v", params, err)
		return
	}

	num := len(resp.Items)
	if num == 0 {
		return
	}

	inode.mu.Lock()
	inode.fs.mu.Lock()

	dirs := make(map[*Inode]bool)
	for _, obj := range resp.Items {
		baseName := (*obj.Key)[len(reqPrefix):]

		slash := strings.Index(baseName, "/")
		if slash != -1 {
			inode.insertSubTree(baseName, &obj, dirs)
		}
	}
	inode.fs.mu.Unlock()
	inode.mu.Unlock()

	for d, sealed := range dirs {
		if d == dh.inode {
			// never seal the current dir because that's
			// handled at upper layer
			continue
		}

		if sealed || !resp.IsTruncated {
			d.dir.DirTime = time.Now()
			d.Attributes.Mtime = d.findChildMaxTime()
		}
	}

	if resp.IsTruncated {
		obj := resp.Items[len(resp.Items)-1]
		// if we are done listing prefix, we are good
		if strings.HasPrefix(*obj.Key, prefix) {
			// if we are done with all the slashes, then we are good
			baseName := (*obj.Key)[len(prefix):]

			for _, c := range baseName {
				if c <= '/' {
					// if an entry is ex: a!b, then the
					// next entry could be a/foo, so we
					// are not done yet.
					resp = nil
					break
				}
			}
		}
	}

	// we only return this response if we are totally done with listing this dir
	if resp != nil {
		resp.IsTruncated = false
		resp.NextContinuationToken = nil
	}

	return
}

func (dh *DirHandle) listObjects(prefix string) (resp *ListBlobsOutput, err error) {
	errSlurpChan := make(chan error, 1)
	slurpChan := make(chan ListBlobsOutput, 1)
	errListChan := make(chan error, 1)
	listChan := make(chan ListBlobsOutput, 1)

	fs := dh.inode.fs

	// try to list without delimiter to see if we can slurp up
	// multiple directories
	parent := dh.inode.Parent

	if dh.Marker == nil &&
		fs.flags.TypeCacheTTL != 0 &&
		(parent != nil && parent.dir.seqOpenDirScore >= 2) {
		go func() {
			resp, err := dh.listObjectsSlurp(prefix)
			if err != nil {
				errSlurpChan <- err
			} else if resp != nil {
				slurpChan <- *resp
			} else {
				errSlurpChan <- fuse.EINVAL
			}
		}()
	} else {
		errSlurpChan <- fuse.EINVAL
	}

	listObjectsFlat := func() {
		params := &ListBlobsInput{
			Delimiter:         aws.String("/"),
			ContinuationToken: dh.Marker,
			Prefix:            &prefix,
		}

		cloud, _ := dh.inode.cloud()

		resp, err := cloud.ListBlobs(params)
		if err != nil {
			errListChan <- err
		} else {
			listChan <- *resp
		}
	}

	if !fs.flags.Cheap {
		// invoke the fallback in parallel if desired
		go listObjectsFlat()
	}

	// first see if we get anything from the slurp
	select {
	case resp := <-slurpChan:
		return &resp, nil
	case err = <-errSlurpChan:
	}

	if fs.flags.Cheap {
		listObjectsFlat()
	}

	// if we got an error (which may mean slurp is not applicable,
	// wait for regular list
	select {
	case resp := <-listChan:
		return &resp, nil
	case err = <-errListChan:
		return
	}
}

// LOCKS_REQUIRED(dh.mu)
// LOCKS_EXCLUDED(dh.inode.mu)
// LOCKS_EXCLUDED(dh.inode.fs)
func (dh *DirHandle) ReadDir(offset fuseops.DirOffset) (en *DirHandleEntry, err error) {
	en, ok := dh.inode.readDirFromCache(offset)
	if ok {
		return
	}

	parent := dh.inode
	fs := parent.fs

	// the dir expired, so we need to fetch from the cloud. there
	// maybe static directories that we want to keep, so cloud
	// listing should not overwrite them. here's what we do:
	//
	// 1. list from cloud and add them all to the tree, remember
	//    which one we added last
	//
	// 2. serve from cache
	//
	// 3. when we serve the entry we added last, signal that next
	//    time we need to list from cloud again with continuation
	//    token
	for dh.lastFromCloud == nil && !dh.done {
		if dh.Marker == nil {
			// Marker, lastFromCloud are nil => We just started
			// refreshing this directory info from cloud.
			dh.refreshStartTime = time.Now()
		}
		dh.mu.Unlock()

		var prefix string
		_, prefix = dh.inode.cloud()
		if len(prefix) != 0 {
			prefix += "/"
		}

		resp, err := dh.listObjects(prefix)
		if err != nil {
			dh.mu.Lock()
			return nil, err
		}

		s3Log.Debug(resp)
		dh.mu.Lock()
		parent.mu.Lock()
		fs.mu.Lock()

		// this is only returned for non-slurped responses
		for _, dir := range resp.Prefixes {
			// strip trailing /
			dirName := (*dir.Prefix)[0 : len(*dir.Prefix)-1]
			// strip previous prefix
			dirName = dirName[len(prefix):]
			if len(dirName) == 0 {
				continue
			}

			if inode := parent.findChildUnlocked(dirName); inode != nil {
				inode.AttrTime = time.Now()
			} else {
				inode := NewInode(fs, parent, &dirName)
				inode.ToDir()
				fs.insertInode(parent, inode)
				// these are fake dir entries, we will
				// realize the refcnt when lookup is
				// done
				inode.refcnt = 0
			}

			dh.lastFromCloud = &dirName
		}

		for _, obj := range resp.Items {
			if !strings.HasPrefix(*obj.Key, prefix) {
				// other slurped objects that we cached
				continue
			}

			baseName := (*obj.Key)[len(prefix):]

			slash := strings.Index(baseName, "/")
			if slash == -1 {
				if len(baseName) == 0 {
					// shouldn't happen
					continue
				}

				inode := parent.findChildUnlocked(baseName)
				if inode == nil {
					inode = NewInode(fs, parent, &baseName)
					// these are fake dir entries,
					// we will realize the refcnt
					// when lookup is done
					inode.refcnt = 0
					fs.insertInode(parent, inode)
				}
				inode.SetFromBlobItem(&obj)
			} else {
				// this is a slurped up object which
				// was already cached
				baseName = baseName[:slash]
			}

			if dh.lastFromCloud == nil ||
				strings.Compare(*dh.lastFromCloud, baseName) < 0 {
				dh.lastFromCloud = &baseName
			}
		}

		parent.mu.Unlock()
		fs.mu.Unlock()

		if resp.IsTruncated {
			dh.Marker = resp.NextContinuationToken
		} else {
			dh.Marker = nil
			dh.done = true
			break
		}
	}

	parent.mu.Lock()
	defer parent.mu.Unlock()

	// Find the first non-stale child inode with offset >= `offset`.
	var child *Inode
	for int(offset) < len(parent.dir.Children) {
		// Note on locking: See comments at Inode::AttrTime, Inode::Parent.
		childTmp := parent.dir.Children[offset]
		if childTmp.AttrTime.Before(dh.refreshStartTime) {
			// childTmp.AttrTime < dh.refreshStartTime => the child entry was not
			// updated from cloud by this dir Handle.
			// So this is a stale entry that should be removed.
			childTmp.Parent = nil
			parent.removeChildUnlocked(childTmp)
		} else {
			// Found a non-stale child inode.
			child = childTmp
			break
		}
	}

	if child == nil {
		// we've reached the end
		parent.dir.DirTime = time.Now()
		parent.Attributes.Mtime = parent.findChildMaxTime()
		return nil, nil
	}

	en = &DirHandleEntry{
		Name:   *child.Name,
		Inode:  child.Id,
		Offset: fuseops.DirOffset(offset) + 1,
	}
	if child.isDir() {
		en.Type = fuseutil.DT_Directory
	} else {
		en.Type = fuseutil.DT_File
	}

	if dh.lastFromCloud != nil && en.Name == *dh.lastFromCloud {
		dh.lastFromCloud = nil
	}
	return en, nil
}

func (dh *DirHandle) CloseDir() error {
	return nil
}

// prefix and newPrefix should include the trailing /
// return all the renamed objects
func (dir *Inode) renameChildren(cloud StorageBackend, prefix string,
	newParent *Inode, newPrefix string) (err error) {

	var copied []string
	var res *ListBlobsOutput

	for true {
		param := ListBlobsInput{
			Prefix: &prefix,
		}
		if res != nil {
			param.ContinuationToken = res.NextContinuationToken
		}

		res, err = cloud.ListBlobs(&param)
		if err != nil {
			return
		}

		if len(res.Items) == 0 {
			return
		}

		if copied == nil {
			copied = make([]string, 0, len(res.Items))
		}

		// after the server side copy, we want to delete all the files
		// using multi-delete, which is capped to 1000 on aws. If we
		// are going to make an arbitrary limit that sounds like a
		// good one (and we want to have an arbitrary limit because we
		// don't want to rename a million objects here)
		total := len(copied) + len(res.Items)
		if total > 1000 || total == 1000 && res.IsTruncated {
			return syscall.E2BIG
		}

		// say dir is "/a/dir" and it has "1", "2", "3", and we are
		// moving it to "/b/" items will be a/dir/1, a/dir/2, a/dir/3,
		// and we will copy them to b/1, b/2, b/3 respectively
		for _, i := range res.Items {
			key := (*i.Key)[len(prefix):]

			// TODO: coordinate with underlining copy and do this in parallel
			_, err = cloud.CopyBlob(&CopyBlobInput{
				Source:       *i.Key,
				Destination:  newPrefix + key,
				Size:         &i.Size,
				ETag:         i.ETag,
				StorageClass: i.StorageClass,
			})
			if err != nil {
				return err
			}

			copied = append(copied, *i.Key)
		}

		if !res.IsTruncated {
			break
		}
	}

	s3Log.Debugf("rename copied %v", copied)
	_, err = cloud.DeleteBlobs(&DeleteBlobsInput{Items: copied})
	return err
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

	inode = parent.findChildUnlocked(name)
	return
}

func (parent *Inode) findInodeFunc(name string) func(i int) bool {
	return func(i int) bool {
		return (*parent.dir.Children[i].Name) >= name
	}
}

func (parent *Inode) findChildUnlocked(name string) (inode *Inode) {
	l := len(parent.dir.Children)
	if l == 0 {
		return
	}
	i := sort.Search(l, parent.findInodeFunc(name))
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
	i := sort.Search(l, parent.findInodeFunc(name))
	if i < l && *parent.dir.Children[i].Name == name {
		return i
	}
	return -1
}

func (parent *Inode) removeChildUnlocked(inode *Inode) {
	l := len(parent.dir.Children)
	if l == 0 {
		return
	}
	i := sort.Search(l, parent.findInodeFunc(*inode.Name))
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

	i := sort.Search(l, parent.findInodeFunc(*inode.Name))
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

	inode := parent.findChildUnlocked(name)
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

	inode := parent.findChildUnlocked(name)
	if inode != nil {
		parent.removeChildUnlocked(inode)
		inode.Parent = nil
	}

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
		inode := parent.findChildUnlocked(path)
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
			inode := parent.findChildUnlocked(dir)
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
			inode := parent.findChildUnlocked(dir)
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
