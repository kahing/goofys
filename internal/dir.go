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
	lastOpenDir     *string
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

	if isS3 && parent != nil && inode.fs.flags.TypeCacheTTL != 0 {
		parent.mu.Lock()
		defer parent.mu.Unlock()

		num := len(parent.dir.Children)

		// we are opening the first child (after . and ..)
		if parent.dir.lastOpenDir == nil && num > 2 && *parent.dir.Children[2].Name == *inode.Name {
			if parent.dir.seqOpenDirScore < 255 {
				parent.dir.seqOpenDirScore += 1
			}
			parent.dir.lastOpenDirIdx = 2
			// 2.1) if I open a/a, a/'s score is now 2
			// ie: handle the depth first search case
			if parent.dir.seqOpenDirScore >= 2 {
				fuseLog.Debugf("%v in readdir mode", *parent.FullName())
			}
		} else if parent.dir.lastOpenDir != nil && parent.dir.lastOpenDirIdx+1 < num &&
			// we are reading the next one as expected
			*parent.dir.Children[parent.dir.lastOpenDirIdx+1].Name == *inode.Name &&
			// check that inode positions haven't moved
			*parent.dir.Children[parent.dir.lastOpenDirIdx].Name == *parent.dir.lastOpenDir {
			// 2.2) if I open b/, root's score is now 2
			// ie: handle the breath first search case
			if parent.dir.seqOpenDirScore < 255 {
				parent.dir.seqOpenDirScore++
			}
			parent.dir.lastOpenDirIdx += 1
			if parent.dir.seqOpenDirScore == 2 {
				fuseLog.Debugf("%v in readdir mode", *parent.FullName())
			}
		} else {
			parent.dir.seqOpenDirScore = 0
			parent.dir.lastOpenDirIdx = parent.findChildIdxUnlocked(*inode.Name)
			if parent.dir.lastOpenDirIdx == -1 {
				panic(fmt.Sprintf("%v is not under %v", *inode.Name, *parent.FullName()))
			}
		}

		parent.dir.lastOpenDir = inode.Name
		inode.mu.Lock()
		defer inode.mu.Unlock()

		if inode.dir.lastOpenDir == nil {
			// 1) if I open a/, root's score = 1 (a is the first dir),
			// so make a/'s count at 1 too
			inode.dir.seqOpenDirScore = parent.dir.seqOpenDirScore
			if inode.dir.seqOpenDirScore >= 2 {
				fuseLog.Debugf("%v in readdir mode", *inode.FullName())
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
	for dh.lastFromCloud == nil {
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

			if inode := parent.findChildUnlockedFull(dirName); inode != nil {
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

				inode := parent.findChildUnlockedFull(baseName)
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
