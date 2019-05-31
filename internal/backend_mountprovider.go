// Copyright 2019 Databricks
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
	"syscall"
	"time"

	"github.com/jacobsa/fuse"
)

type Mount struct {
	name    string
	cloud   StorageBackend
	prefix  string
	mounted bool
}

type MountProvider interface {
	List() ([]*Mount, error)
	Lookup(name string) (*Mount, error)
}

type MountProviderBackend struct {
	MountProvider
	inode *Inode
}

func (m *MountProviderBackend) Init(key string) error {
	return nil
}

func (m *MountProviderBackend) Capabilities() *Capabilities {
	return &Capabilities{
		DirBlob: true,
	}
}

func (m *MountProviderBackend) mount(b *Mount) {
	if b.mounted {
		return
	}

	fs := m.inode.fs

	mp := m.inode
	name := strings.Trim(b.name, "/")

	for {
		idx := strings.Index(name, "/")
		if idx == -1 {
			break
		}
		dirName := name[0:idx]
		name = name[idx+1:]

		mp.mu.Lock()
		dirInode := mp.findChildUnlockedFull(dirName)
		if dirInode == nil {
			fs.mu.Lock()

			dirInode = NewInode(fs, mp, &dirName)
			dirInode.ToDir()
			dirInode.AttrTime = TIME_MAX
			// if nothing is mounted here, set DirTime to
			// infinite so listing will always use cache
			dirInode.dir.DirTime = TIME_MAX

			fs.insertInode(mp, dirInode)

			// insert the fake . and ..
			dot := NewInode(fs, dirInode, PString("."))
			dot.ToDir()
			dot.AttrTime = TIME_MAX
			fs.insertInode(dirInode, dot)

			dot = NewInode(fs, dirInode, PString(".."))
			dot.ToDir()
			dot.AttrTime = TIME_MAX
			fs.insertInode(dirInode, dot)
			fs.mu.Unlock()
		}
		mp.mu.Unlock()
		mp = dirInode
	}

	mp.mu.Lock()
	defer mp.mu.Unlock()

	prev := mp.findChildUnlockedFull(name)
	if prev == nil {
		mountInode := NewInode(fs, mp, &name)
		mountInode.ToDir()
		mountInode.dir.cloud = b.cloud
		mountInode.dir.mountPrefix = b.prefix
		mountInode.AttrTime = TIME_MAX

		fs.mu.Lock()
		defer fs.mu.Unlock()

		fs.insertInode(mp, mountInode)
		prev = mountInode
	} else {
		if !prev.isDir() {
			panic(fmt.Sprintf("inode %v is not a directory", *prev.FullName()))
		}

		prev.mu.Lock()
		defer prev.mu.Unlock()
		prev.dir.cloud = b.cloud
		prev.dir.mountPrefix = b.prefix
		prev.AttrTime = TIME_MAX
		// unset this so that if there was a specific mount
		// for a/b and we are mounted at a/, listing would
		// actually list this backend
		prev.dir.DirTime = time.Time{}
	}
	fuseLog.Infof("mounted /%v", *prev.FullName())
	b.mounted = true
}

func (m *MountProviderBackend) refreshMounts() error {
	mounts, err := m.List()
	if err != nil {
		return err
	}

	for _, b := range mounts {
		m.mount(b)
	}

	return nil
}

func (m *MountProviderBackend) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	err := m.refreshMounts()
	if err != nil {
		return nil, err
	}

	node := m.inode.findPath(param.Key)
	if node == nil {
		return nil, fuse.ENOENT
	} else {
		return &HeadBlobOutput{
			BlobItemOutput: BlobItemOutput{
				Key:          &param.Key,
				LastModified: &node.Attributes.Mtime,
				Size:         node.Attributes.Size,
			},
			IsDirBlob: node.isDir(),
		}, nil
	}
}

func (m *MountProviderBackend) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	if param.Delimiter == nil || *param.Delimiter != "/" ||
		(param.Prefix != nil && *param.Prefix != "" && !strings.HasSuffix(*param.Prefix, "/")) ||
		param.StartAfter != nil || param.ContinuationToken != nil || param.MaxKeys != nil {
		return nil, syscall.ENOTSUP
	}

	err := m.refreshMounts()
	if err != nil {
		return nil, err
	}

	var dir *Inode
	var prefix string
	if param.Prefix == nil || *param.Prefix == "" {
		dir = m.inode
	} else {
		prefix = *param.Prefix
		dir = m.inode.findPath(strings.TrimRight(prefix, "/"))
		if !dir.isDir() {
			return &ListBlobsOutput{}, nil
		}
	}

	var prefixes []BlobPrefixOutput
	var items []BlobItemOutput

	for _, c := range m.inode.dir.Children {
		if *c.Name != "." && *c.Name != ".." {
			if c.isDir() {
				prefixes = append(prefixes, BlobPrefixOutput{
					Prefix: PString(prefix + *c.Name + "/"),
				})
			} else {
				items = append(items, BlobItemOutput{
					Key:          PString(prefix + *c.Name),
					LastModified: &c.Attributes.Mtime,
					Size:         c.Attributes.Size,
				})
			}
		}
	}

	return &ListBlobsOutput{
		Prefixes: prefixes,
		Items:    items,
	}, nil
}

func (m *MountProviderBackend) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

func (m *MountProviderBackend) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	return nil, syscall.ENOTSUP
}

func (m *MountProviderBackend) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

func (m *MountProviderBackend) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

func (m *MountProviderBackend) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

func (m *MountProviderBackend) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

func (m *MountProviderBackend) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	return nil, syscall.ENOTSUP
}

func (m *MountProviderBackend) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	return nil, syscall.ENOTSUP
}

func (m *MountProviderBackend) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	return nil, syscall.ENOTSUP
}

func (m *MountProviderBackend) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	return nil, syscall.ENOTSUP
}

func (m *MountProviderBackend) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	return nil, syscall.ENOTSUP
}

func (m *MountProviderBackend) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error) {
	return nil, syscall.ENOTSUP
}

func (m *MountProviderBackend) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error) {
	return nil, syscall.ENOTSUP
}
