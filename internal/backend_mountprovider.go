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
	"syscall"
)

type Mount struct {
	name   string
	cloud  StorageBackend
	prefix string
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

// LOCKS_REQUIRED(m.inode.mu)
// LOCKS_REQUIRED(m.inode.fs.mu)
func (m *MountProviderBackend) mount(b *Mount) {
	fs := m.inode.fs

	prev := m.inode.findChildUnlockedFull(b.name)
	if prev == nil {
		mountInode := NewInode(fs, m.inode, &b.name)
		mountInode.ToDir()
		mountInode.dir.cloud = b.cloud
		mountInode.dir.mountPrefix = b.prefix
		mountInode.AttrTime = TIME_MAX

		fs.insertInode(m.inode, mountInode)
	} else {
		if !prev.isDir() {
			panic(fmt.Sprintf("inode %v is not a directory", *prev.FullName()))
		}

		prev.mu.Lock()
		defer prev.mu.Unlock()
		prev.dir.cloud = b.cloud
		prev.dir.mountPrefix = b.prefix
		prev.AttrTime = TIME_MAX
	}
}

func (m *MountProviderBackend) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	b, err := m.Lookup(param.Key)
	if err != nil {
		return nil, err
	}

	m.inode.mu.Lock()
	defer m.inode.mu.Unlock()
	fs := m.inode.fs
	fs.mu.Lock()
	defer fs.mu.Unlock()

	m.mount(b)

	return &HeadBlobOutput{
		BlobItemOutput: BlobItemOutput{
			Key: &b.name,
		},
		IsDirBlob: true,
	}, nil
}

func (m *MountProviderBackend) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	if param.Delimiter == nil || *param.Delimiter != "/" ||
		(param.Prefix != nil && *param.Prefix != "") ||
		param.StartAfter != nil || param.ContinuationToken != nil || param.MaxKeys != nil {
		return nil, syscall.ENOTSUP
	}

	mounts, err := m.List()
	if err != nil {
		return nil, err
	}

	m.inode.mu.Lock()
	defer m.inode.mu.Unlock()

	fs := m.inode.fs
	fs.mu.Lock()
	defer fs.mu.Unlock()

	var prefixes []BlobPrefixOutput
	for _, b := range mounts {
		prefixes = append(prefixes, BlobPrefixOutput{
			Prefix: PString(b.name + "/"),
		})

		m.mount(b)
	}

	return &ListBlobsOutput{
		Prefixes: prefixes,
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
