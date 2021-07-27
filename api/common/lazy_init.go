// Copyright 2021 Databricks
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

package common

import (
	"context"
	"sync"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

type Initer interface {
	Init() error
}

type InitFileSystem interface {
	Initer
	fuseutil.FileSystem
}

type LazyInitFileSystem struct {
	Fs        InitFileSystem
	init      sync.Once
	initError error
}

func (fs *LazyInitFileSystem) Init() error {
	fs.init.Do(func() {
		fs.initError = fs.Fs.Init()
	})

	return fs.initError
}

func (fs *LazyInitFileSystem) StatFS(ctx context.Context, op *fuseops.StatFSOp) (err error) {
	// don't need to init filesystem to do statfs() since we
	// return fake data anyway
	return fs.Fs.StatFS(ctx, op)
}

func (fs *LazyInitFileSystem) LookUpInode(ctx context.Context, op *fuseops.LookUpInodeOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.LookUpInode(ctx, op)
}

func (fs *LazyInitFileSystem) GetInodeAttributes(ctx context.Context, op *fuseops.GetInodeAttributesOp) (err error) {
	// don't need to init filesystem if we are checking the root
	// inode since we return fake data anyway. Also this is used
	// by mountpoint(1) and we don't want that to block
	if op.Inode != fuseops.RootInodeID {
		err = fs.Init()
		if err != nil {
			return
		}
	}

	return fs.Fs.GetInodeAttributes(ctx, op)
}

func (fs *LazyInitFileSystem) SetInodeAttributes(ctx context.Context, op *fuseops.SetInodeAttributesOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.SetInodeAttributes(ctx, op)
}

func (fs *LazyInitFileSystem) Fallocate(ctx context.Context, op *fuseops.FallocateOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.Fallocate(ctx, op)
}

func (fs *LazyInitFileSystem) ForgetInode(ctx context.Context, op *fuseops.ForgetInodeOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.ForgetInode(ctx, op)
}

func (fs *LazyInitFileSystem) MkDir(ctx context.Context, op *fuseops.MkDirOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.MkDir(ctx, op)
}

func (fs *LazyInitFileSystem) MkNode(ctx context.Context, op *fuseops.MkNodeOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.MkNode(ctx, op)
}

func (fs *LazyInitFileSystem) CreateFile(ctx context.Context, op *fuseops.CreateFileOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.CreateFile(ctx, op)
}

func (fs *LazyInitFileSystem) CreateLink(ctx context.Context, op *fuseops.CreateLinkOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.CreateLink(ctx, op)
}

func (fs *LazyInitFileSystem) CreateSymlink(ctx context.Context, op *fuseops.CreateSymlinkOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.CreateSymlink(ctx, op)
}

func (fs *LazyInitFileSystem) Rename(ctx context.Context, op *fuseops.RenameOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.Rename(ctx, op)
}

func (fs *LazyInitFileSystem) RmDir(ctx context.Context, op *fuseops.RmDirOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.RmDir(ctx, op)
}

func (fs *LazyInitFileSystem) Unlink(ctx context.Context, op *fuseops.UnlinkOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.Unlink(ctx, op)
}

func (fs *LazyInitFileSystem) OpenDir(ctx context.Context, op *fuseops.OpenDirOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.OpenDir(ctx, op)
}

func (fs *LazyInitFileSystem) ReadDir(ctx context.Context, op *fuseops.ReadDirOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.ReadDir(ctx, op)
}

func (fs *LazyInitFileSystem) ReleaseDirHandle(ctx context.Context, op *fuseops.ReleaseDirHandleOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.ReleaseDirHandle(ctx, op)
}

func (fs *LazyInitFileSystem) OpenFile(ctx context.Context, op *fuseops.OpenFileOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.OpenFile(ctx, op)
}

func (fs *LazyInitFileSystem) ReadFile(ctx context.Context, op *fuseops.ReadFileOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.ReadFile(ctx, op)
}

func (fs *LazyInitFileSystem) WriteFile(ctx context.Context, op *fuseops.WriteFileOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.WriteFile(ctx, op)
}

func (fs *LazyInitFileSystem) SyncFile(ctx context.Context, op *fuseops.SyncFileOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.SyncFile(ctx, op)
}

func (fs *LazyInitFileSystem) FlushFile(ctx context.Context, op *fuseops.FlushFileOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.FlushFile(ctx, op)
}

func (fs *LazyInitFileSystem) ReleaseFileHandle(ctx context.Context, op *fuseops.ReleaseFileHandleOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.ReleaseFileHandle(ctx, op)
}

func (fs *LazyInitFileSystem) ReadSymlink(ctx context.Context, op *fuseops.ReadSymlinkOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.ReadSymlink(ctx, op)
}

func (fs *LazyInitFileSystem) RemoveXattr(ctx context.Context, op *fuseops.RemoveXattrOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.RemoveXattr(ctx, op)
}

func (fs *LazyInitFileSystem) GetXattr(ctx context.Context, op *fuseops.GetXattrOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.GetXattr(ctx, op)
}

func (fs *LazyInitFileSystem) ListXattr(ctx context.Context, op *fuseops.ListXattrOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.ListXattr(ctx, op)
}

func (fs *LazyInitFileSystem) SetXattr(ctx context.Context, op *fuseops.SetXattrOp) (err error) {
	err = fs.Init()
	if err != nil {
		return
	}

	return fs.Fs.SetXattr(ctx, op)
}

func (fs *LazyInitFileSystem) Destroy() {
	fs.Fs.Destroy()
}
