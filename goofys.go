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
	"os"
	"time"

	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/jacobsa/syncutil"
)


// goofys is a Filey System written in Go. All the backend data is
// stored on S3 as is. It's a Filey System instead of a File System
// because it makes minimal effort at being POSIX
// compliant. Particularly things that are difficult to support on S3
// or would translate into more than one round-trip would either fail
// (rename non-empty dir) or faked (no per-file permission). goofys
// does not have a on disk data cache, and consistency model is
// close-to-open.

type Goofys struct {
	fuseutil.NotImplementedFileSystem
	bucket string
	// The UID and GID that every inode receives.
	uid uint32
	gid uint32
	umask uint32

	s3 *s3.S3
	rootAttrs fuseops.InodeAttributes;

	bufferPool *BufferPool

	// A lock protecting the state of the file system struct itself (distinct
	// from per-inode locks). Make sure to see the notes on lock ordering above.
	mu syncutil.InvariantMutex

	// The next inode ID to hand out. We assume that this will never overflow,
	// since even if we were handing out inode IDs at 4 GHz, it would still take
	// over a century to do so.
	//
	// GUARDED_BY(mu)
	nextInodeID fuseops.InodeID

	// The collection of live inodes, keyed by inode ID. No ID less than
	// fuseops.RootInodeID is ever used.
	//
	// INVARIANT: For all keys k, fuseops.RootInodeID <= k < nextInodeID
	// INVARIANT: For all keys k, inodes[k].ID() == k
	// INVARIANT: inodes[fuseops.RootInodeID] is missing or of type inode.DirInode
	// INVARIANT: For all v, if IsDirName(v.Name()) then v is inode.DirInode
	//
	// GUARDED_BY(mu)
	inodes map[fuseops.InodeID]*Inode
	inodesCache map[string]*Inode // fullname to inode

	nextHandleID fuseops.HandleID
	dirHandles map[fuseops.HandleID]*DirHandle

	fileHandles map[fuseops.HandleID]*FileHandle
}

func NewGoofys(bucket string, awsConfig *aws.Config, uid uint32, gid uint32) *Goofys {
	// Set up the basic struct.
	fs := &Goofys{
		bucket: bucket,
		uid:    uid,
		gid:    gid,
		umask:  0122,
	}

	fs.s3 = s3.New(awsConfig)
	now := time.Now()
	fs.rootAttrs = fuseops.InodeAttributes{
		Size: 4096,
		Nlink: 2,
		Mode: 0700 | os.ModeDir,
		Atime: now,
		Mtime: now,
		Ctime: now,
		Crtime: now,
		Uid:  uid,
		Gid:  gid,
	}

	fs.bufferPool = NewBufferPool(100 * 1024 * 1024, 20 * 1024 * 1024)

	fs.nextInodeID = fuseops.RootInodeID + 1
	fs.inodes = make(map[fuseops.InodeID]*Inode)
	root := NewInode(aws.String(""), aws.String(""))
	root.Id = fuseops.RootInodeID
	root.Attributes = &fs.rootAttrs

	fs.inodes[fuseops.RootInodeID] = root
	fs.inodesCache = make(map[string]*Inode)

	fs.nextHandleID = 1
	fs.dirHandles = make(map[fuseops.HandleID]*DirHandle)

	fs.fileHandles = make(map[fuseops.HandleID]*FileHandle)

	// Set up invariant checking.
	fs.mu = syncutil.NewInvariantMutex(fs.checkInvariants)

	return fs;
}

func (fs *Goofys) checkInvariants() {
	//////////////////////////////////
	// inodes
	//////////////////////////////////

	// INVARIANT: For all keys k, fuseops.RootInodeID <= k < nextInodeID
	for id, _ := range fs.inodes {
		if id < fuseops.RootInodeID || id >= fs.nextInodeID {
			panic(fmt.Sprintf("Illegal inode ID: %v", id))
		}
	}
}

// Find the given inode. Panic if it doesn't exist.
//
// LOCKS_REQUIRED(fs.mu)
func (fs *Goofys) getInodeOrDie(id fuseops.InodeID) (inode *Inode) {
	inode = fs.inodes[id]
	if inode == nil {
		panic(fmt.Sprintf("Unknown inode: %v", id))
	}

	return
}

func (fs *Goofys) logFuse(op string, args ...interface{}) {
	log.Printf("%v: %v", op, args)
}

func (fs *Goofys) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) (err error) {

	const BLOCK_SIZE = 4096
	const TOTAL_SPACE = 1 * 1024 * 1024 * 1024 * 1024 * 1024 // 1PB
	const TOTAL_BLOCKS = TOTAL_SPACE / BLOCK_SIZE
	const INODES = 1 * 1000 * 1000 * 1000 // 1 billion
	op.BlockSize = BLOCK_SIZE
	op.Blocks = TOTAL_BLOCKS
	op.BlocksFree = TOTAL_BLOCKS
	op.BlocksAvailable = TOTAL_BLOCKS
	op.IoSize = 1 * 1024 * 1024 // 1MB
	op.Inodes = INODES
	op.InodesFree = INODES
	return
}

func (fs *Goofys) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) (err error) {

	fs.mu.Lock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.Unlock()

	attr, err := inode.GetAttributes(fs)
	op.Attributes = *attr
	op.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)

	return
}

func mapAwsError(err error) error {
	if awsErr, ok := err.(awserr.Error); ok {
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			// A service error occurred
			switch reqErr.StatusCode() {
			case 404:
				return fuse.ENOENT
			default:
				log.Printf("code=%v request=%v\n", reqErr.StatusCode(), reqErr.RequestID())
				return reqErr
			}
		} else {
			// Generic AWS Error with Code, Message, and original error (if any) 
			log.Printf("code=%v msg=%v, err=%v\n", awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			return awsErr
		}
	} else {
		return err
	}
}

func (fs *Goofys) LookUpInodeNotDir(name *string, c chan s3.HeadObjectOutput, errc chan error) {
	params := &s3.HeadObjectInput{ Bucket: &fs.bucket, Key: name }
	resp, err := fs.s3.HeadObject(params)
	if err != nil {
		errc <- mapAwsError(err)
		return
	}

	log.Println(resp)
	c <- *resp
}

func (fs *Goofys) LookUpInodeDir(name *string, c chan s3.ListObjectsOutput, errc chan error) {
	params := &s3.ListObjectsInput{
		Bucket:       &fs.bucket,
		Delimiter:    aws.String("/"),
		MaxKeys:      aws.Int64(1),
		Prefix:       aws.String(*name + "/"),
	}

	resp, err := fs.s3.ListObjects(params)
	if err != nil {
		errc <- mapAwsError(err)
		return
	}

	log.Println(resp)
	c <- *resp
}

func (fs *Goofys) allocateInodeId() (id fuseops.InodeID) {
	id = fs.nextInodeID
	fs.nextInodeID++
	return
}

// returned inode has nil Id
func (fs *Goofys) LookUpInodeMaybeDir(name *string, fullName *string) (inode *Inode, err error) {
	errObjectChan := make(chan error, 1)
	objectChan := make(chan s3.HeadObjectOutput, 1)
	errDirChan := make(chan error, 1)
	dirChan := make(chan s3.ListObjectsOutput, 1)

	go fs.LookUpInodeNotDir(fullName, objectChan, errObjectChan)
	go fs.LookUpInodeDir(fullName, dirChan, errDirChan)

	notFound := false

	for {
		select {
		case resp := <- objectChan:
			// XXX/TODO if both object and object/ exists, return dir
			inode = NewInode(name, fullName)
			inode.Attributes = &fuseops.InodeAttributes{
				Size: uint64(*resp.ContentLength),
				Nlink: 1,
				Mode: 0644,
				Atime: *resp.LastModified,
				Mtime: *resp.LastModified,
				Ctime: *resp.LastModified,
				Crtime: *resp.LastModified,
				Uid:  fs.uid,
				Gid:  fs.gid,
			}
			return
		case err = <- errObjectChan:
			if err == fuse.ENOENT {
				if notFound {
					return nil, err
				} else {
					notFound = true
					err = nil
				}
			} else {
				// XXX retry
			}
		case resp := <- dirChan:
			if len(resp.CommonPrefixes) != 0 || len(resp.Contents) != 0 {
				inode = NewInode(name, fullName)
				inode.Attributes = &fs.rootAttrs
				return
			} else {
				// 404
				if notFound {
					return nil, fuse.ENOENT
				} else {
					notFound = true
				}
			}
		case err = <- errDirChan:
			// XXX retry
		}
	}
}

func (fs *Goofys) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) (err error) {

	fs.mu.Lock()

	parent := fs.getInodeOrDie(op.Parent)
	inode, ok := fs.inodesCache[*parent.getChildName(&op.Name)]
	if ok {
		defer inode.Ref()
	} else {
		fs.mu.Unlock()

		inode, err = parent.LookUp(fs, &op.Name)
		if err != nil {
			return err
		}

		fs.mu.Lock()
		inode.Id = fs.allocateInodeId()
		fs.inodesCache[*inode.FullName] = inode
	}

	fs.inodes[inode.Id] = inode
	op.Entry.Child = inode.Id
	op.Entry.Attributes = *inode.Attributes
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = time.Now().Add(365 * 24 * time.Hour)
	fs.mu.Unlock()

	return
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *Goofys) ForgetInode(
	ctx context.Context,
	op *fuseops.ForgetInodeOp) (err error) {

	fs.mu.Lock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.Unlock()

	stale := inode.DeRef(op.N)

	if (stale) {
		fs.mu.Lock()
		defer fs.mu.Unlock()

		delete(fs.inodes, op.Inode)
		delete(fs.inodesCache, *inode.FullName)
	}

	return
}

func (fs *Goofys) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) (err error) {
	fs.mu.Lock()

	handleID := fs.nextHandleID
	fs.nextHandleID++

	in := fs.getInodeOrDie(op.Inode)
	fs.mu.Unlock()

	// XXX/is this a dir?
	dh := in.OpenDir()

	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.dirHandles[handleID] = dh
	op.Handle = handleID

	return
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *Goofys) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) (err error) {

	// Find the handle.
	fs.mu.Lock()
	dh := fs.dirHandles[op.Handle]
	//inode := fs.inodes[op.Inode]
	fs.mu.Unlock()

	if dh == nil {
		panic(fmt.Sprintf("can't find dh=%v", op.Handle))
	}

	log.Println("> ReadDir", *dh.inode.FullName, op.Offset)

	for i := op.Offset; ; i++ {
		e, err := dh.ReadDir(fs, i)
		if err != nil {
			return err
		}
		if e == nil {
			break
		}

		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], *e)
		if n == 0 {
			break
		}
		log.Printf("< ReadDir %v %v", *dh.inode.FullName, e.Name)

		op.BytesRead += n
	}

	return
}


func (fs *Goofys) ReleaseDirHandle(
	ctx context.Context,
	op *fuseops.ReleaseDirHandleOp) (err error) {


	fs.mu.Lock()
	defer fs.mu.Unlock()

	dh := fs.dirHandles[op.Handle]
	dh.CloseDir()
	log.Printf("ReleaseDirHandle %v", *dh.inode.FullName)

	delete(fs.dirHandles, op.Handle)

	return
}


func (fs *Goofys) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) (err error) {
	fs.mu.Lock()
	in := fs.getInodeOrDie(op.Inode)
	fs.mu.Unlock()

	fh := in.OpenFile(fs)

	fs.mu.Lock()
	defer fs.mu.Unlock()

	handleID := fs.nextHandleID
	fs.nextHandleID++

	fs.fileHandles[handleID] = fh

	op.Handle = handleID
	op.KeepPageCache = true

	return
}

func (fs *Goofys) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) (err error) {

	fs.mu.Lock()
	fh := fs.fileHandles[op.Handle]
	fs.mu.Unlock()

	op.BytesRead, err = fh.ReadFile(fs, op.Offset, op.Dst)

	return
}

func (fs *Goofys) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) (err error) {

	fs.mu.Lock()
	fh := fs.fileHandles[op.Handle]
	fs.mu.Unlock()

	err = fh.FlushFile(fs)

	return
}


func (fs *Goofys) ReleaseFileHandle(
	ctx context.Context,
	op *fuseops.ReleaseFileHandleOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	delete(fs.fileHandles, op.Handle)
	return
}


func (fs *Goofys) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) (err error) {

	fs.mu.Lock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.Unlock()

	inode, fh := parent.Create(fs, &op.Name)

	fs.mu.Lock()
	defer fs.mu.Unlock()

	nextInode := fs.nextInodeID
	fs.nextInodeID++

	inode.Id = nextInode

	fs.inodes[inode.Id] = inode
	op.Entry.Child = inode.Id
	op.Entry.Attributes = *inode.Attributes
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	op.Entry.EntryExpiration = time.Now().Add(365 * 24 * time.Hour)

	// Allocate a handle.
	handleID := fs.nextHandleID
	fs.nextHandleID++

	fs.fileHandles[handleID] = fh

	op.Handle = handleID

	return
}


func (fs *Goofys) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) (err error) {
	// do nothing, we don't support any of the changes
	return
}

func (fs *Goofys) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) (err error) {

	fs.mu.Lock()
	defer fs.mu.Unlock()

	fh, ok := fs.fileHandles[op.Handle]
	if !ok {
		panic(fmt.Sprintf("WriteFile: can't find handle %v", op.Handle))
	}

	err = fh.WriteFile(fs, op.Offset, op.Data)

	return
}

func (fs *Goofys) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) (err error) {

	fs.mu.Lock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.Unlock()

	err = parent.Unlink(fs, &op.Name)
	return
}
