// Copyright 2015 Ka-Hing Cheung
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
	"os"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/corehandlers"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	"github.com/Sirupsen/logrus"
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

	flags *FlagStorage

	umask uint32

	awsConfig *aws.Config
	sess      *session.Session
	s3        *s3.S3
	v2Signer  bool
	rootAttrs fuseops.InodeAttributes

	bufferPool *BufferPool

	// A lock protecting the state of the file system struct itself (distinct
	// from per-inode locks). Make sure to see the notes on lock ordering above.
	mu sync.Mutex

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
	inodes      map[fuseops.InodeID]*Inode
	inodesCache map[string]*Inode // fullname to inode

	nextHandleID fuseops.HandleID
	dirHandles   map[fuseops.HandleID]*DirHandle

	fileHandles map[fuseops.HandleID]*FileHandle
}

var s3Log = GetLogger("s3")

func NewGoofys(bucket string, awsConfig *aws.Config, flags *FlagStorage) *Goofys {
	// Set up the basic struct.
	fs := &Goofys{
		bucket: bucket,
		flags:  flags,
		umask:  0122,
	}

	if flags.DebugS3 {
		awsConfig.LogLevel = aws.LogLevel(aws.LogDebug | aws.LogDebugWithRequestErrors)
		s3Log.Level = logrus.DebugLevel
	}

	fs.awsConfig = awsConfig
	fs.sess = session.New(awsConfig)
	fs.s3 = fs.newS3()

	err := fs.testBucket()
	if err != nil {
		switch mapAwsError(err) {
		case fuse.ENOENT:
			log.Errorf("bucket %v does not exist", fs.bucket)
			return nil
		case fuse.EINVAL:
			// only non-aws would require v2 signer, and it's not clear
			// how to detect region in those cases
			fs.fallbackV2Signer()
			err = fs.testBucket()
			if err != nil {
				log.Errorf("Unable to access '%v': %v", fs.bucket, err)
				return nil
			}
		default:
			fs.detectBucketLocationByHEAD()
			fs.sess = session.New(awsConfig)
			fs.s3 = fs.newS3()

			// try again to make sure
			err = fs.testBucket()
			if err != nil {
				log.Errorf("Unable to access '%v': %v", fs.bucket, mapAwsError(err))
				return nil
			}
		}
	}

	now := time.Now()
	fs.rootAttrs = fuseops.InodeAttributes{
		Size:   4096,
		Nlink:  2,
		Mode:   flags.DirMode | os.ModeDir,
		Atime:  now,
		Mtime:  now,
		Ctime:  now,
		Crtime: now,
		Uid:    fs.flags.Uid,
		Gid:    fs.flags.Gid,
	}

	fs.bufferPool = BufferPool{}.Init()

	fs.nextInodeID = fuseops.RootInodeID + 1
	fs.inodes = make(map[fuseops.InodeID]*Inode)
	root := NewInode(aws.String(""), aws.String(""), flags)
	root.Id = fuseops.RootInodeID
	root.Attributes = &fs.rootAttrs

	fs.inodes[fuseops.RootInodeID] = root
	fs.inodesCache = make(map[string]*Inode)

	fs.nextHandleID = 1
	fs.dirHandles = make(map[fuseops.HandleID]*DirHandle)

	fs.fileHandles = make(map[fuseops.HandleID]*FileHandle)

	return fs
}

func (fs *Goofys) fallbackV2Signer() (err error) {
	if fs.v2Signer {
		return fuse.EINVAL
	}

	s3Log.Infoln("Falling back to v2 signer")
	fs.v2Signer = true
	fs.s3 = fs.newS3()
	return
}

func (fs *Goofys) newS3() *s3.S3 {
	svc := s3.New(fs.sess)
	if fs.v2Signer {
		svc.Handlers.Sign.Clear()
		svc.Handlers.Sign.PushBack(SignV2)
		svc.Handlers.Sign.PushBackNamed(corehandlers.BuildContentLengthHandler)
	}
	return svc
}

func (fs *Goofys) testBucket() (err error) {
	_, err = fs.s3.HeadBucket(&s3.HeadBucketInput{Bucket: &fs.bucket})
	return
}

func (fs *Goofys) detectBucketLocationByHEAD() {
	config := &aws.Config{
		Credentials: credentials.AnonymousCredentials,
		Endpoint:    fs.awsConfig.Endpoint,
		// always probe with us-east-1 region, otherwise the behavior of other endpoints
		// maybe different
		Region:   aws.String("us-east-1"),
		Logger:   GetLogger("s3"),
		LogLevel: aws.LogLevel(aws.LogDebug | aws.LogDebugWithRequestErrors),
	}
	sess := session.New(config)
	tmpS3 := s3.New(sess)

	req, _ := tmpS3.HeadBucketRequest(&s3.HeadBucketInput{Bucket: &fs.bucket})
	req.Send()

	region := req.HTTPResponse.Header["X-Amz-Bucket-Region"]
	if len(region) != 0 {
		if region[0] != *fs.awsConfig.Region {
			s3Log.Infof("Switching from region '%v' to '%v'", *fs.awsConfig.Region, region[0])
			fs.awsConfig.Region = &region[0]
		}
	} else {
		s3Log.Infof("Unable to detect bucket region, staying at '%v'", *fs.awsConfig.Region)
	}
	return
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
	op.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)

	return
}

func mapAwsError(err error) error {
	if awsErr, ok := err.(awserr.Error); ok {
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			// A service error occurred
			switch reqErr.StatusCode() {
			case 400:
				return fuse.EINVAL
			case 403:
				return syscall.EACCES
			case 404:
				return fuse.ENOENT
			case 405:
				return syscall.ENOTSUP
			default:
				s3Log.Errorf("code=%v msg=%v request=%v\n", reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
				return reqErr
			}
		} else {
			switch awsErr.Code() {
			case "BucketRegionError":
				// don't need to log anything, we should detect region after
				return err
			default:
				// Generic AWS Error with Code, Message, and original error (if any)
				s3Log.Errorf("code=%v msg=%v, err=%v\n", awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
				return awsErr
			}
		}
	} else {
		return err
	}
}

func (fs *Goofys) LookUpInodeNotDir(name string, c chan s3.HeadObjectOutput, errc chan error) {
	params := &s3.HeadObjectInput{Bucket: &fs.bucket, Key: &name}
	resp, err := fs.s3.HeadObject(params)
	if err != nil {
		errc <- mapAwsError(err)
		return
	}

	s3Log.Debug(resp)
	c <- *resp
}

func (fs *Goofys) LookUpInodeDir(name string, c chan s3.ListObjectsOutput, errc chan error) {
	params := &s3.ListObjectsInput{
		Bucket:    &fs.bucket,
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int64(1),
		Prefix:    aws.String(name + "/"),
	}

	resp, err := fs.s3.ListObjects(params)
	if err != nil {
		errc <- mapAwsError(err)
		return
	}

	s3Log.Debug(resp)
	c <- *resp
}

func (fs *Goofys) mpuCopyPart(from string, to string, mpuId string, bytes string, part int64, wg *sync.WaitGroup,
	etag **string, errout *error) {

	defer func() {
		wg.Done()
	}()

	// XXX use CopySourceIfUnmodifiedSince to ensure that
	// we are copying from the same object
	params := &s3.UploadPartCopyInput{
		Bucket:          &fs.bucket,
		Key:             &to,
		CopySource:      &from,
		UploadId:        &mpuId,
		CopySourceRange: &bytes,
		PartNumber:      &part,
	}

	s3Log.Debug(params)

	resp, err := fs.s3.UploadPartCopy(params)
	if err != nil {
		*errout = mapAwsError(err)
		return
	}

	*etag = resp.CopyPartResult.ETag
	return
}

func sizeToParts(size int64) int {
	const PART_SIZE = 5 * 1024 * 1024 * 1024

	nParts := int(size / PART_SIZE)
	if size%PART_SIZE != 0 {
		nParts++
	}
	return nParts
}

func (fs *Goofys) mpuCopyParts(size int64, from string, to string, mpuId string,
	wg *sync.WaitGroup, etags []*string, err *error) {

	const PART_SIZE = 5 * 1024 * 1024 * 1024

	rangeFrom := int64(0)
	rangeTo := int64(0)

	for i := int64(1); rangeTo < size; i++ {
		rangeFrom = rangeTo
		rangeTo = i * PART_SIZE
		if rangeTo > size {
			rangeTo = size
		}
		bytes := fmt.Sprintf("bytes=%v-%v", rangeFrom, rangeTo-1)

		wg.Add(1)
		go fs.mpuCopyPart(from, to, mpuId, bytes, i, wg, &etags[i-1], err)
	}
}

func (fs *Goofys) copyObjectMultipart(size int64, from string, to string) (err error) {
	var wg sync.WaitGroup
	nParts := sizeToParts(size)
	etags := make([]*string, nParts)

	params := &s3.CreateMultipartUploadInput{
		Bucket:       &fs.bucket,
		Key:          &to,
		StorageClass: &fs.flags.StorageClass,
	}

	resp, err := fs.s3.CreateMultipartUpload(params)
	if err != nil {
		return mapAwsError(err)
	}

	mpuId := *resp.UploadId

	fs.mpuCopyParts(size, from, to, mpuId, &wg, etags, &err)
	wg.Wait()

	if err != nil {
		return
	} else {
		parts := make([]*s3.CompletedPart, nParts)
		for i := 0; i < nParts; i++ {
			parts[i] = &s3.CompletedPart{
				ETag:       etags[i],
				PartNumber: aws.Int64(int64(i + 1)),
			}
		}

		params := &s3.CompleteMultipartUploadInput{
			Bucket:   &fs.bucket,
			Key:      &to,
			UploadId: &mpuId,
			MultipartUpload: &s3.CompletedMultipartUpload{
				Parts: parts,
			},
		}

		s3Log.Debug(params)

		_, err = fs.s3.CompleteMultipartUpload(params)
		if err != nil {
			return mapAwsError(err)
		}
	}

	return
}

func (fs *Goofys) copyObjectMaybeMultipart(size int64, from string, to string) (err error) {
	if size == -1 {
		params := &s3.HeadObjectInput{Bucket: &fs.bucket, Key: &from}
		resp, err := fs.s3.HeadObject(params)
		if err != nil {
			return mapAwsError(err)
		}

		size = *resp.ContentLength
	}

	from = fs.bucket + "/" + from

	if size > 5*1024*1024*1024 {
		return fs.copyObjectMultipart(size, from, to)
	}

	params := &s3.CopyObjectInput{
		Bucket:       &fs.bucket,
		CopySource:   &from,
		Key:          &to,
		StorageClass: &fs.flags.StorageClass,
	}

	_, err = fs.s3.CopyObject(params)
	if err != nil {
		err = mapAwsError(err)
	}

	return
}

func (fs *Goofys) allocateInodeId() (id fuseops.InodeID) {
	id = fs.nextInodeID
	fs.nextInodeID++
	return
}

// returned inode has nil Id
func (fs *Goofys) LookUpInodeMaybeDir(name string, fullName string) (inode *Inode, err error) {
	errObjectChan := make(chan error, 1)
	objectChan := make(chan s3.HeadObjectOutput, 1)
	errDirChan := make(chan error, 1)
	dirChan := make(chan s3.ListObjectsOutput, 1)

	go fs.LookUpInodeNotDir(fullName, objectChan, errObjectChan)
	go fs.LookUpInodeDir(fullName, dirChan, errDirChan)

	notFound := false

	for {
		select {
		case resp := <-objectChan:
			// XXX/TODO if both object and object/ exists, return dir
			inode = NewInode(&name, &fullName, fs.flags)
			inode.Attributes = &fuseops.InodeAttributes{
				Size:   uint64(aws.Int64Value(resp.ContentLength)),
				Nlink:  1,
				Mode:   fs.flags.FileMode,
				Atime:  *resp.LastModified,
				Mtime:  *resp.LastModified,
				Ctime:  *resp.LastModified,
				Crtime: *resp.LastModified,
				Uid:    fs.flags.Uid,
				Gid:    fs.flags.Gid,
			}
			return
		case err = <-errObjectChan:
			if err == fuse.ENOENT {
				if notFound {
					return nil, err
				} else {
					notFound = true
					err = nil
				}
			} else {
				return
			}
		case resp := <-dirChan:
			if len(resp.CommonPrefixes) != 0 || len(resp.Contents) != 0 {
				inode = NewInode(&name, &fullName, fs.flags)
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
		case err = <-errDirChan:
			return
		}
	}
}

func (fs *Goofys) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) (err error) {

	fs.mu.Lock()

	parent := fs.getInodeOrDie(op.Parent)
	inode, ok := fs.inodesCache[parent.getChildName(op.Name)]
	if ok {
		inode.Ref()
		expireTime := inode.AttrTime.Add(fs.flags.StatCacheTTL)
		if !expireTime.After(time.Now()) {
			ok = false
		}
	}
	fs.mu.Unlock()

	if !ok {
		var newInode *Inode

		newInode, err = parent.LookUp(fs, op.Name)
		if err != nil {
			if inode != nil {
				// just kidding! pretend we didn't up the ref
				inode.DeRef(1)
			}
			return err
		}

		if inode == nil {
			fs.mu.Lock()
			inode = newInode
			inode.Id = fs.allocateInodeId()
			fs.inodesCache[*inode.FullName] = inode
			fs.inodes[inode.Id] = inode
			fs.mu.Unlock()
		} else {
			inode.Attributes = newInode.Attributes
			inode.AttrTime = time.Now()
		}
	}

	op.Entry.Child = inode.Id
	op.Entry.Attributes = *inode.Attributes
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.TypeCacheTTL)

	inode.logFuse("<-- LookUpInode")

	return
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *Goofys) ForgetInode(
	ctx context.Context,
	op *fuseops.ForgetInodeOp) (err error) {

	fs.mu.Lock()
	defer fs.mu.Unlock()

	inode := fs.getInodeOrDie(op.Inode)
	stale := inode.DeRef(op.N)

	if stale {
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

	dh.inode.logFuse("ReadDir", op.Offset)

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

		dh.inode.logFuse("<-- ReadDir", e.Name, e.Offset)

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

	fuseLog.Debugln("ReleaseDirHandle", *dh.inode.FullName)

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

func (fs *Goofys) SyncFile(
	ctx context.Context,
	op *fuseops.SyncFileOp) (err error) {

	fs.mu.Lock()
	fh := fs.fileHandles[op.Handle]
	fs.mu.Unlock()

	err = fh.FlushFile(fs)
	return
}

func (fs *Goofys) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) (err error) {

	fs.mu.Lock()
	fh := fs.fileHandles[op.Handle]
	fs.mu.Unlock()

	err = fh.FlushFile(fs)
	if err == nil {
		fs.mu.Lock()
		fs.inodesCache[*fh.inode.FullName] = fh.inode
		fs.mu.Unlock()
	}

	return
}

func (fs *Goofys) ReleaseFileHandle(
	ctx context.Context,
	op *fuseops.ReleaseFileHandleOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fh := fs.fileHandles[op.Handle]
	fh.Release()

	fuseLog.Debugln("ReleaseFileHandle", *fh.inode.FullName)

	delete(fs.fileHandles, op.Handle)
	return
}

func (fs *Goofys) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) (err error) {

	fs.mu.Lock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.Unlock()

	inode, fh := parent.Create(fs, op.Name)

	fs.mu.Lock()
	defer fs.mu.Unlock()

	nextInode := fs.nextInodeID
	fs.nextInodeID++

	inode.Id = nextInode

	fs.inodes[inode.Id] = inode

	op.Entry.Child = inode.Id
	op.Entry.Attributes = *inode.Attributes
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.TypeCacheTTL)

	// Allocate a handle.
	handleID := fs.nextHandleID
	fs.nextHandleID++

	fs.fileHandles[handleID] = fh

	op.Handle = handleID

	inode.logFuse("<-- CreateFile")

	return
}

func (fs *Goofys) MkDir(
	ctx context.Context,
	op *fuseops.MkDirOp) (err error) {

	fs.mu.Lock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.Unlock()

	// ignore op.Mode for now
	inode, err := parent.MkDir(fs, op.Name)
	if err != nil {
		return err
	}

	fs.mu.Lock()
	defer fs.mu.Unlock()

	nextInode := fs.nextInodeID
	fs.nextInodeID++

	inode.Id = nextInode

	fs.inodesCache[*inode.FullName] = inode
	fs.inodes[inode.Id] = inode
	op.Entry.Child = inode.Id
	op.Entry.Attributes = *inode.Attributes
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.TypeCacheTTL)

	return
}

func (fs *Goofys) RmDir(
	ctx context.Context,
	op *fuseops.RmDirOp) (err error) {

	fs.mu.Lock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.Unlock()

	err = parent.RmDir(fs, op.Name)
	return
}

func (fs *Goofys) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) (err error) {

	fs.mu.Lock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.Unlock()

	attr, err := inode.GetAttributes(fs)
	op.Attributes = *attr
	op.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	return
}

func (fs *Goofys) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) (err error) {

	fs.mu.Lock()

	fh, ok := fs.fileHandles[op.Handle]
	if !ok {
		panic(fmt.Sprintf("WriteFile: can't find handle %v", op.Handle))
	}
	fs.mu.Unlock()

	err = fh.WriteFile(fs, op.Offset, op.Data)

	return
}

func (fs *Goofys) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) (err error) {

	fs.mu.Lock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.Unlock()

	err = parent.Unlink(fs, op.Name)
	return
}

func (fs *Goofys) Rename(
	ctx context.Context,
	op *fuseops.RenameOp) (err error) {

	fs.mu.Lock()
	parent := fs.getInodeOrDie(op.OldParent)
	newParent := fs.getInodeOrDie(op.NewParent)
	fs.mu.Unlock()

	return parent.Rename(fs, op.OldName, newParent, op.NewName)
}
