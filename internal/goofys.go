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
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"

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
	bucket string

	flags *FlagStorage

	umask uint32

	awsConfig *aws.Config
	sess      *session.Session
	s3        *s3.S3
	rootAttrs fuse.Attr
	root      *Inode

	bufferPool *BufferPool

	// A lock protecting the state of the file system struct itself (distinct
	// from per-inode locks). Make sure to see the notes on lock ordering above.
	mu sync.Mutex

	inodesCache map[string]*Inode // fullname to inode
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
	fs.s3 = s3.New(fs.sess)

	params := &s3.GetBucketLocationInput{Bucket: &bucket}
	resp, err := fs.s3.GetBucketLocation(params)
	var fromRegion, toRegion string
	if err != nil {
		if mapAwsError(err) == fuse.ENOENT {
			log.Errorf("bucket %v does not exist", bucket)
			return nil
		}
		fromRegion, toRegion = parseRegionError(err)
	} else {
		s3Log.Debug(resp)

		if resp.LocationConstraint == nil {
			toRegion = "us-east-1"
		} else {
			toRegion = *resp.LocationConstraint
		}

		fromRegion = *awsConfig.Region
	}

	if len(toRegion) != 0 && fromRegion != toRegion {
		s3Log.Infof("Switching from region '%v' to '%v'", fromRegion, toRegion)
		awsConfig.Region = &toRegion
		fs.sess = session.New(awsConfig)
		fs.s3 = s3.New(fs.sess)
		_, err = fs.s3.GetBucketLocation(params)
		if err != nil {
			log.Errorln(err)
			return nil
		}
	} else if len(toRegion) == 0 && *awsConfig.Region != "milkyway" {
		s3Log.Infof("Unable to detect bucket region, staying at '%v'", *awsConfig.Region)
	}

	now := time.Now()
	fs.rootAttrs = fuse.Attr{
		Inode:  0,
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
	rootName := ""
	fs.root = NewInode(&rootName, &rootName, flags)
	fs.root.Id = 1
	fs.root.Attributes = &fs.rootAttrs

	fs.bufferPool = NewBufferPool(1000*1024*1024, 200*1024*1024)

	fs.inodesCache = make(map[string]*Inode)

	return fs
}

type node struct {
	fs    *Goofys
	inode *Inode
}

type fhandle struct {
	fs     *Goofys
	handle *FileHandle
}

type dhandle struct {
	fs     *Goofys
	handle *DirHandle
}

func (fs *Goofys) Root() (fs.Node, error) {
	return &node{fs, fs.root}, nil
}

func (fs *Goofys) Statfs(
	ctx context.Context,
	req *fuse.StatfsRequest,
	resp *fuse.StatfsResponse) (err error) {

	const BLOCK_SIZE = 4096
	const TOTAL_SPACE = 1 * 1024 * 1024 * 1024 * 1024 * 1024 // 1PB
	const TOTAL_BLOCKS = TOTAL_SPACE / BLOCK_SIZE
	const INODES = 1 * 1000 * 1000 * 1000 // 1 billion
	resp.Blocks = TOTAL_BLOCKS
	resp.Bfree = TOTAL_BLOCKS
	resp.Bavail = TOTAL_BLOCKS
	resp.Files = 2
	resp.Ffree = INODES
	resp.Bsize = 1 * 1024 * 1024
	resp.Namelen = 1024 // s3 max key length
	resp.Frsize = BLOCK_SIZE

	return
}

func (n *node) Attr(ctx context.Context, attr *fuse.Attr) error {
	*attr = *n.inode.Attributes
	attr.Inode = n.inode.Id
	return nil
}

func (n *node) Create(
	ctx context.Context,
	req *fuse.CreateRequest,
	resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {

	inode, fh := n.inode.Create(n.fs, req.Name)
	if inode != nil {
		n.fs.mu.Lock()
		n.fs.inodesCache[*inode.FullName] = inode
		n.fs.mu.Unlock()
	}

	return &node{n.fs, inode}, &fhandle{n.fs, fh}, nil
}

func (n *node) Forget() {
	n.fs.mu.Lock()
	delete(n.fs.inodesCache, *n.inode.FullName)
	n.fs.mu.Unlock()
}

func (n *node) Getattr(
	ctx context.Context,
	req *fuse.GetattrRequest,
	resp *fuse.GetattrResponse) error {

	attr, err := n.inode.GetAttributes(n.fs)
	resp.Attr = *attr
	resp.Attr.Inode = n.inode.Id
	return err
}

func (n *node) Mkdir(
	ctx context.Context,
	req *fuse.MkdirRequest) (fs.Node, error) {

	inode, err := n.inode.MkDir(n.fs, req.Name)
	return &node{n.fs, inode}, err
}

func (n *node) Open(
	ctx context.Context,
	req *fuse.OpenRequest,
	resp *fuse.OpenResponse) (fs.Handle, error) {

	if req.Dir {
		dh := n.inode.OpenDir()
		return &dhandle{n.fs, dh}, nil
	} else {
		fh := n.inode.OpenFile(n.fs)
		resp.Flags |= /*fuse.OpenDirectIO | */ fuse.OpenKeepCache
		return &fhandle{n.fs, fh}, nil
	}
}

func (n *node) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	if req.Dir {
		return n.inode.RmDir(n.fs, req.Name)
	} else {
		return n.inode.Unlink(n.fs, req.Name)
	}
}

func (n *node) Rename(
	ctx context.Context,
	req *fuse.RenameRequest,
	newDir fs.Node) error {

	newParent := newDir.(*node)
	n.fs.mu.Lock()
	inode := n.fs.inodesCache[n.inode.getChildName(req.OldName)]
	n.fs.mu.Unlock()

	err := n.inode.Rename(n.fs, req.OldName, newParent.inode, req.NewName)

	if inode != nil {
		n.fs.mu.Lock()
		inode.Name = &req.NewName
		inode.FullName = aws.String(newParent.inode.getChildName(req.NewName))
		n.fs.mu.Unlock()
	}

	return err
}

func (n *node) Lookup(
	ctx context.Context,
	name string) (fs.Node, error) {

	inode, err := n.inode.LookUp(n.fs, name)
	if inode != nil {
		n.fs.mu.Lock()
		n.fs.inodesCache[*inode.FullName] = inode
		n.fs.mu.Unlock()
	}
	return &node{n.fs, inode}, err
}

func (f *fhandle) Flush(
	ctx context.Context,
	req *fuse.FlushRequest) error {

	return f.handle.FlushFile(f.fs)
}

func (f *fhandle) Read(
	ctx context.Context,
	req *fuse.ReadRequest,
	resp *fuse.ReadResponse) error {

	resp.Data = resp.Data[0:req.Size]
	nread, err := f.handle.ReadFile(f.fs, req.Offset, resp.Data)
	if err == nil {
		resp.Data = resp.Data[:nread]
	}
	return err
}

func (f *fhandle) Release(
	ctx context.Context,
	req *fuse.ReleaseRequest) error {

	f.handle.Release()
	return nil
}

func (f *fhandle) Write(
	ctx context.Context,
	req *fuse.WriteRequest,
	resp *fuse.WriteResponse) error {

	err := f.handle.WriteFile(f.fs, req.Offset, req.Data)
	if err == nil {
		resp.Size = len(req.Data)
	}
	return err
}

func (d *dhandle) ReadDir(ctx context.Context, offset int64) (dir *fuse.Dirent, err error) {
	dir, err = d.handle.ReadDir(d.fs, int(offset))
	if err != nil || dir == nil {
		return
	}

	d.handle.inode.logFuse("<-- ReadDir", dir.Name)

	return
}

func (d *dhandle) Release(
	ctx context.Context,
	req *fuse.ReleaseRequest) error {

	return d.handle.CloseDir()
}

const REGION_ERROR_MSG = "The authorization header is malformed; the region %s is wrong; expecting %s"

func parseRegionError(err error) (fromRegion, toRegion string) {
	if reqErr, ok := err.(awserr.RequestFailure); ok {
		// A service error occurred
		if reqErr.StatusCode() == 400 && reqErr.Code() == "AuthorizationHeaderMalformed" {
			n, scanErr := fmt.Sscanf(reqErr.Message(), REGION_ERROR_MSG, &fromRegion, &toRegion)
			if n != 2 || scanErr != nil {
				fmt.Println(n, scanErr)
				return
			}
			fromRegion = fromRegion[1 : len(fromRegion)-1]
			toRegion = toRegion[1 : len(toRegion)-1]
			return
		} else if reqErr.StatusCode() == 501 {
			// method not implemented,
			// do nothing, use existing region
		} else {
			s3Log.Errorf("code=%v msg=%v request=%v\n",
				reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
		}
	}
	return
}

func mapAwsError(err error) error {
	if awsErr, ok := err.(awserr.Error); ok {
		if reqErr, ok := err.(awserr.RequestFailure); ok {
			// A service error occurred
			switch reqErr.StatusCode() {
			case 404:
				return fuse.ENOENT
			case 405:
				return fuse.Errno(syscall.ENOTSUP)
			default:
				s3Log.Errorf("code=%v msg=%v request=%v\n", reqErr.Message(), reqErr.StatusCode(), reqErr.RequestID())
				return reqErr
			}
		} else {
			// Generic AWS Error with Code, Message, and original error (if any)
			s3Log.Errorf("code=%v msg=%v, err=%v\n", awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			return awsErr
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

func (fs *Goofys) copyObjectMultipart(size int64, from string, to string, mpuId string) (err error) {
	var wg sync.WaitGroup
	nParts := sizeToParts(size)
	etags := make([]*string, nParts)

	if mpuId == "" {
		params := &s3.CreateMultipartUploadInput{
			Bucket:       &fs.bucket,
			Key:          &to,
			StorageClass: &fs.flags.StorageClass,
		}

		resp, err := fs.s3.CreateMultipartUpload(params)
		if err != nil {
			return mapAwsError(err)
		}

		mpuId = *resp.UploadId
	}

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
		return fs.copyObjectMultipart(size, from, to, "")
	}

	params := &s3.CopyObjectInput{
		Bucket:       &fs.bucket,
		CopySource:   &from,
		Key:          &to,
		StorageClass: &fs.flags.StorageClass,
	}

	s3Log.Debug(params)

	_, err = fs.s3.CopyObject(params)
	if err != nil {
		err = mapAwsError(err)
	}

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
			inode.Attributes = &fuse.Attr{
				Size:   uint64(*resp.ContentLength),
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
