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
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	"github.com/sirupsen/logrus"
)

type Inode struct {
	Id          fuseops.InodeID
	Name        *string
	FullName    *string
	flags       *FlagStorage
	Attributes  *fuseops.InodeAttributes
	KnownSize   *uint64
	Invalid     bool
	AttrTime    time.Time
	ImplicitDir bool
	DirTime     time.Time
	// these 2 refer to readdir of the Children
	lastOpenDirIdx  int
	lastOpenDir     *string
	seqOpenDirScore uint32

	Parent   *Inode
	Children []*Inode

	userMetadata map[string][]byte
	s3Metadata   map[string][]byte

	log *LogHandle

	mu          sync.Mutex // everything below is protected by mu
	fileHandles map[*FileHandle]bool

	// the refcnt is an exception, it's protected by the global lock
	// Goofys.mu
	refcnt uint64
}

func NewInode(parent *Inode, name *string, fullName *string, flags *FlagStorage) (inode *Inode) {
	inode = &Inode{
		Name:        name,
		FullName:    fullName,
		flags:       flags,
		AttrTime:    time.Now(),
		Parent:      parent,
		s3Metadata:  make(map[string][]byte),
		log:         GetLogger(*fullName),
		fileHandles: make(map[*FileHandle]bool),
		refcnt:      1,
	}

	if inode.flags.DebugFuse {
		inode.log.Level = logrus.DebugLevel
	}
	return
}

type DirHandleEntry struct {
	Name   *string
	Inode  fuseops.InodeID
	Type   fuseutil.DirentType
	Offset fuseops.DirOffset

	Attributes   *fuseops.InodeAttributes // XXX use a smaller struct
	ETag         *string
	StorageClass *string
}

func (entry DirHandleEntry) Init(name *string, t fuseutil.DirentType, attr *fuseops.InodeAttributes) *DirHandleEntry {
	entry.Name = name
	entry.Attributes = attr
	entry.Type = t
	return &entry
}

type DirHandle struct {
	inode *Inode

	mu         sync.Mutex // everything below is protected by mu
	Entries    []*DirHandleEntry
	Marker     *string
	BaseOffset int
}

func NewDirHandle(inode *Inode) (dh *DirHandle) {
	dh = &DirHandle{inode: inode}
	return
}

type FileHandle struct {
	inode *Inode

	mpuKey    *string
	dirty     bool
	writeInit sync.Once
	mpuWG     sync.WaitGroup
	etags     []*string

	mu              sync.Mutex
	mpuId           *string
	nextWriteOffset int64
	lastPartId      int

	poolHandle *BufferPool
	buf        *MBuf

	lastWriteError error

	// read
	reader        io.ReadCloser
	readBufOffset int64

	// parallel read
	buffers           []*S3ReadBuffer
	existingReadahead int
	seqReadAmount     uint64
	numOOORead        uint64 // number of out of order read
}

const MAX_READAHEAD = uint32(100 * 1024 * 1024)
const READAHEAD_CHUNK = uint32(20 * 1024 * 1024)

func NewFileHandle(in *Inode) *FileHandle {
	fh := &FileHandle{inode: in}
	return fh
}

func (inode *Inode) logFuse(op string, args ...interface{}) {
	fuseLog.Debugln(op, inode.Id, *inode.FullName, args)
}

func (inode *Inode) errFuse(op string, args ...interface{}) {
	fuseLog.Errorln(op, inode.Id, *inode.FullName, args)
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
		if parent.Children[i].isDir() != isDir {
			return isDir
		}
		return (*parent.Children[i].Name) >= name
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
	l := len(parent.Children)
	if l == 0 {
		return
	}
	i := sort.Search(l, parent.findInodeFunc(name, isDir))
	if i < l {
		// found
		if *parent.Children[i].Name == name {
			inode = parent.Children[i]
		}
	}
	return
}

func (parent *Inode) findChildIdxUnlocked(name string) int {
	l := len(parent.Children)
	if l == 0 {
		return -1
	}
	i := sort.Search(l, parent.findInodeFunc(name, true))
	if i < l {
		// found
		if *parent.Children[i].Name == name {
			return i
		}
	}
	return -1
}

func (parent *Inode) removeChildUnlocked(inode *Inode) {
	l := len(parent.Children)
	if l == 0 {
		return
	}
	i := sort.Search(l, parent.findInodeFunc(*inode.Name, inode.isDir()))
	if i >= l || *parent.Children[i].Name != *inode.Name {
		panic(fmt.Sprintf("%v.removeName(%v) but child not found: %v",
			*parent.FullName, *inode.Name, i))
	}

	copy(parent.Children[i:], parent.Children[i+1:])
	parent.Children = parent.Children[:l-1]
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
	l := len(parent.Children)
	if l == 0 {
		parent.Children = []*Inode{inode}
		return
	}

	i := sort.Search(l, parent.findInodeFunc(*inode.Name, inode.isDir()))
	if i == l {
		// not found = new value is the biggest
		parent.Children = append(parent.Children, inode)
	} else {
		if *parent.Children[i].Name == *inode.Name {
			panic(fmt.Sprintf("double insert of %v", parent.getChildName(*inode.Name)))
		}

		parent.Children = append(parent.Children, nil)
		copy(parent.Children[i+1:], parent.Children[i:])
		parent.Children[i] = inode
	}
}

func (parent *Inode) LookUp(fs *Goofys, name string) (inode *Inode, err error) {
	parent.logFuse("Inode.LookUp", name)

	inode, err = fs.LookUpInodeMaybeDir(parent, name, parent.getChildName(name))
	if err != nil {
		return nil, err
	}

	return
}

func (parent *Inode) getChildName(name string) string {
	if parent.Id == fuseops.RootInodeID {
		return name
	} else {
		return fmt.Sprintf("%v/%v", *parent.FullName, name)
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

// LOCKS_REQUIRED(fs.mu)
func (inode *Inode) DeRef(n uint64) (stale bool) {
	inode.logFuse("DeRef", n, inode.refcnt)

	if inode.refcnt < n {
		panic(fmt.Sprintf("deref %v from %v", n, inode.refcnt))
	}

	inode.refcnt -= n

	stale = (inode.refcnt == 0)
	return
}

func (parent *Inode) Unlink(fs *Goofys, name string) (err error) {
	parent.logFuse("Unlink", name)

	fullName := parent.getChildName(name)

	params := &s3.DeleteObjectInput{
		Bucket: &fs.bucket,
		Key:    fs.key(fullName),
	}

	resp, err := fs.s3.DeleteObject(params)
	if err != nil {
		return mapAwsError(err)
	}

	s3Log.Debug(resp)

	return
}

func (parent *Inode) Create(
	fs *Goofys,
	name string) (inode *Inode, fh *FileHandle) {

	parent.logFuse("Create", name)
	fullName := parent.getChildName(name)

	parent.mu.Lock()
	defer parent.mu.Unlock()

	now := time.Now()
	inode = NewInode(parent, &name, &fullName, parent.flags)
	inode.Attributes = &fuseops.InodeAttributes{
		Size:   0,
		Nlink:  1,
		Mode:   fs.flags.FileMode,
		Atime:  now,
		Mtime:  now,
		Ctime:  now,
		Crtime: now,
		Uid:    fs.flags.Uid,
		Gid:    fs.flags.Gid,
	}

	fh = NewFileHandle(inode)
	fh.poolHandle = fs.bufferPool
	fh.buf = MBuf{}.Init(fh.poolHandle, 0, true)
	fh.dirty = true
	inode.fileHandles[fh] = true

	return
}

func (parent *Inode) MkDir(
	fs *Goofys,
	name string) (inode *Inode, err error) {

	parent.logFuse("MkDir", name)

	fullName := parent.getChildName(name)

	params := &s3.PutObjectInput{
		Bucket: &fs.bucket,
		Key:    fs.key(fullName + "/"),
		Body:   nil,
	}

	if fs.flags.UseSSE {
		params.ServerSideEncryption = &fs.sseType
		if fs.flags.UseKMS && fs.flags.KMSKeyID != "" {
			params.SSEKMSKeyId = &fs.flags.KMSKeyID
		}
	}

	_, err = fs.s3.PutObject(params)
	if err != nil {
		err = mapAwsError(err)
		return
	}

	parent.mu.Lock()
	defer parent.mu.Unlock()

	inode = NewInode(parent, &name, &fullName, parent.flags)
	inode.Attributes = &fs.rootAttrs
	inode.KnownSize = &inode.Attributes.Size

	return
}

func isEmptyDir(fs *Goofys, fullName string) (isDir bool, err error) {
	fullName += "/"

	params := &s3.ListObjectsInput{
		Bucket:    &fs.bucket,
		Delimiter: aws.String("/"),
		MaxKeys:   aws.Int64(2),
		Prefix:    fs.key(fullName),
	}

	resp, err := fs.s3.ListObjects(params)
	if err != nil {
		return false, mapAwsError(err)
	}

	if len(resp.CommonPrefixes) > 0 || len(resp.Contents) > 1 {
		err = fuse.ENOTEMPTY
		isDir = true
		return
	}

	if len(resp.Contents) == 1 {
		isDir = true

		if *resp.Contents[0].Key != *fs.key(fullName) {
			err = fuse.ENOTEMPTY
		}
	}

	return
}

func (parent *Inode) RmDir(
	fs *Goofys,
	name string) (err error) {

	parent.logFuse("Rmdir", name)

	fullName := parent.getChildName(name)

	isDir, err := isEmptyDir(fs, fullName)
	if err != nil {
		return
	}
	if !isDir {
		return fuse.ENOENT
	}

	fullName += "/"

	params := &s3.DeleteObjectInput{
		Bucket: &fs.bucket,
		Key:    fs.key(fullName),
	}

	_, err = fs.s3.DeleteObject(params)
	if err != nil {
		return mapAwsError(err)
	}

	return
}

func (inode *Inode) GetAttributes(fs *Goofys) (*fuseops.InodeAttributes, error) {
	// XXX refresh attributes
	inode.logFuse("GetAttributes")
	if inode.Invalid {
		return nil, fuse.ENOENT
	}
	return inode.Attributes, nil
}

func (inode *Inode) isDir() bool {
	return inode.Attributes.Mode&os.ModeDir != 0
}

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) fillXattrFromHead(resp *s3.HeadObjectOutput) {
	inode.userMetadata = make(map[string][]byte)

	if resp.ETag != nil {
		inode.s3Metadata["etag"] = []byte(*resp.ETag)
	}
	if resp.StorageClass != nil {
		inode.s3Metadata["storage-class"] = []byte(*resp.StorageClass)
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
func (inode *Inode) fillXattr(fs *Goofys) (err error) {
	if !inode.ImplicitDir &&
		(len(inode.s3Metadata) == 0 && inode.userMetadata == nil) {

		fullName := *inode.FullName
		if inode.isDir() {
			fullName += "/"
		}

		params := &s3.HeadObjectInput{Bucket: &fs.bucket, Key: fs.key(fullName)}
		resp, err := fs.s3.HeadObject(params)
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
func (inode *Inode) getXattrMap(fs *Goofys, name string, userOnly bool) (
	meta map[string][]byte, newName string, err error) {

	err = inode.fillXattr(fs)
	if err != nil {
		return nil, "", err
	}

	if strings.HasPrefix(name, "s3.") {
		if userOnly {
			return nil, "", syscall.EACCES
		}

		newName = name[3:]
		meta = inode.s3Metadata
	} else if strings.HasPrefix(name, "user.") {
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
		metadata[k] = aws.String(xattrEscape(v))
	}
	return
}

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) updateXattr(fs *Goofys) (err error) {
	err = fs.copyObjectMaybeMultipart(int64(inode.Attributes.Size),
		*inode.FullName, *inode.FullName,
		aws.String(string(inode.s3Metadata["etag"])), convertMetadata(inode.userMetadata))
	return
}

func (inode *Inode) SetXattr(fs *Goofys, name string, value []byte, flags uint32) error {
	inode.logFuse("RemoveXattr", name)

	inode.mu.Lock()
	defer inode.mu.Unlock()

	meta, name, err := inode.getXattrMap(fs, name, true)
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
	err = inode.updateXattr(fs)
	return err
}

func (inode *Inode) RemoveXattr(fs *Goofys, name string) error {
	inode.logFuse("RemoveXattr", name)

	inode.mu.Lock()
	defer inode.mu.Unlock()

	meta, name, err := inode.getXattrMap(fs, name, true)
	if err != nil {
		return err
	}

	if _, ok := meta[name]; ok {
		delete(meta, name)
		err = inode.updateXattr(fs)
		return err
	} else {
		return syscall.ENODATA
	}
}

func (inode *Inode) GetXattr(fs *Goofys, name string) ([]byte, error) {
	inode.logFuse("GetXattr", name)

	inode.mu.Lock()
	defer inode.mu.Unlock()

	meta, name, err := inode.getXattrMap(fs, name, false)
	if err != nil {
		return nil, err
	}

	value, ok := meta[name]
	if ok {
		return []byte(value), nil
	} else {
		return nil, syscall.ENODATA
	}
}

func (inode *Inode) ListXattr(fs *Goofys) ([]string, error) {
	inode.logFuse("ListXattr")

	inode.mu.Lock()
	defer inode.mu.Unlock()

	var xattrs []string

	err := inode.fillXattr(fs)
	if err != nil {
		return nil, err
	}

	for k, _ := range inode.s3Metadata {
		xattrs = append(xattrs, "s3."+k)
	}

	for k, _ := range inode.userMetadata {
		xattrs = append(xattrs, "user."+k)
	}

	return xattrs, nil
}

func (inode *Inode) OpenFile(fs *Goofys) *FileHandle {
	inode.logFuse("OpenFile")

	inode.mu.Lock()
	defer inode.mu.Unlock()

	fh := NewFileHandle(inode)
	inode.fileHandles[fh] = true
	return fh
}

func (fh *FileHandle) initWrite(fs *Goofys) {
	fh.writeInit.Do(func() {
		fh.mpuWG.Add(1)
		go fh.initMPU(fs)
	})
}

func (fh *FileHandle) initMPU(fs *Goofys) {
	defer func() {
		fh.mpuWG.Done()
	}()

	fh.mpuKey = fh.inode.FullName

	params := &s3.CreateMultipartUploadInput{
		Bucket:       &fs.bucket,
		Key:          fs.key(*fh.mpuKey),
		StorageClass: &fs.flags.StorageClass,
		ContentType:  fs.getMimeType(*fh.inode.FullName),
	}

	if fs.flags.UseSSE {
		params.ServerSideEncryption = &fs.sseType
		if fs.flags.UseKMS && fs.flags.KMSKeyID != "" {
			params.SSEKMSKeyId = &fs.flags.KMSKeyID
		}
	}

	if fs.flags.ACL != "" {
		params.ACL = &fs.flags.ACL
	}

	resp, err := fs.s3.CreateMultipartUpload(params)

	fh.mu.Lock()
	defer fh.mu.Unlock()

	if err != nil {
		fh.lastWriteError = mapAwsError(err)
	}

	s3Log.Debug(resp)

	fh.mpuId = resp.UploadId
	fh.etags = make([]*string, 10000) // at most 10K parts

	return
}

func (fh *FileHandle) mpuPartNoSpawn(fs *Goofys, buf *MBuf, part int) (err error) {
	//fh.inode.logFuse("mpuPartNoSpawn", cap(buf), part)
	fs.replicators.Take(1, true)
	defer fs.replicators.Return(1)

	defer buf.Free()

	if part == 0 || part > 10000 {
		return errors.New(fmt.Sprintf("invalid part number: %v", part))
	}

	params := &s3.UploadPartInput{
		Bucket:     &fs.bucket,
		Key:        fs.key(*fh.inode.FullName),
		PartNumber: aws.Int64(int64(part)),
		UploadId:   fh.mpuId,
		Body:       buf,
	}

	s3Log.Debug(params)

	resp, err := fs.s3.UploadPart(params)
	if err != nil {
		return mapAwsError(err)
	}

	en := &fh.etags[part-1]

	if *en != nil {
		panic(fmt.Sprintf("etags for part %v already set: %v", part, **en))
	}
	*en = resp.ETag
	return
}

func (fh *FileHandle) mpuPart(fs *Goofys, buf *MBuf, part int) {
	defer func() {
		fh.mpuWG.Done()
	}()

	// maybe wait for CreateMultipartUpload
	if fh.mpuId == nil {
		fh.mpuWG.Wait()
		// initMPU might have errored
		if fh.mpuId == nil {
			return
		}
	}

	err := fh.mpuPartNoSpawn(fs, buf, part)
	if err != nil {
		if fh.lastWriteError == nil {
			fh.lastWriteError = mapAwsError(err)
		}
	}
}

func (fh *FileHandle) waitForCreateMPU(fs *Goofys) (err error) {
	if fh.mpuId == nil {
		fh.mu.Unlock()
		fh.initWrite(fs)
		fh.mpuWG.Wait() // wait for initMPU
		fh.mu.Lock()

		if fh.lastWriteError != nil {
			return fh.lastWriteError
		}
	}

	return
}

func (fh *FileHandle) partSize() uint64 {
	if fh.lastPartId < 1000 {
		return 5 * 1024 * 1024
	} else if fh.lastPartId < 2000 {
		return 25 * 1024 * 1024
	} else {
		return 125 * 1024 * 1024
	}
}

func (fh *FileHandle) WriteFile(fs *Goofys, offset int64, data []byte) (err error) {
	fh.inode.logFuse("WriteFile", offset, len(data))

	fh.mu.Lock()
	defer fh.mu.Unlock()

	if fh.lastWriteError != nil {
		return fh.lastWriteError
	}

	if offset != fh.nextWriteOffset {
		fh.inode.errFuse("WriteFile: only sequential writes supported", fh.nextWriteOffset, offset)
		fh.lastWriteError = fuse.EINVAL
		return fh.lastWriteError
	}

	if offset == 0 {
		fh.poolHandle = fs.bufferPool
		fh.buf = MBuf{}.Init(fh.poolHandle, 0, true)
		fh.dirty = true
	}

	for {
		if fh.buf == nil || fh.buf.Full() {
			fh.buf = MBuf{}.Init(fh.poolHandle, fh.partSize(), true)
		}

		nCopied, _ := fh.buf.Write(data)
		fh.nextWriteOffset += int64(nCopied)

		if fh.buf.Full() {
			// we filled this buffer, upload this part
			err = fh.waitForCreateMPU(fs)
			if err != nil {
				return
			}

			fh.lastPartId++
			part := fh.lastPartId
			buf := fh.buf
			fh.buf = nil
			fh.mpuWG.Add(1)

			go fh.mpuPart(fs, buf, part)
		}

		if nCopied == len(data) {
			break
		}

		data = data[nCopied:]
	}

	fh.inode.Attributes.Size = uint64(fh.nextWriteOffset)

	return
}

type S3ReadBuffer struct {
	s3     *s3.S3
	offset uint64
	size   uint32
	buf    *Buffer
}

func (b S3ReadBuffer) Init(fs *Goofys, fh *FileHandle, offset uint64, size uint32) *S3ReadBuffer {
	b.s3 = fs.s3
	b.offset = offset
	b.size = size

	mbuf := MBuf{}.Init(fh.poolHandle, uint64(size), false)
	if mbuf == nil {
		return nil
	}

	b.buf = Buffer{}.Init(mbuf, func() (io.ReadCloser, error) {
		params := &s3.GetObjectInput{
			Bucket: &fs.bucket,
			Key:    fs.key(*fh.inode.FullName),
		}

		bytes := fmt.Sprintf("bytes=%v-%v", offset, offset+uint64(size)-1)
		params.Range = &bytes

		req, resp := fs.s3.GetObjectRequest(params)

		err := req.Send()
		if err != nil {
			return nil, mapAwsError(err)
		}

		return resp.Body, nil
	})

	return &b
}

func (b *S3ReadBuffer) Read(offset uint64, p []byte) (n int, err error) {
	if b.offset == offset {
		n, err = io.ReadFull(b.buf, p)
		if n != 0 && err == io.ErrUnexpectedEOF {
			err = nil
		}
		if err == nil {
			if uint32(n) > b.size {
				panic(fmt.Sprintf("read more than available %v %v", n, b.size))
			}

			b.offset += uint64(n)
			b.size -= uint32(n)
		}

		return
	} else {
		panic(fmt.Sprintf("not the right buffer, expecting %v got %v, %v left", b.offset, offset, b.size))
		err = errors.New(fmt.Sprintf("not the right buffer, expecting %v got %v", b.offset, offset))
		return
	}
}

func (fh *FileHandle) readFromReadAhead(fs *Goofys, offset uint64, buf []byte) (bytesRead int, err error) {
	var nread int
	for len(fh.buffers) != 0 {
		nread, err = fh.buffers[0].Read(offset+uint64(bytesRead), buf)
		bytesRead += nread
		if err != nil {
			return
		}

		if fh.buffers[0].size == 0 {
			// we've exhausted the first buffer
			fh.buffers[0].buf.Close()
			fh.buffers = fh.buffers[1:]
		}

		buf = buf[nread:]

		if len(buf) == 0 {
			// we've filled the user buffer
			return
		}
	}

	return
}

func (fh *FileHandle) readAhead(fs *Goofys, offset uint64, needAtLeast int) (err error) {
	existingReadahead := uint32(0)
	for _, b := range fh.buffers {
		existingReadahead += b.size
	}

	readAheadAmount := MAX_READAHEAD

	for readAheadAmount-existingReadahead >= READAHEAD_CHUNK {
		off := offset + uint64(existingReadahead)
		remaining := fh.inode.Attributes.Size - off

		// only read up to readahead chunk each time
		size := MinUInt32(readAheadAmount-existingReadahead, READAHEAD_CHUNK)
		// but don't read past the file
		size = uint32(MinUInt64(uint64(size), remaining))

		if size != 0 {
			fh.inode.logFuse("readahead", off, size, existingReadahead)

			readAheadBuf := S3ReadBuffer{}.Init(fs, fh, off, size)
			if readAheadBuf != nil {
				fh.buffers = append(fh.buffers, readAheadBuf)
				existingReadahead += size
			} else {
				if existingReadahead != 0 {
					// don't do more readahead now, but don't fail, cross our
					// fingers that we will be able to allocate the buffers
					// later
					return nil
				} else {
					return syscall.ENOMEM
				}
			}
		}

		if size != READAHEAD_CHUNK {
			// that was the last remaining chunk to readahead
			break
		}
	}

	return nil
}

func (fh *FileHandle) ReadFile(fs *Goofys, offset int64, buf []byte) (bytesRead int, err error) {
	fh.inode.logFuse("ReadFile", offset, len(buf))
	defer func() {
		fh.inode.logFuse("< ReadFile", bytesRead, err)

		if err != nil {
			if bytesRead > 0 || err == io.EOF {
				err = nil
			}
		}
	}()

	fh.mu.Lock()
	defer fh.mu.Unlock()

	nwant := len(buf)
	var nread int

	for bytesRead < nwant && err == nil {
		nread, err = fh.readFile(fs, offset+int64(bytesRead), buf[bytesRead:])
		if nread > 0 {
			bytesRead += nread
		}
	}

	return
}

func (fh *FileHandle) readFile(fs *Goofys, offset int64, buf []byte) (bytesRead int, err error) {
	defer func() {
		if bytesRead > 0 {
			fh.readBufOffset += int64(bytesRead)
			fh.seqReadAmount += uint64(bytesRead)
		}

		fh.inode.logFuse("< readFile", bytesRead, err)
	}()

	if uint64(offset) >= fh.inode.Attributes.Size {
		// nothing to read
		if fh.inode.Invalid {
			err = fuse.ENOENT
		} else if fh.inode.KnownSize == nil {
			err = io.EOF
		} else {
			err = io.EOF
		}
		return
	}

	if fh.poolHandle == nil {
		fh.poolHandle = fs.bufferPool
	}

	if fh.readBufOffset != offset {
		// XXX out of order read, maybe disable prefetching
		fh.inode.logFuse("out of order read", offset, fh.readBufOffset)

		fh.readBufOffset = offset
		fh.seqReadAmount = 0
		if fh.reader != nil {
			fh.reader.Close()
			fh.reader = nil
		}

		if fh.buffers != nil {
			// we misdetected
			fh.numOOORead++
		}

		for _, b := range fh.buffers {
			b.buf.Close()
		}
		fh.buffers = nil
	}

	if !fs.flags.Cheap && fh.seqReadAmount >= uint64(READAHEAD_CHUNK) && fh.numOOORead < 3 {
		if fh.reader != nil {
			fh.inode.logFuse("cutover to the parallel algorithm")
			fh.reader.Close()
			fh.reader = nil
		}

		err = fh.readAhead(fs, uint64(offset), len(buf))
		if err == nil {
			bytesRead, err = fh.readFromReadAhead(fs, uint64(offset), buf)
			return
		} else {
			// fall back to read serially
			fh.inode.logFuse("not enough memory, fallback to serial read")
			fh.seqReadAmount = 0
			for _, b := range fh.buffers {
				b.buf.Close()
			}
			fh.buffers = nil
		}
	}

	bytesRead, err = fh.readFromStream(fs, offset, buf)

	return
}

func (fh *FileHandle) Release() {
	// read buffers
	for _, b := range fh.buffers {
		b.buf.Close()
	}
	fh.buffers = nil

	if fh.reader != nil {
		fh.reader.Close()
	}

	// write buffers
	if fh.poolHandle != nil {
		if fh.buf != nil && fh.buf.buffers != nil {
			if fh.lastWriteError == nil {
				panic("buf not freed but error is nil")
			}

			fh.buf.Free()
			// the other in-flight multipart PUT buffers will be
			// freed when they finish/error out
		}
	}

	fh.inode.mu.Lock()
	defer fh.inode.mu.Unlock()

	delete(fh.inode.fileHandles, fh)
}

func (fh *FileHandle) readFromStream(fs *Goofys, offset int64, buf []byte) (bytesRead int, err error) {
	defer func() {
		if fh.inode.flags.DebugFuse {
			fh.inode.logFuse("< readFromStream", bytesRead)
		}
	}()

	if uint64(offset) >= fh.inode.Attributes.Size {
		// nothing to read
		return
	}

	if fh.reader == nil {
		params := &s3.GetObjectInput{
			Bucket: &fs.bucket,
			Key:    fs.key(*fh.inode.FullName),
		}

		if offset != 0 {
			bytes := fmt.Sprintf("bytes=%v-", offset)
			params.Range = &bytes
		}

		req, resp := fs.s3.GetObjectRequest(params)

		err = req.Send()
		if err != nil {
			return bytesRead, mapAwsError(err)
		}

		fh.reader = resp.Body
	}

	bytesRead, err = fh.reader.Read(buf)
	if err != nil {
		if err != io.EOF {
			fh.inode.logFuse("< readFromStream error", bytesRead, err)
		}
		// always retry error on read
		fh.reader.Close()
		fh.reader = nil
		err = nil
	}

	return
}

func (fh *FileHandle) flushSmallFile(fs *Goofys) (err error) {
	buf := fh.buf
	fh.buf = nil

	if buf == nil {
		panic(fmt.Sprintf("%s size is %d", *fh.inode.Name, fh.nextWriteOffset))
	}

	defer buf.Free()

	params := &s3.PutObjectInput{
		Bucket:       &fs.bucket,
		Key:          fs.key(*fh.inode.FullName),
		Body:         buf,
		StorageClass: &fs.flags.StorageClass,
		ContentType:  fs.getMimeType(*fh.inode.FullName),
	}

	if fs.flags.UseSSE {
		params.ServerSideEncryption = &fs.sseType
		if fs.flags.UseKMS && fs.flags.KMSKeyID != "" {
			params.SSEKMSKeyId = &fs.flags.KMSKeyID
		}
	}

	if fs.flags.ACL != "" {
		params.ACL = &fs.flags.ACL
	}

	fs.replicators.Take(1, true)
	defer fs.replicators.Return(1)

	_, err = fs.s3.PutObject(params)
	if err != nil {
		err = mapAwsError(err)
		fh.lastWriteError = err
	}
	return
}

func (fh *FileHandle) resetToKnownSize() {
	if fh.inode.KnownSize != nil {
		fh.inode.Attributes.Size = *fh.inode.KnownSize
	} else {
		fh.inode.Attributes.Size = 0
		fh.inode.Invalid = true
	}
}

func (fh *FileHandle) FlushFile(fs *Goofys) (err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	fh.inode.logFuse("FlushFile")

	if !fh.dirty || fh.lastWriteError != nil {
		if fh.lastWriteError != nil {
			err = fh.lastWriteError
			fh.resetToKnownSize()
		}
		return
	}

	// abort mpu on error
	defer func() {
		if err != nil {
			if fh.mpuId != nil {
				go func() {
					params := &s3.AbortMultipartUploadInput{
						Bucket:   &fs.bucket,
						Key:      fs.key(*fh.inode.FullName),
						UploadId: fh.mpuId,
					}

					fh.mpuId = nil
					resp, _ := fs.s3.AbortMultipartUpload(params)
					s3Log.Debug(resp)
				}()
			}

			fh.resetToKnownSize()
		} else {
			if fh.dirty {
				// don't unset this if we never actually flushed
				size := fh.inode.Attributes.Size
				fh.inode.KnownSize = &size
				fh.inode.Invalid = false
			}
			fh.dirty = false
		}

		fh.writeInit = sync.Once{}
		fh.nextWriteOffset = 0
		fh.lastPartId = 0
	}()

	if fh.lastPartId == 0 {
		return fh.flushSmallFile(fs)
	}

	fh.mpuWG.Wait()

	if fh.lastWriteError != nil {
		return fh.lastWriteError
	}

	if fh.mpuId == nil {
		return
	}

	nParts := fh.lastPartId
	if fh.buf != nil {
		// upload last part
		nParts++
		err = fh.mpuPartNoSpawn(fs, fh.buf, nParts)
		if err != nil {
			return
		}
	}

	parts := make([]*s3.CompletedPart, nParts)
	for i := 0; i < nParts; i++ {
		parts[i] = &s3.CompletedPart{
			ETag:       fh.etags[i],
			PartNumber: aws.Int64(int64(i + 1)),
		}
	}

	params := &s3.CompleteMultipartUploadInput{
		Bucket:   &fs.bucket,
		Key:      fs.key(*fh.mpuKey),
		UploadId: fh.mpuId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: parts,
		},
	}

	s3Log.Debug(params)

	resp, err := fs.s3.CompleteMultipartUpload(params)
	if err != nil {
		return mapAwsError(err)
	}

	s3Log.Debug(resp)
	fh.mpuId = nil

	if *fh.mpuKey != *fh.inode.FullName {
		// the file was renamed
		err = renameObject(fs, fh.nextWriteOffset, *fh.mpuKey, *fh.inode.FullName)
	}

	return
}

func (parent *Inode) Rename(fs *Goofys, from string, newParent *Inode, to string) (err error) {
	parent.logFuse("Rename", from, newParent.getChildName(to))

	fromFullName := parent.getChildName(from)

	var size int64
	var fromIsDir bool
	var toIsDir bool

	fromIsDir, err = isEmptyDir(fs, fromFullName)
	if err != nil {
		// we don't support renaming a directory that's not empty
		return
	}

	toFullName := newParent.getChildName(to)

	toIsDir, err = isEmptyDir(fs, toFullName)
	if err != nil {
		return
	}

	if fromIsDir && !toIsDir {
		_, err = fs.s3.HeadObject(&s3.HeadObjectInput{Bucket: &fs.bucket, Key: fs.key(toFullName)})
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

	size = int64(-1)
	if fromIsDir {
		fromFullName += "/"
		toFullName += "/"
		size = 0
	}

	err = renameObject(fs, size, fromFullName, toFullName)
	return
}

func renameObject(fs *Goofys, size int64, fromFullName string, toFullName string) (err error) {
	err = fs.copyObjectMaybeMultipart(size, fromFullName, toFullName, nil, nil)
	if err != nil {
		return err
	}

	delParams := &s3.DeleteObjectInput{
		Bucket: &fs.bucket,
		Key:    fs.key(fromFullName),
	}

	_, err = fs.s3.DeleteObject(delParams)
	if err != nil {
		return mapAwsError(err)
	}
	s3Log.Debugf("Deleted %v", delParams)

	return
}

func (inode *Inode) OpenDir() (dh *DirHandle) {
	inode.logFuse("OpenDir")

	parent := inode.Parent
	if parent != nil && inode.flags.TypeCacheTTL != 0 {
		parent.mu.Lock()
		defer parent.mu.Unlock()

		num := len(parent.Children)

		if parent.lastOpenDir == nil && num > 0 && *parent.Children[0].Name == *inode.Name {
			parent.seqOpenDirScore += 1
			// 2.1) if I open a/a, a/'s score is now 2
			// ie: handle the depth first search case
			if parent.seqOpenDirScore >= 2 {
				fuseLog.Debugf("%v in readdir mode", *parent.FullName)
			}
		} else if parent.lastOpenDir != nil && parent.lastOpenDirIdx+1 < num &&
			// we are reading the next one as expected
			*parent.Children[parent.lastOpenDirIdx+1].Name == *inode.Name &&
			// check that inode positions haven't moved
			*parent.Children[parent.lastOpenDirIdx].Name == *parent.lastOpenDir {
			// 2.2) if I open b/, root's score is now 2
			// ie: handle the breath first search case
			parent.seqOpenDirScore++
			parent.lastOpenDirIdx += 1
			if parent.seqOpenDirScore == 2 {
				fuseLog.Debugf("%v in readdir mode", *parent.FullName)
			}
		} else {
			parent.seqOpenDirScore = 0
			parent.lastOpenDirIdx = parent.findChildIdxUnlocked(*inode.Name)
			if parent.lastOpenDirIdx == -1 {
				panic(fmt.Sprintf("%v is not under %v", *inode.Name, *parent.FullName))
			}
		}

		parent.lastOpenDir = inode.Name
		inode.mu.Lock()
		defer inode.mu.Unlock()

		if inode.lastOpenDir == nil {
			// 1) if I open a/, root's score = 1 (a is the first dir),
			// so make a/'s count at 1 too
			inode.seqOpenDirScore = parent.seqOpenDirScore
			if inode.seqOpenDirScore >= 2 {
				fuseLog.Debugf("%v in readdir mode", *inode.FullName)
			}
		}
	}

	dh = NewDirHandle(inode)
	return
}

// Dirents, sorted by name.
type sortedDirents []*DirHandleEntry

func (p sortedDirents) Len() int           { return len(p) }
func (p sortedDirents) Less(i, j int) bool { return *p[i].Name < *p[j].Name }
func (p sortedDirents) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

func (inode *Inode) readDirFromCache(fs *Goofys, offset fuseops.DirOffset) (en *DirHandleEntry, ok bool) {
	inode.mu.Lock()
	defer inode.mu.Unlock()

	if !expired(inode.DirTime, fs.flags.TypeCacheTTL) {
		ok = true

		if int(offset) >= len(inode.Children) {
			return
		}
		child := inode.Children[offset]

		en = &DirHandleEntry{
			Name:       child.Name,
			Inode:      child.Id,
			Offset:     offset + 1,
			Attributes: child.Attributes,
		}
		if inode.isDir() {
			en.Type = fuseutil.DT_Directory
		} else {
			en.Type = fuseutil.DT_File
		}

	}
	return
}

func (dh *DirHandle) listObjectsSlurp(fs *Goofys, prefix string) (resp *s3.ListObjectsOutput, err error) {
	var marker *string
	reqPrefix := prefix
	inode := dh.inode
	if dh.inode.Parent != nil {
		inode = dh.inode.Parent
		reqPrefix = *fs.key(*inode.FullName)
		if len(*inode.FullName) != 0 {
			reqPrefix += "/"
		}
		marker = fs.key(*dh.inode.FullName + "/")
	}

	params := &s3.ListObjectsInput{
		Bucket: &fs.bucket,
		Prefix: &reqPrefix,
		Marker: marker,
	}

	resp, err = fs.s3.ListObjects(params)
	if err != nil {
		s3Log.Errorf("ListObjects %v = %v", params, err)
		return
	}

	num := len(resp.Contents)
	if num == 0 {
		return
	}

	dirs := make(map[*Inode]bool)
	for _, obj := range resp.Contents {
		baseName := (*obj.Key)[len(reqPrefix):]

		slash := strings.Index(baseName, "/")
		if slash != -1 {
			inode.insertSubTree(fs, baseName, obj, dirs)
		}
	}

	for d, sealed := range dirs {
		if d == dh.inode {
			// never seal the current dir because that's
			// handled at upper layer
			continue
		}

		if sealed || !*resp.IsTruncated {
			d.DirTime = time.Now()
		}
	}

	if *resp.IsTruncated {
		obj := resp.Contents[len(resp.Contents)-1]
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
		resp.IsTruncated = aws.Bool(false)
		resp.NextMarker = nil
	}

	return
}

func (dh *DirHandle) listObjects(fs *Goofys, prefix string) (resp *s3.ListObjectsOutput, err error) {
	errSlurpChan := make(chan error, 1)
	slurpChan := make(chan s3.ListObjectsOutput, 1)
	errListChan := make(chan error, 1)
	listChan := make(chan s3.ListObjectsOutput, 1)

	// try to list without delimiter to see if we have slurp up
	// multiple directories
	if dh.Marker == nil &&
		fs.flags.TypeCacheTTL != 0 &&
		(dh.inode.Parent != nil && dh.inode.Parent.seqOpenDirScore >= 2) {
		go func() {
			resp, err := dh.listObjectsSlurp(fs, prefix)
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
		params := &s3.ListObjectsInput{
			Bucket:    &fs.bucket,
			Delimiter: aws.String("/"),
			Marker:    dh.Marker,
			Prefix:    &prefix,
		}

		resp, err := fs.s3.ListObjects(params)
		if err != nil {
			errListChan <- err
		} else {
			listChan <- *resp
		}
	}

	if !fs.flags.Cheap {
		// invoke the fallback in parallel if desired
		listObjectsFlat()
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

func (parent *Inode) addDotAndDotDot(fs *Goofys) {
	en := &DirHandleEntry{
		Name:       aws.String("."),
		Type:       fuseutil.DT_Directory,
		Attributes: &fs.rootAttrs,
		Offset:     1,
	}
	fs.insertInodeFromDirEntry(parent, en)
	en = &DirHandleEntry{
		Name:       aws.String(".."),
		Type:       fuseutil.DT_Directory,
		Attributes: &fs.rootAttrs,
		Offset:     2,
	}
	fs.insertInodeFromDirEntry(parent, en)
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

func (parent *Inode) insertSubTree(fs *Goofys, path string, obj *s3.Object, dirs map[*Inode]bool) {
	slash := strings.Index(path, "/")
	if slash == -1 {
		fs.insertInodeFromDirEntry(parent, objectToDirEntry(fs, obj, path, false))
		sealPastDirs(dirs, parent)
	} else {
		dir := path[:slash]
		path = path[slash+1:]

		if len(path) == 0 {
			inode := fs.insertInodeFromDirEntry(parent, objectToDirEntry(fs, obj, dir, true))
			inode.addDotAndDotDot(fs)

			sealPastDirs(dirs, inode)
		} else {
			// ensure that the potentially implicit dir is added
			en := &DirHandleEntry{
				Name:       &dir,
				Type:       fuseutil.DT_Directory,
				Attributes: &fs.rootAttrs,
				Offset:     1,
			}
			inode := fs.insertInodeFromDirEntry(parent, en)
			// mark this dir but don't seal anything else
			// until we get to the leaf
			dirs[inode] = false

			inode.addDotAndDotDot(fs)
			inode.insertSubTree(fs, path, obj, dirs)
		}
	}
}

func objectToDirEntry(fs *Goofys, obj *s3.Object, name string, isDir bool) (en *DirHandleEntry) {
	if isDir {
		en = &DirHandleEntry{
			Name:       &name,
			Type:       fuseutil.DT_Directory,
			Attributes: &fs.rootAttrs,
		}
	} else {
		en = &DirHandleEntry{
			Name: &name,
			Type: fuseutil.DT_File,
			Attributes: &fuseops.InodeAttributes{
				Size:   uint64(*obj.Size),
				Nlink:  1,
				Mode:   fs.flags.FileMode,
				Atime:  *obj.LastModified,
				Mtime:  *obj.LastModified,
				Ctime:  *obj.LastModified,
				Crtime: *obj.LastModified,
				Uid:    fs.flags.Uid,
				Gid:    fs.flags.Gid,
			},
			ETag:         obj.ETag,
			StorageClass: obj.StorageClass,
		}
	}

	return
}

// LOCKS_REQUIRED(dh.mu)
func (dh *DirHandle) ReadDir(fs *Goofys, offset fuseops.DirOffset) (en *DirHandleEntry, err error) {
	// If the request is for offset zero, we assume that either this is the first
	// call or rewinddir has been called. Reset state.
	if offset == 0 {
		dh.Entries = nil
	}

	en, ok := dh.inode.readDirFromCache(fs, offset)
	if ok {
		return
	}

	if offset == 0 {
		en = &DirHandleEntry{
			Name:       aws.String("."),
			Type:       fuseutil.DT_Directory,
			Attributes: &fs.rootAttrs,
			Offset:     1,
		}
		return
	} else if offset == 1 {
		en = &DirHandleEntry{
			Name:       aws.String(".."),
			Type:       fuseutil.DT_Directory,
			Attributes: &fs.rootAttrs,
			Offset:     2,
		}
		return
	}

	i := int(offset) - dh.BaseOffset - 2
	if i < 0 {
		panic(fmt.Sprintf("invalid offset %v, base=%v", offset, dh.BaseOffset))
	}

	if i >= len(dh.Entries) {
		if dh.Marker != nil {
			// we need to fetch the next page
			dh.Entries = nil
			dh.BaseOffset += i
			i = 0
		}
	}

	if dh.Entries == nil {
		// try not to hold the lock when we make the request
		dh.mu.Unlock()

		prefix := *fs.key(*dh.inode.FullName)
		if len(*dh.inode.FullName) != 0 {
			prefix += "/"
		}

		resp, err := dh.listObjects(fs, prefix)
		if err != nil {
			dh.mu.Lock()
			return nil, mapAwsError(err)
		}

		s3Log.Debug(resp)
		dh.mu.Lock()

		dh.Entries = make([]*DirHandleEntry, 0, len(resp.CommonPrefixes)+len(resp.Contents))

		// this is only returned for non-slurped responses
		for _, dir := range resp.CommonPrefixes {
			// strip trailing /
			dirName := (*dir.Prefix)[0 : len(*dir.Prefix)-1]
			// strip previous prefix
			dirName = dirName[len(*resp.Prefix):]
			if len(dirName) == 0 {
				continue
			}
			en = &DirHandleEntry{
				Name:       &dirName,
				Type:       fuseutil.DT_Directory,
				Attributes: &fs.rootAttrs,
			}

			dh.Entries = append(dh.Entries, en)
		}

		lastDir := ""
		for _, obj := range resp.Contents {
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
				dh.Entries = append(dh.Entries,
					objectToDirEntry(fs, obj, baseName, false))
			} else {
				// this is a slurped up object which
				// was already cached, unless it's a
				// directory right under this dir that
				// we need to return
				dirName := baseName[:slash]
				if dirName != lastDir && lastDir != "" {
					// make a copy so we can take the address
					dir := lastDir
					en := &DirHandleEntry{
						Name:       &dir,
						Type:       fuseutil.DT_Directory,
						Attributes: &fs.rootAttrs,
					}
					dh.Entries = append(dh.Entries, en)
				}
				lastDir = dirName
			}
		}
		if lastDir != "" {
			en := &DirHandleEntry{
				Name:       &lastDir,
				Type:       fuseutil.DT_Directory,
				Attributes: &fs.rootAttrs,
			}
			dh.Entries = append(dh.Entries, en)
		}

		sort.Sort(sortedDirents(dh.Entries))

		// Fix up offset fields.
		for i := 0; i < len(dh.Entries); i++ {
			en := dh.Entries[i]
			// offset is 1 based, also need to account for "." and ".."
			en.Offset = fuseops.DirOffset(i+dh.BaseOffset) + 1 + 2
		}

		if *resp.IsTruncated {
			dh.Marker = resp.NextMarker
		} else {
			dh.Marker = nil
		}
	}

	if i == len(dh.Entries) {
		// we've reached the end
		return nil, nil
	} else if i > len(dh.Entries) {
		return nil, fuse.EINVAL
	}

	return dh.Entries[i], nil
}

func (dh *DirHandle) CloseDir() error {
	return nil
}
