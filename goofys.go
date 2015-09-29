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
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
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
	nameToID map[string]fuseops.InodeID 

	nextHandleID fuseops.HandleID
	dirHandles map[fuseops.HandleID]*DirHandle
	nameToDir map[string]*DirHandle

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
	fs.nextInodeID = fuseops.RootInodeID + 1
	fs.inodes = make(map[fuseops.InodeID]*Inode)
	fs.inodes[fuseops.RootInodeID] = &Inode{
		Name: aws.String(""),
		FullName: aws.String(""),
		Id: fuseops.RootInodeID,
		Attributes: fs.rootAttrs,
	}
	fs.nameToID = make(map[string]fuseops.InodeID)

	fs.nextHandleID = 1
	fs.dirHandles = make(map[fuseops.HandleID]*DirHandle)
	fs.nameToDir = make(map[string]*DirHandle)

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


func (fs *Goofys) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) (err error) {

	if op.Inode == fuseops.RootInodeID {
		op.Attributes = fs.rootAttrs
		op.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
	} else {
		fs.mu.Lock()
		defer fs.mu.Unlock()

		inode, ok := fs.inodes[op.Inode]
		if !ok {
			log.Printf("GetInodeAttributes: %v %v\n", op.Inode, op)
		} else {
			log.Printf("GetInodeAttributes: %v %v\n", op.Inode, *inode.FullName)
			op.Attributes = inode.Attributes
			op.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)
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
		log.Printf("LookUpInode %v %v", *name, err)
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


// returned inode has nil Id
func (fs *Goofys) LookUpInodeMaybeDir(name string, fullName string) (inode *Inode, err error) {
	errObjectChan := make(chan error, 1)
	objectChan := make(chan s3.HeadObjectOutput, 1)
	errDirChan := make(chan error, 1)
	dirChan := make(chan s3.ListObjectsOutput, 1)

	go fs.LookUpInodeNotDir(&fullName, objectChan, errObjectChan)
	go fs.LookUpInodeDir(&fullName, dirChan, errDirChan)

	notFound := false

	for {
		select {
		case resp := <- objectChan:
			// XXX/TODO if both object and object/ exists, return dir
			inode = &Inode{ Name: &name, FullName: &fullName }
			inode.Attributes = fuseops.InodeAttributes{
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
				inode = &Inode{ Name: &name, FullName: &fullName }
				inode.Attributes = fs.rootAttrs
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

	fs.mu.Lock();
	parent := fs.getInodeOrDie(op.Parent)

	var fullName string
	if op.Parent == fuseops.RootInodeID {
		fullName = op.Name
	} else {
		fullName = fmt.Sprintf("%v/%v", *parent.FullName, op.Name)
	}

	log.Printf("> LookUpInode: %v\n", fullName)

	var inode *Inode
	id, ok := fs.nameToID[fullName]
	if ok {
		defer fs.mu.Unlock()

		inode = fs.inodes[id]
		op.Entry.Child = inode.Id
		op.Entry.Attributes = inode.Attributes
		op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)

		log.Printf("< LookUpInode: %v from nameToID\n", fullName)
		return
	}

	dh, ok := fs.nameToDir[*parent.Name]
	if ok {
		defer fs.mu.Unlock()
		fs.nextInodeID++
		inode = &Inode{ Name: &op.Name, FullName: &fullName, Id: fs.nextInodeID - 1 }

		if dh.IsDir(&op.Name) {
			log.Printf("< LookUpInode: %v dir\n", fullName)

			inode.Attributes = fs.rootAttrs
		} else {
			log.Printf("< LookUpInode: %v from dh.NameToEntry\n", fullName)

			inode.Attributes = dh.NameToEntry[op.Name]
		}
	} else {
		fs.mu.Unlock()

		inode, err = fs.LookUpInodeMaybeDir(op.Name, fullName)
		if err != nil {
			return err
		}
		log.Printf("< LookUpInode: %v from LookUpInodeMaybeDir\n", fullName)

		fs.mu.Lock()
		defer fs.mu.Unlock()

		fs.nextInodeID++
		inode.Id = fs.nextInodeID - 1
	}

	fs.inodes[inode.Id] = inode
	fs.nameToID[fullName] = inode.Id
	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.Attributes
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)

	return
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *Goofys) ForgetInode(
	ctx context.Context,
	op *fuseops.ForgetInodeOp) (err error) {

	fs.mu.Lock()
	defer fs.mu.Unlock()

	inode, ok := fs.inodes[op.Inode]
	if !ok {
		panic(fmt.Sprintf("%v not found", op.Inode))
	}
	name := *inode.FullName
	log.Printf("ForgetInode %v=%v", name, op.Inode)

	delete(fs.inodes, op.Inode)
	delete(fs.nameToID, name)

	return
}

func (fs *Goofys) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Make sure the inode still exists and is a directory. If not, something has
	// screwed up because the VFS layer shouldn't have let us forget the inode
	// before opening it.
	// Allocate a handle.
	handleID := fs.nextHandleID
	fs.nextHandleID++

	in, ok := fs.inodes[op.Inode]
	if (!ok) {
		panic(fmt.Sprintf("Could not find inode: %v", op.Inode))
	}
	// XXX/is this a dir?

	dirName := in.FullName

	log.Printf("OpenDir: %v", *dirName)

	dh := &DirHandle{ Name: in.Name, FullName: dirName }
	dh.NameToEntry = make(map[string]fuseops.InodeAttributes)
	fs.dirHandles[handleID] = dh
	fs.nameToDir[*dirName] = dh

	op.Handle = handleID

	return
}

func makeDirEntry(name *string, t fuseutil.DirentType) fuseutil.Dirent {
	return fuseutil.Dirent{ Name: *name, Type: t, Inode: fuseops.RootInodeID + 1 }
}

// Dirents, sorted by name.
type sortedDirents []fuseutil.Dirent

func (p sortedDirents) Len() int           { return len(p) }
func (p sortedDirents) Less(i, j int) bool { return p[i].Name < p[j].Name }
func (p sortedDirents) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }


// LOCKS_EXCLUDED(fs.mu)
func (fs *Goofys) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) (err error) {

	// Find the handle.
	fs.mu.Lock()
	dh := fs.dirHandles[op.Handle]
	inode := fs.inodes[op.Inode]
	fs.mu.Unlock()

	if dh == nil || inode == nil {
		panic(fmt.Sprintf("dh=%v inode=%v", dh, inode))
	}

	if *dh.Name != *inode.Name {
		panic(fmt.Sprintf("%v != %v", *dh.Name, *inode.Name))
	}

	log.Println("> ReadDir", *dh.Name, op.Offset)

	// If the request is for offset zero, we assume that either this is the first
	// call or rewinddir has been called. Reset state.
	if op.Offset == 0 {
		dh.Entries = nil
	}

	i := int(op.Offset) - dh.BaseOffset
	if i >= len(dh.Entries) {
		if dh.Marker != nil {
			dh.Entries = nil
			dh.BaseOffset += i
			i = 0
		}
	}

	if dh.BaseOffset > 5000 {
		// XXX prevent infinite loop, raise the limit later
		panic("too many results")
	}

	if dh.Entries == nil {
		prefix := *dh.FullName
		if len(prefix) != 0 {
			prefix += "/"
		}

		params := &s3.ListObjectsInput{
			Bucket:       &fs.bucket,
			Delimiter:    aws.String("/"),
			Marker:       dh.Marker,
			Prefix:       &prefix,
			//MaxKeys:      aws.Int64(10),
		}

		resp, err := fs.s3.ListObjects(params)
		if err != nil {
			return mapAwsError(err)
		}

		log.Println(resp)

		dh.Entries = make([]fuseutil.Dirent, 0, len(resp.CommonPrefixes) + len(resp.Contents))
		
		for _, dir := range resp.CommonPrefixes {
			// strip trailing /
			dirName := (*dir.Prefix)[0:len(*dir.Prefix)-1]
			// strip previous prefix
			dirName = dirName[len(*params.Prefix):]
			dh.Entries = append(dh.Entries, makeDirEntry(&dirName, fuseutil.DT_Directory))
			dh.NameToEntry[dirName] = fs.rootAttrs
		}

		for _, obj := range resp.Contents {
			baseName := (*obj.Key)[len(prefix):]
			if len(baseName) == 0 {
				// this is a directory blob
				continue
			}
			dh.Entries = append(dh.Entries, makeDirEntry(&baseName, fuseutil.DT_File))
			dh.NameToEntry[baseName] = fuseops.InodeAttributes{
				Size: uint64(*obj.Size),
				Nlink: 1,
				Mode: 0644,
				Atime: *obj.LastModified,
				Mtime: *obj.LastModified,
				Ctime: *obj.LastModified,
				Crtime: *obj.LastModified,
				Uid:  fs.uid,
				Gid:  fs.gid,
			}
		}

		sort.Sort(sortedDirents(dh.Entries))

		// Fix up offset fields.
		for i := 0; i < len(dh.Entries); i++ {
			en := &dh.Entries[i]
			en.Offset = fuseops.DirOffset(i + dh.BaseOffset) + 1
		}


		if *resp.IsTruncated {
			// do something with resp.NextMarker
			dh.Marker = resp.NextMarker
		} else {
			dh.Marker = nil
		}
	}

	if i == len(dh.Entries) {
		// we've reached the end
		return
	} else if i > len(dh.Entries) {
		return fuse.EINVAL
	}


	for ; i < len(dh.Entries); i++ {
		e := &dh.Entries[i]
		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], *e)
		if n == 0 {
			break
		}
		log.Printf("< ReadDir %v %v", *dh.Name, e.Name)

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
	log.Printf("ReleaseDirHandle %v", *dh.FullName)

	delete(fs.dirHandles, op.Handle)
	delete(fs.nameToDir, *dh.FullName)

	return
}


func (fs *Goofys) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Find the inode.
	in := fs.getInodeOrDie(op.Inode)

	// Allocate a handle.
	handleID := fs.nextHandleID
	fs.nextHandleID++

	fs.fileHandles[handleID] = NewFileHandle(in)

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

	end := op.Offset + int64(len(op.Dst)) - 1
	bytes := fmt.Sprintf("bytes=%v-%v", op.Offset, end)
	toRead := len(op.Dst)

	log.Printf("> ReadFile %v %v", *fh.FullName, bytes)

	params := &s3.GetObjectInput{
		Bucket:                     &fs.bucket,
		Key:                        fh.FullName,
		Range:                      &bytes,
	}

	resp, err := fs.s3.GetObject(params)
	if err != nil {
		return mapAwsError(err)
	}

	toRead = int(*resp.ContentLength)

	for toRead > 0 {
		buf := op.Dst[op.BytesRead : op.BytesRead + int(toRead)]

		nread, err := resp.Body.Read(buf)
		op.BytesRead += nread
		toRead -= nread

		if err != nil {
			if err != io.EOF {
				return err
			} else {
				break;
			}
		}
	}

	log.Printf("< ReadFile %v %v = %v", *fh.FullName, bytes, op.BytesRead)

	return
}

func (fs *Goofys) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) (err error) {

	fs.mu.Lock()
	fh := fs.fileHandles[op.Handle]
	fs.mu.Unlock()

	log.Printf("FlushFile %v", *fh.FullName)

	// XXX lock the file handle
	if fh.Dirty {
		params := &s3.PutObjectInput{
			Bucket: &fs.bucket,
			Key: fh.FullName,
			Body: bytes.NewReader(fh.Buf),
		}

		resp, err := fs.s3.PutObject(params)

		log.Println(resp)

		if err == nil {
			fh.Dirty = false
		} else {
			return mapAwsError(err)
		}
	}

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
	defer fs.mu.Unlock()

	parent := fs.getInodeOrDie(op.Parent)

	var fullName string
	if op.Parent == fuseops.RootInodeID {
		fullName = op.Name
	} else {
		fullName = fmt.Sprintf("%v/%v", *parent.FullName, op.Name)
	}

	log.Printf("> CreateFile: %v\n", fullName)

	fs.nextInodeID += 1
	nextInode := fs.nextInodeID - 1

	now := time.Now()
	inode := &Inode{ Name: &op.Name, FullName: &fullName, Id: nextInode }
	inode.Attributes = fuseops.InodeAttributes{
		Size: 0,
		Nlink: 1,
		Mode: op.Mode,
		Atime: now,
		Mtime: now,
		Ctime: now,
		Crtime: now,
		Uid:  fs.uid,
		Gid:  fs.gid,
	}

	fs.inodes[inode.Id] = inode
	fs.nameToID[fullName] = inode.Id
	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.Attributes
	op.Entry.AttributesExpiration = time.Now().Add(365 * 24 * time.Hour)

	// Allocate a handle.
	handleID := fs.nextHandleID
	fs.nextHandleID++

	fh := NewFileHandle(inode)
	fh.Dirty = true
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

	inode := fs.getInodeOrDie(op.Inode)
	fh, ok := fs.fileHandles[op.Handle]
	if !ok {
		panic(fmt.Sprintf("WriteFile: can't find handle %v", op.Handle))
	}

	log.Printf("> WriteFile %v off=%v len=%v", *fh.FullName, op.Offset, len(op.Data))
	const MaxInt = int(^uint(0) >> 1)

	if (op.Offset + int64(len(op.Data)) > int64(MaxInt)) {
		return fuse.EINVAL
	}

	last := int(op.Offset) + len(op.Data)
	if last > cap(fh.Buf) {
		log.Printf(">> WriteFile: enlarging buffer: %v -> %v", cap(fh.Buf), last * 2)
		tmp := make([]byte, len(op.Data), last * 2)
		copy(tmp, fh.Buf)
		fh.Buf = tmp
	}

	if last > len(fh.Buf) {
		fh.Buf = fh.Buf[0 : last]
		inode.Attributes.Size = uint64(last)
	}

	copy(fh.Buf[op.Offset : last], op.Data)
	fh.Dirty = true

	return
}

func (fs *Goofys) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) (err error) {

	fs.mu.Lock()
	defer fs.mu.Unlock()

	parent := fs.getInodeOrDie(op.Parent)
	var fullName string
	if op.Parent == fuseops.RootInodeID {
		fullName = op.Name
	} else {
		fullName = fmt.Sprintf("%v/%v", *parent.FullName, op.Name)
	}

	params := &s3.DeleteObjectInput{
		Bucket:       &fs.bucket,
		Key:          &fullName,
	}

	resp, err := fs.s3.DeleteObject(params)
	if err != nil {
		return mapAwsError(err)
	}

	log.Println(resp)

	return
}
