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

func NewGoofys(bucket string, uid uint32, gid uint32) *Goofys {
	// Set up the basic struct.
	fs := &Goofys{
		bucket: bucket,
		uid:    uid,
		gid:    gid,
		umask:  0122,
	}

	fs.s3 = s3.New(&aws.Config{Region: aws.String("us-west-2")})
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
				fmt.Printf("code=%v request=%v\n", reqErr.StatusCode(), reqErr.RequestID())
				return reqErr
			}
		} else {
			// Generic AWS Error with Code, Message, and original error (if any) 
			fmt.Printf("code=%v msg=%v, err=%v\n", awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			return awsErr
		}
	} else {
		return err
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
		fullName = fmt.Sprintf("%v/%v", *parent.Name, op.Name)
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

		params := &s3.HeadObjectInput{ Bucket: &fs.bucket, Key: &op.Name }
		resp, err := fs.s3.HeadObject(params)
		if err != nil {
			log.Printf("LookUpInode %v %v", op.Name, err)
			return mapAwsError(err)
		}

		log.Println(resp)

		fs.mu.Lock()
		defer fs.mu.Unlock()

		fs.nextInodeID++
		inode = &Inode{ Name: &op.Name, FullName: &fullName, Id: fs.nextInodeID - 1 }
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

	name := *fs.inodes[op.Inode].FullName
	log.Printf("ForgetInode %v", name)

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
		panic(fmt.Sprintf("%v != %v", dh.Name, inode.Name))
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
		}
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
		}

		resp, err := fs.s3.ListObjects(params)
		if err != nil {
			return mapAwsError(err)
		}

		log.Println(resp)

		dh.Entries = make([]fuseutil.Dirent, 0, len(resp.CommonPrefixes) + len(resp.Contents))
		
		for _, dir := range resp.CommonPrefixes {
			dirName := (*dir.Prefix)[0:len(*dir.Prefix)-1]
			dh.Entries = append(dh.Entries, makeDirEntry(&dirName, fuseutil.DT_Directory))
			dh.NameToEntry[dirName] = fs.rootAttrs
		}

		for _, obj := range resp.Contents {
			baseName := (*obj.Key)[len(prefix):]
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


		if *resp.IsTruncated && dh.Marker == nil {
			// do something with resp.NextMarker
			dh.Marker = resp.NextMarker
		}
	}

	if i > len(dh.Entries) {
		err = fuse.EINVAL
		return
	}


	for ; i < len(dh.Entries); i++ {
		e := &dh.Entries[i]
		log.Printf("< ReadDir %v %v", *dh.Name, e.Name)
		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], *e)
		if n == 0 {
			dh.Marker = &e.Name
			break
		}

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

	// everything is readonly for now
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
