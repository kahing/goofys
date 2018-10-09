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
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"os/user"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/corehandlers"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/kahing/go-xattr"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"
)

// so I don't get complains about unused imports
var ignored = logrus.DebugLevel

func currentUid() uint32 {
	user, err := user.Current()
	if err != nil {
		panic(err)
	}

	uid, err := strconv.ParseUint(user.Uid, 10, 32)
	if err != nil {
		panic(err)
	}

	return uint32(uid)
}

func currentGid() uint32 {
	user, err := user.Current()
	if err != nil {
		panic(err)
	}

	gid, err := strconv.ParseUint(user.Gid, 10, 32)
	if err != nil {
		panic(err)
	}

	return uint32(gid)
}

type GoofysTest struct {
	fs        *Goofys
	ctx       context.Context
	awsConfig *aws.Config
	s3        *s3.S3
	sess      *session.Session
	env       map[string]io.ReadSeeker
}

func Test(t *testing.T) {
	TestingT(t)
}

var _ = Suite(&GoofysTest{})

func logOutput(t *C, tag string, r io.ReadCloser) {
	in := bufio.NewScanner(r)

	for in.Scan() {
		t.Log(tag, in.Text())
	}
}

func waitFor(t *C, addr string) (err error) {
	// wait for it to listen on port
	for i := 0; i < 10; i++ {
		var conn net.Conn
		conn, err = net.Dial("tcp", addr)
		if err == nil {
			// we are done!
			conn.Close()
			return
		} else {
			t.Logf("Cound not connect: %v", err)
			time.Sleep(100 * time.Millisecond)
		}
	}

	return
}

func selectTestConfig(t *C) *aws.Config {
	if hasEnv("AWS") {
		return &aws.Config{
			Region:     aws.String("us-west-2"),
			DisableSSL: aws.Bool(true),
			//LogLevel:         aws.LogLevel(aws.LogDebug | aws.LogDebugWithSigning),
			S3ForcePathStyle: aws.Bool(true),
		}
	} else if hasEnv("GCS") {
		return &aws.Config{
			Region:      aws.String("us-west1"),
			Endpoint:    aws.String("http://storage.googleapis.com"),
			Credentials: credentials.NewSharedCredentials("", os.Getenv("GCS")),
			//LogLevel:         aws.LogLevel(aws.LogDebug | aws.LogDebugWithSigning),
			S3ForcePathStyle: aws.Bool(true),
		}
	} else if hasEnv("MINIO") {
		return &aws.Config{
			Credentials: credentials.NewStaticCredentials("Q3AM3UQ867SPQQA43P2F",
				"zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG", ""),
			Region: aws.String("us-east-1"),
			//LogLevel:         aws.LogLevel(aws.LogDebug | aws.LogDebugWithSigning),
			S3ForcePathStyle: aws.Bool(true),
			Endpoint:         aws.String("https://play.minio.io:9000"),
		}
	} else {
		addr := "127.0.0.1:8080"

		err := waitFor(t, addr)
		t.Assert(err, IsNil)

		return &aws.Config{
			//Credentials: credentials.AnonymousCredentials,
			Credentials:      credentials.NewStaticCredentials("foo", "bar", ""),
			Region:           aws.String("us-west-2"),
			Endpoint:         aws.String("http://" + addr),
			DisableSSL:       aws.Bool(true),
			S3ForcePathStyle: aws.Bool(true),
			MaxRetries:       aws.Int(0),
			//Logger: t,
			//LogLevel: aws.LogLevel(aws.LogDebug),
			//LogLevel: aws.LogLevel(aws.LogDebug | aws.LogDebugWithHTTPBody),
		}
	}
}

func (s *GoofysTest) SetUpSuite(t *C) {
}

func (s *GoofysTest) deleteBucket(t *C) {
	resp, err := s.s3.ListObjects(&s3.ListObjectsInput{Bucket: &s.fs.bucket})
	t.Assert(err, IsNil)

	if hasEnv("GCS") {
		// GCS does not have multi-delete
		var wg sync.WaitGroup

		for _, o := range resp.Contents {
			wg.Add(1)
			key := *o.Key
			go func() {
				_, err = s.s3.DeleteObject(&s3.DeleteObjectInput{
					Bucket: &s.fs.bucket,
					Key:    &key,
				})
				wg.Done()
				t.Assert(err, IsNil)
			}()
		}
		wg.Wait()
	} else {
		num_objs := len(resp.Contents)

		var items s3.Delete
		var objs = make([]*s3.ObjectIdentifier, num_objs)

		for i, o := range resp.Contents {
			objs[i] = &s3.ObjectIdentifier{Key: aws.String(*o.Key)}
		}

		// Add list of objects to delete to Delete object
		items.SetObjects(objs)
		_, err = s.s3.DeleteObjects(&s3.DeleteObjectsInput{Bucket: &s.fs.bucket, Delete: &items})
		t.Assert(err, IsNil)
	}

	s.s3.DeleteBucket(&s3.DeleteBucketInput{Bucket: &s.fs.bucket})
}

func (s *GoofysTest) TearDownSuite(t *C) {
	s.deleteBucket(t)
}

func (s *GoofysTest) setupEnv(t *C, bucket string, env map[string]io.ReadSeeker, public bool) {
	param := s3.CreateBucketInput{
		Bucket: &bucket,
		//ACL: aws.String(s3.BucketCannedACLPrivate),
	}
	if public {
		param.ACL = aws.String("public-read")
	}
	_, err := s.s3.CreateBucket(&param)
	t.Assert(err, IsNil)

	for path, r := range env {
		if r == nil {
			if strings.HasSuffix(path, "/") {
				r = bytes.NewReader([]byte{})
			} else {
				r = bytes.NewReader([]byte(path))
			}
		}

		params := &s3.PutObjectInput{
			Bucket: &bucket,
			Key:    &path,
			Body:   r,
			Metadata: map[string]*string{
				"name": aws.String(path + "+/#%00"),
			},
		}

		_, err := s.s3.PutObject(params)
		t.Assert(err, IsNil)
	}

	// double check
	for path := range env {
		params := &s3.HeadObjectInput{Bucket: &bucket, Key: &path}
		_, err := s.s3.HeadObject(params)
		t.Assert(err, IsNil)
	}

	t.Log("setupEnv done")
}

func (s *GoofysTest) setupDefaultEnv(t *C, public bool) (bucket string) {
	s.env = map[string]io.ReadSeeker{
		"file1":           nil,
		"file2":           nil,
		"dir1/file3":      nil,
		"dir2/dir3/":      nil,
		"dir2/dir3/file4": nil,
		"dir4/":           nil,
		"dir4/file5":      nil,
		"empty_dir/":      nil,
		"empty_dir2/":     nil,
		"zero":            bytes.NewReader([]byte{}),
	}

	bucket = "goofys-test-" + RandStringBytesMaskImprSrc(16)
	s.setupEnv(t, bucket, s.env, public)
	return bucket
}

func (s *GoofysTest) SetUpTest(t *C) {
	s.awsConfig = selectTestConfig(t)
	s.sess = session.New(s.awsConfig)
	s.s3 = s3.New(s.sess)

	if !hasEnv("MINIO") {
		s.s3.Handlers.Sign.Clear()
		s.s3.Handlers.Sign.PushBack(SignV2)
		s.s3.Handlers.Sign.PushBackNamed(corehandlers.BuildContentLengthHandler)
	}

	_, err := s.s3.ListBuckets(nil)
	t.Assert(err, IsNil)

	bucket := s.setupDefaultEnv(t, false)

	s.ctx = context.Background()

	uid, gid := MyUserAndGroup()
	flags := &FlagStorage{
		StorageClass: "STANDARD",
		DirMode:      0700,
		FileMode:     0700,
		Uid:          uint32(uid),
		Gid:          uint32(gid),
	}
	if hasEnv("GCS") {
		flags.Endpoint = "http://storage.googleapis.com"
	}
	s.fs = NewGoofys(context.Background(), bucket, s.awsConfig, flags)
	t.Assert(s.fs, NotNil)
}

func (s *GoofysTest) getRoot(t *C) (inode *Inode) {
	inode = s.fs.inodes[fuseops.RootInodeID]
	t.Assert(inode, NotNil)
	return
}

func (s *GoofysTest) TestGetRootInode(t *C) {
	root := s.getRoot(t)
	t.Assert(root.Id, Equals, fuseops.InodeID(fuseops.RootInodeID))
}

func (s *GoofysTest) TestGetRootAttributes(t *C) {
	_, err := s.getRoot(t).GetAttributes()
	t.Assert(err, IsNil)
}

func (s *GoofysTest) ForgetInode(t *C, inode fuseops.InodeID) {
	err := s.fs.ForgetInode(s.ctx, &fuseops.ForgetInodeOp{Inode: inode})
	t.Assert(err, IsNil)
}

func (s *GoofysTest) LookUpInode(t *C, name string) (in *Inode, err error) {
	parent := s.getRoot(t)

	for {
		idx := strings.Index(name, "/")
		if idx == -1 {
			break
		}

		dirName := name[0:idx]
		name = name[idx+1:]

		lookup := fuseops.LookUpInodeOp{
			Parent: parent.Id,
			Name:   dirName,
		}

		err = s.fs.LookUpInode(nil, &lookup)
		if err != nil {
			return
		}
		parent = s.fs.inodes[lookup.Entry.Child]
	}

	lookup := fuseops.LookUpInodeOp{
		Parent: parent.Id,
		Name:   name,
	}

	err = s.fs.LookUpInode(nil, &lookup)
	if err != nil {
		return
	}
	in = s.fs.inodes[lookup.Entry.Child]
	return
}

func (s *GoofysTest) TestLookUpInode(t *C) {
	_, err := s.LookUpInode(t, "file1")
	t.Assert(err, IsNil)

	_, err = s.LookUpInode(t, "fileNotFound")
	t.Assert(err, Equals, fuse.ENOENT)

	_, err = s.LookUpInode(t, "dir1/file3")
	t.Assert(err, IsNil)

	_, err = s.LookUpInode(t, "dir2/dir3")
	t.Assert(err, IsNil)

	_, err = s.LookUpInode(t, "dir2/dir3/file4")
	t.Assert(err, IsNil)

	_, err = s.LookUpInode(t, "empty_dir")
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestGetInodeAttributes(t *C) {
	inode, err := s.getRoot(t).LookUp("file1")
	t.Assert(err, IsNil)

	attr, err := inode.GetAttributes()
	t.Assert(err, IsNil)
	t.Assert(attr.Size, Equals, uint64(len("file1")))
}

func (s *GoofysTest) readDirFully(t *C, dh *DirHandle) (entries []DirHandleEntry) {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	en, err := dh.ReadDir(fuseops.DirOffset(0))
	t.Assert(err, IsNil)
	t.Assert(en, NotNil)
	t.Assert(*en.Name, Equals, ".")

	en, err = dh.ReadDir(fuseops.DirOffset(1))
	t.Assert(err, IsNil)
	t.Assert(en, NotNil)
	t.Assert(*en.Name, Equals, "..")

	for i := fuseops.DirOffset(2); ; i++ {
		en, err = dh.ReadDir(i)
		t.Assert(err, IsNil)

		if en == nil {
			return
		}

		entries = append(entries, *en)
	}
}

func namesOf(entries []DirHandleEntry) (names []string) {
	for _, en := range entries {
		names = append(names, *en.Name)
	}
	return
}

func (s *GoofysTest) assertEntries(t *C, in *Inode, names []string) {
	dh := in.OpenDir()
	defer dh.CloseDir()

	t.Assert(namesOf(s.readDirFully(t, dh)), DeepEquals, names)
}

func (s *GoofysTest) readDirIntoCache(t *C, inode fuseops.InodeID) {
	openDirOp := fuseops.OpenDirOp{Inode: inode}
	err := s.fs.OpenDir(nil, &openDirOp)
	t.Assert(err, IsNil)

	readDirOp := fuseops.ReadDirOp{
		Inode:  inode,
		Handle: openDirOp.Handle,
		Dst:    make([]byte, 8*1024),
	}

	err = s.fs.ReadDir(nil, &readDirOp)
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestReadDirCacheLookup(t *C) {
	s.fs.flags.StatCacheTTL = 1 * time.Minute
	s.fs.flags.TypeCacheTTL = 1 * time.Minute

	s.readDirIntoCache(t, fuseops.RootInodeID)
	s.disableS3()

	// should be cached so lookup should not need to talk to s3
	entries := []string{"dir1", "dir2", "dir4", "empty_dir", "empty_dir2", "file1", "file2", "zero"}
	for _, en := range entries {
		err := s.fs.LookUpInode(nil, &fuseops.LookUpInodeOp{
			Parent: fuseops.RootInodeID,
			Name:   en,
		})
		t.Assert(err, IsNil)
	}
}

func (s *GoofysTest) TestReadDir(t *C) {
	// test listing /
	dh := s.getRoot(t).OpenDir()
	defer dh.CloseDir()

	s.assertEntries(t, s.getRoot(t), []string{"dir1", "dir2", "dir4", "empty_dir", "empty_dir2", "file1", "file2", "zero"})

	// test listing dir1/
	in, err := s.LookUpInode(t, "dir1")
	t.Assert(err, IsNil)
	s.assertEntries(t, in, []string{"file3"})

	// test listing dir2/
	in, err = s.LookUpInode(t, "dir2")
	t.Assert(err, IsNil)
	s.assertEntries(t, in, []string{"dir3"})

	// test listing dir2/dir3/
	in, err = in.LookUp("dir3")
	t.Assert(err, IsNil)
	s.assertEntries(t, in, []string{"file4"})
}

func (s *GoofysTest) TestReadFiles(t *C) {
	parent := s.getRoot(t)
	dh := parent.OpenDir()
	defer dh.CloseDir()

	var entries []*DirHandleEntry

	dh.mu.Lock()
	for i := fuseops.DirOffset(0); ; i++ {
		en, err := dh.ReadDir(i)
		t.Assert(err, IsNil)

		if en == nil {
			break
		}

		entries = append(entries, en)
	}
	dh.mu.Unlock()

	for _, en := range entries {
		if en.Type == fuseutil.DT_File {
			in, err := parent.LookUp(*en.Name)
			t.Assert(err, IsNil)

			fh, err := in.OpenFile()
			t.Assert(err, IsNil)

			buf := make([]byte, 4096)

			nread, err := fh.ReadFile(0, buf)
			if *en.Name == "zero" {
				t.Assert(nread, Equals, 0)
			} else {
				t.Assert(nread, Equals, len(*en.Name))
				buf = buf[0:nread]
				t.Assert(string(buf), Equals, *en.Name)
			}
		} else {

		}
	}
}

func (s *GoofysTest) TestReadOffset(t *C) {
	root := s.getRoot(t)
	f := "file1"

	in, err := root.LookUp(f)
	t.Assert(err, IsNil)

	fh, err := in.OpenFile()
	t.Assert(err, IsNil)

	buf := make([]byte, 4096)

	nread, err := fh.ReadFile(1, buf)
	t.Assert(err, IsNil)
	t.Assert(nread, Equals, len(f)-1)
	t.Assert(string(buf[0:nread]), DeepEquals, f[1:])

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < 3; i++ {
		off := r.Int31n(int32(len(f)))
		nread, err = fh.ReadFile(int64(off), buf)
		t.Assert(err, IsNil)
		t.Assert(nread, Equals, len(f)-int(off))
		t.Assert(string(buf[0:nread]), DeepEquals, f[off:])
	}
}

func (s *GoofysTest) TestCreateFiles(t *C) {
	fileName := "testCreateFile"

	_, fh := s.getRoot(t).Create(fileName)

	err := fh.FlushFile()
	t.Assert(err, IsNil)

	resp, err := s.s3.GetObject(&s3.GetObjectInput{Bucket: &s.fs.bucket, Key: &fileName})
	t.Assert(err, IsNil)
	t.Assert(*resp.ContentLength, DeepEquals, int64(0))
	defer resp.Body.Close()

	_, err = s.getRoot(t).LookUp(fileName)
	t.Assert(err, IsNil)

	fileName = "testCreateFile2"
	s.testWriteFile(t, fileName, 1, 128*1024)

	inode, err := s.getRoot(t).LookUp(fileName)
	t.Assert(err, IsNil)

	fh, err = inode.OpenFile()
	t.Assert(err, IsNil)

	err = fh.FlushFile()
	t.Assert(err, IsNil)

	resp, err = s.s3.GetObject(&s3.GetObjectInput{Bucket: &s.fs.bucket, Key: &fileName})
	t.Assert(err, IsNil)
	t.Assert(*resp.ContentLength, Equals, int64(1))
	defer resp.Body.Close()
}

func (s *GoofysTest) TestUnlink(t *C) {
	fileName := "file1"

	err := s.getRoot(t).Unlink(fileName)
	t.Assert(err, IsNil)

	// make sure that it's gone from s3
	_, err = s.s3.GetObject(&s3.GetObjectInput{Bucket: &s.fs.bucket, Key: &fileName})
	t.Assert(mapAwsError(err), Equals, fuse.ENOENT)
}

type FileHandleReader struct {
	fs     *Goofys
	fh     *FileHandle
	offset int64
}

func (r *FileHandleReader) Read(p []byte) (nread int, err error) {
	nread, err = r.fh.ReadFile(r.offset, p)
	r.offset += int64(nread)
	return
}

func (r *FileHandleReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		r.offset = offset
	case 1:
		r.offset += offset
	default:
		panic(fmt.Sprintf("unsupported whence: %v", whence))
	}

	return r.offset, nil
}

func (s *GoofysTest) testWriteFile(t *C, fileName string, size int64, write_size int) {
	s.testWriteFileAt(t, fileName, int64(0), size, write_size)
}

func (s *GoofysTest) testWriteFileAt(t *C, fileName string, offset int64, size int64, write_size int) {
	var fh *FileHandle

	if offset == 0 {
		_, fh = s.getRoot(t).Create(fileName)
	} else {
		in, err := s.getRoot(t).LookUp(fileName)
		t.Assert(err, IsNil)

		fh, err = in.OpenFile()
		t.Assert(err, IsNil)
	}

	buf := make([]byte, write_size)
	nwritten := offset

	src := io.LimitReader(&SeqReader{}, size)

	for {
		nread, err := src.Read(buf)
		if err == io.EOF {
			t.Assert(nwritten, Equals, size)
			break
		}
		t.Assert(err, IsNil)

		err = fh.WriteFile(nwritten, buf[:nread])
		t.Assert(err, IsNil)
		nwritten += int64(nread)
	}

	err := fh.FlushFile()
	t.Assert(err, IsNil)

	resp, err := s.s3.HeadObject(&s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: &fileName})
	t.Assert(err, IsNil)
	t.Assert(*resp.ContentLength, DeepEquals, size+offset)

	fr := &FileHandleReader{s.fs, fh, offset}
	diff, err := CompareReader(fr, io.LimitReader(&SeqReader{offset}, size))
	t.Assert(err, IsNil)
	t.Assert(diff, Equals, -1)
	t.Assert(fr.offset, Equals, size)

	fh.Release()
}

func (s *GoofysTest) TestWriteLargeFile(t *C) {
	s.testWriteFile(t, "testLargeFile", 21*1024*1024, 128*1024)
	s.testWriteFile(t, "testLargeFile2", 20*1024*1024, 128*1024)
}

func (s *GoofysTest) TestWriteReplicatorThrottle(t *C) {
	s.fs.replicators = Ticket{Total: 1}.Init()
	s.testWriteFile(t, "testLargeFile", 21*1024*1024, 128*1024)
}

func (s *GoofysTest) TestReadWriteMinimumMemory(t *C) {
	s.fs.bufferPool.maxBuffers = 2
	s.fs.bufferPool.computedMaxbuffers = s.fs.bufferPool.maxBuffers
	s.testWriteFile(t, "testLargeFile", 21*1024*1024, 128*1024)
}

func (s *GoofysTest) TestWriteManyFilesFile(t *C) {
	var files sync.WaitGroup

	for i := 0; i < 21; i++ {
		files.Add(1)
		fileName := "testSmallFile" + strconv.Itoa(i)
		go func() {
			defer files.Done()
			s.testWriteFile(t, fileName, 1, 128*1024)
		}()
	}

	files.Wait()
}

func (s *GoofysTest) testWriteFileNonAlign(t *C) {
	s.testWriteFile(t, "testWriteFileNonAlign", 6*1024*1024, 128*1024+1)
}

func (s *GoofysTest) TestReadRandom(t *C) {
	size := int64(21 * 1024 * 1024)

	s.testWriteFile(t, "testLargeFile", size, 128*1024)
	in, err := s.LookUpInode(t, "testLargeFile")
	t.Assert(err, IsNil)

	fh, err := in.OpenFile()
	t.Assert(err, IsNil)
	fr := &FileHandleReader{s.fs, fh, 0}

	src := rand.NewSource(time.Now().UnixNano())
	truth := &SeqReader{}

	for i := 0; i < 10; i++ {
		offset := src.Int63() % (size / 2)

		fr.Seek(offset, 0)
		truth.Seek(offset, 0)

		// read 5MB+1 from that offset
		nread := int64(5*1024*1024 + 1)
		CompareReader(io.LimitReader(fr, nread), io.LimitReader(truth, nread))
	}
}

func (s *GoofysTest) TestMkDir(t *C) {
	_, err := s.LookUpInode(t, "new_dir/file")
	t.Assert(err, Equals, fuse.ENOENT)

	dirName := "new_dir"
	inode, err := s.getRoot(t).MkDir(dirName)
	t.Assert(err, IsNil)
	t.Assert(*inode.FullName(), Equals, dirName)

	_, err = s.LookUpInode(t, dirName)
	t.Assert(err, IsNil)

	fileName := "file"
	_, fh := inode.Create(fileName)

	err = fh.FlushFile()
	t.Assert(err, IsNil)

	_, err = s.LookUpInode(t, dirName+"/"+fileName)
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestRmDir(t *C) {
	root := s.getRoot(t)

	err := root.RmDir("dir1")
	t.Assert(err, Equals, fuse.ENOTEMPTY)

	err = root.RmDir("dir2")
	t.Assert(err, Equals, fuse.ENOTEMPTY)

	err = root.RmDir("empty_dir")
	t.Assert(err, IsNil)

}

func (s *GoofysTest) TestRenamePreserveMetadata(t *C) {
	root := s.getRoot(t)

	from, to := "file1", "new_file"

	metadata := make(map[string]*string)
	metadata["foo"] = aws.String("bar")

	_, err := s.s3.CopyObject(&s3.CopyObjectInput{
		Bucket:            &s.fs.bucket,
		CopySource:        aws.String(s.fs.bucket + "/" + from),
		Key:               &from,
		Metadata:          metadata,
		MetadataDirective: aws.String(s3.MetadataDirectiveReplace),
	})
	t.Assert(err, IsNil)

	err = root.Rename(from, root, to)
	t.Assert(err, IsNil)

	resp, err := s.s3.HeadObject(&s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: &to})
	t.Assert(err, IsNil)
	t.Assert(resp.Metadata["Foo"], NotNil)
	t.Assert(*resp.Metadata["Foo"], Equals, "bar")
}

func (s *GoofysTest) TestRenameLarge(t *C) {
	s.testWriteFile(t, "large_file", 21*1024*1024, 128*1024)

	root := s.getRoot(t)

	from, to := "large_file", "large_file2"
	err := root.Rename(from, root, to)
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestRenameToExisting(t *C) {
	root := s.getRoot(t)

	// cache these 2 files first
	_, err := s.LookUpInode(t, "file1")
	t.Assert(err, IsNil)

	_, err = s.LookUpInode(t, "file2")
	t.Assert(err, IsNil)

	err = s.fs.Rename(nil, &fuseops.RenameOp{
		OldParent: root.Id,
		NewParent: root.Id,
		OldName:   "file1",
		NewName:   "file2",
	})
	t.Assert(err, IsNil)

	file1 := root.findChild("file1")
	t.Assert(file1, IsNil)

	file2 := root.findChild("file2")
	t.Assert(file2, NotNil)
	t.Assert(*file2.Name, Equals, "file2")
}

func (s *GoofysTest) TestRename(t *C) {
	root := s.getRoot(t)
	from, to := "dir1", "new_dir"
	err := root.Rename(from, root, to)
	t.Assert(err, Equals, fuse.ENOTEMPTY)

	dir2, err := root.LookUp("dir2")
	t.Assert(err, IsNil)

	from, to = "dir3", "new_dir"
	err = dir2.Rename(from, root, to)
	t.Assert(err, Equals, fuse.ENOTEMPTY)

	from, to = "empty_dir", "dir1"
	err = root.Rename(from, root, to)
	t.Assert(err, Equals, fuse.ENOTEMPTY)

	from, to = "empty_dir", "file1"
	err = root.Rename(from, root, to)
	t.Assert(err, Equals, fuse.ENOTDIR)

	from, to = "file1", "empty_dir"
	err = root.Rename(from, root, to)
	t.Assert(err, Equals, syscall.EISDIR)

	from, to = "empty_dir", "new_dir"
	err = root.Rename(from, root, to)
	t.Assert(err, IsNil)

	from, to = "file1", "new_file"
	err = root.Rename(from, root, to)
	t.Assert(err, IsNil)

	_, err = s.s3.HeadObject(&s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: &to})
	t.Assert(err, IsNil)

	_, err = s.s3.HeadObject(&s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: &from})
	t.Assert(mapAwsError(err), Equals, fuse.ENOENT)

	from, to = "file3", "new_file"
	dir, _ := s.LookUpInode(t, "dir1")
	err = dir.Rename(from, root, to)
	t.Assert(err, IsNil)

	_, err = s.s3.HeadObject(&s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: &to})
	t.Assert(err, IsNil)

	_, err = s.s3.HeadObject(&s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: &from})
	t.Assert(mapAwsError(err), Equals, fuse.ENOENT)

	from, to = "no_such_file", "new_file"
	err = root.Rename(from, root, to)
	t.Assert(err, Equals, fuse.ENOENT)

	if !hasEnv("GCS") {
		// not really rename but can be used by rename
		from, to = s.fs.bucket+"/file2", "new_file"
		err = copyObjectMultipart(s.fs, int64(len("file2")), from, to, "", nil, nil)
		t.Assert(err, IsNil)
	}
}

func (s *GoofysTest) TestConcurrentRefDeref(t *C) {
	root := s.getRoot(t)

	lookupOp := fuseops.LookUpInodeOp{
		Parent: root.Id,
		Name:   "file1",
	}

	for i := 0; i < 20; i++ {
		err := s.fs.LookUpInode(nil, &lookupOp)
		t.Assert(err, IsNil)

		var wg sync.WaitGroup

		wg.Add(2)
		go func() {
			// we want to yield to the forget goroutine so that it's run first
			// to trigger this bug
			if i%2 == 0 {
				runtime.Gosched()
			}
			s.fs.LookUpInode(nil, &lookupOp)
			wg.Done()
		}()
		go func() {
			s.fs.ForgetInode(nil, &fuseops.ForgetInodeOp{
				Inode: lookupOp.Entry.Child,
				N:     1,
			})
			wg.Done()
		}()

		wg.Wait()
	}
}

func hasEnv(env string) bool {
	v := os.Getenv(env)

	return !(v == "" || v == "0" || v == "false")
}

func isTravis() bool {
	return hasEnv("TRAVIS")
}

func isCatfs() bool {
	return hasEnv("CATFS")
}

func (s *GoofysTest) mount(t *C, mountPoint string) {
	err := os.MkdirAll(mountPoint, 0700)
	t.Assert(err, IsNil)

	server := fuseutil.NewFileSystemServer(s.fs)

	if isCatfs() {
		s.fs.flags.MountOptions = make(map[string]string)
		s.fs.flags.MountOptions["allow_other"] = ""
	}

	// Mount the file system.
	mountCfg := &fuse.MountConfig{
		FSName:                  s.fs.bucket,
		Options:                 s.fs.flags.MountOptions,
		ErrorLogger:             GetStdLogger(NewLogger("fuse"), logrus.ErrorLevel),
		DisableWritebackCaching: true,
	}

	_, err = fuse.Mount(mountPoint, server, mountCfg)
	t.Assert(err, IsNil)

	if isCatfs() {
		cacheDir := mountPoint + "-cache"
		err := os.MkdirAll(cacheDir, 0700)
		t.Assert(err, IsNil)

		catfs := exec.Command("catfs", "--test", "-ononempty", "--", mountPoint, cacheDir, mountPoint)
		_, err = catfs.Output()
		if err != nil {
			if ee, ok := err.(*exec.ExitError); ok {
				panic(ee.Stderr)
			}
		}

		catfs = exec.Command("catfs", "-ononempty", "--", mountPoint, cacheDir, mountPoint)

		if isTravis() {
			logger := NewLogger("catfs")
			lvl := logrus.InfoLevel
			logger.Formatter.(*LogHandle).Lvl = &lvl
			w := logger.Writer()

			catfs.Stdout = w
			catfs.Stderr = w

			catfs.Env = append(catfs.Env, "RUST_LOG=debug")
		}

		err = catfs.Start()
		t.Assert(err, IsNil)

		time.Sleep(time.Second)
	}
}

func (s *GoofysTest) umount(t *C, mountPoint string) {
	err := fuse.Unmount(mountPoint)
	if err != nil {
		time.Sleep(100 * time.Millisecond)
		err = fuse.Unmount(mountPoint)
		t.Assert(err, IsNil)
	}

	os.Remove(mountPoint)
	if isCatfs() {
		cacheDir := mountPoint + "-cache"
		os.Remove(cacheDir)
	}
}

func (s *GoofysTest) runFuseTest(t *C, mountPoint string, umount bool, cmdArgs ...string) {
	s.mount(t, mountPoint)

	if umount {
		defer s.umount(t, mountPoint)
	}

	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	cmd.Env = append(cmd.Env, os.Environ()...)
	cmd.Env = append(cmd.Env, "TRAVIS=true")
	cmd.Env = append(cmd.Env, "FAST=true")

	if isTravis() {
		logger := NewLogger("test")
		lvl := logrus.InfoLevel
		logger.Formatter.(*LogHandle).Lvl = &lvl
		w := logger.Writer()

		cmd.Stdout = w
		cmd.Stderr = w
	}

	err := cmd.Run()
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestFuse(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket

	s.runFuseTest(t, mountPoint, true, "../test/fuse-test.sh", mountPoint)
}

func (s *GoofysTest) TestFuseWithTTL(t *C) {
	s.fs.flags.StatCacheTTL = 60 * 1000 * 1000 * 1000
	mountPoint := "/tmp/mnt" + s.fs.bucket

	s.runFuseTest(t, mountPoint, true, "../test/fuse-test.sh", mountPoint)
}

func (s *GoofysTest) TestCheap(t *C) {
	s.fs.flags.Cheap = true
	s.TestLookUpInode(t)
	s.TestWriteLargeFile(t)
}

func (s *GoofysTest) TestExplicitDir(t *C) {
	s.fs.flags.ExplicitDir = true
	s.testExplicitDir(t)
}

func (s *GoofysTest) TestExplicitDirAndCheap(t *C) {
	s.fs.flags.ExplicitDir = true
	s.fs.flags.Cheap = true
	s.testExplicitDir(t)
}

func (s *GoofysTest) testExplicitDir(t *C) {

	_, err := s.LookUpInode(t, "file1")
	t.Assert(err, IsNil)

	_, err = s.LookUpInode(t, "fileNotFound")
	t.Assert(err, Equals, fuse.ENOENT)

	// dir1/ doesn't exist so we shouldn't be able to see it
	_, err = s.LookUpInode(t, "dir1/file3")
	t.Assert(err, Equals, fuse.ENOENT)

	_, err = s.LookUpInode(t, "dir4/file5")
	t.Assert(err, IsNil)

	_, err = s.LookUpInode(t, "empty_dir")
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestBenchLs(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket
	s.runFuseTest(t, mountPoint, false, "../bench/bench.sh", "cat", mountPoint, "ls")
}

func (s *GoofysTest) TestBenchCreate(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket

	s.runFuseTest(t, mountPoint, false, "../bench/bench.sh", "cat", mountPoint, "create")
}

func (s *GoofysTest) TestBenchCreateParallel(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket

	s.runFuseTest(t, mountPoint, false, "../bench/bench.sh", "cat", mountPoint, "create_parallel")
}

func (s *GoofysTest) TestBenchIO(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket

	s.runFuseTest(t, mountPoint, false, "../bench/bench.sh", "cat", mountPoint, "io")
}

func (s *GoofysTest) TestBenchFindTree(t *C) {
	s.fs.flags.TypeCacheTTL = 1 * time.Minute
	mountPoint := "/tmp/mnt" + s.fs.bucket

	s.runFuseTest(t, mountPoint, false, "../bench/bench.sh", "cat", mountPoint, "find")
}

func (s *GoofysTest) TestIssue231(t *C) {
	if isTravis() {
		t.Skip("disable in travis, not sure if it has enough memory")
	}
	mountPoint := "/tmp/mnt" + s.fs.bucket
	s.runFuseTest(t, mountPoint, false, "../bench/bench.sh", "cat", mountPoint, "issue231")
}

func (s *GoofysTest) TestChmod(t *C) {
	root := s.getRoot(t)

	lookupOp := fuseops.LookUpInodeOp{
		Parent: root.Id,
		Name:   "file1",
	}

	err := s.fs.LookUpInode(nil, &lookupOp)
	t.Assert(err, IsNil)

	targetMode := os.FileMode(0777)
	setOp := fuseops.SetInodeAttributesOp{Inode: lookupOp.Entry.Child, Mode: &targetMode}

	err = s.fs.SetInodeAttributes(s.ctx, &setOp)
	t.Assert(err, IsNil)
	t.Assert(setOp.Attributes, NotNil)
}

func (s *GoofysTest) TestIssue64(t *C) {
	/*
		mountPoint := "/tmp/mnt" + s.fs.bucket
		log.Level = logrus.DebugLevel

		err := os.MkdirAll(mountPoint, 0700)
		t.Assert(err, IsNil)

		defer os.Remove(mountPoint)

		s.runFuseTest(t, mountPoint, false, "../bench/bench.sh", "cat", mountPoint, "issue64")
	*/
}

func (s *GoofysTest) TestIssue69Fuse(t *C) {
	s.fs.flags.StatCacheTTL = 0

	mountPoint := "/tmp/mnt" + s.fs.bucket

	s.mount(t, mountPoint)

	defer func() {
		err := os.Chdir("/")
		t.Assert(err, IsNil)

		s.umount(t, mountPoint)
	}()

	err := os.Chdir(mountPoint)
	t.Assert(err, IsNil)

	_, err = os.Stat("dir1")
	t.Assert(err, IsNil)

	err = os.Remove("dir1/file3")
	t.Assert(err, IsNil)

	// don't really care about error code, but it should be a PathError
	os.Stat("dir1")
	os.Stat("dir1")
}

func (s *GoofysTest) TestGetMimeType(t *C) {
	// option to use mime type not turned on
	mime := s.fs.getMimeType("foo.css")
	t.Assert(mime, IsNil)

	s.fs.flags.UseContentType = true

	mime = s.fs.getMimeType("foo.css")
	t.Assert(mime, NotNil)
	t.Assert(*mime, Equals, "text/css")

	mime = s.fs.getMimeType("foo")
	t.Assert(mime, IsNil)

	mime = s.fs.getMimeType("foo.")
	t.Assert(mime, IsNil)

	mime = s.fs.getMimeType("foo.unknownExtension")
	t.Assert(mime, IsNil)
}

func (s *GoofysTest) TestPutMimeType(t *C) {
	s.fs.flags.UseContentType = true

	root := s.getRoot(t)
	jpg := "test.jpg"
	jpg2 := "test2.jpg"
	file := "test"

	s.testWriteFile(t, jpg, 0, 0)

	resp, err := s.s3.HeadObject(&s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: &jpg})
	t.Assert(err, IsNil)
	t.Assert(*resp.ContentType, Equals, "image/jpeg")

	err = root.Rename(jpg, root, file)
	t.Assert(err, IsNil)

	resp, err = s.s3.HeadObject(&s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: &file})
	t.Assert(err, IsNil)
	if hasEnv("AWS") {
		t.Assert(*resp.ContentType, Equals, "binary/octet-stream")
	} else if hasEnv("GCS") {
		t.Assert(*resp.ContentType, Equals, "application/octet-stream")
	} else {
		// workaround s3proxy https://github.com/andrewgaul/s3proxy/issues/179
		t.Assert(*resp.ContentType, Equals, "application/unknown")
	}

	err = root.Rename(file, root, jpg2)
	t.Assert(err, IsNil)

	resp, err = s.s3.HeadObject(&s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: &jpg2})
	t.Assert(err, IsNil)
	t.Assert(*resp.ContentType, Equals, "image/jpeg")
}

func (s *GoofysTest) TestBucketPrefixSlash(t *C) {
	s.fs = NewGoofys(context.Background(), s.fs.bucket+":dir2", s.awsConfig, s.fs.flags)
	t.Assert(s.fs.prefix, Equals, "dir2/")

	s.fs = NewGoofys(context.Background(), s.fs.bucket+":dir2///", s.awsConfig, s.fs.flags)
	t.Assert(s.fs.prefix, Equals, "dir2/")
}

func (s *GoofysTest) TestFuseWithPrefix(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket

	s.fs = NewGoofys(context.Background(), s.fs.bucket+":testprefix", s.awsConfig, s.fs.flags)

	s.runFuseTest(t, mountPoint, true, "../test/fuse-test.sh", mountPoint)
}

func (s *GoofysTest) TestRenameCache(t *C) {
	root := s.getRoot(t)
	s.fs.flags.StatCacheTTL = 60 * 1000 * 1000 * 1000

	lookupOp1 := fuseops.LookUpInodeOp{
		Parent: root.Id,
		Name:   "file1",
	}

	lookupOp2 := lookupOp1
	lookupOp2.Name = "newfile"

	err := s.fs.LookUpInode(nil, &lookupOp1)
	t.Assert(err, IsNil)

	err = s.fs.LookUpInode(nil, &lookupOp2)
	t.Assert(err, Equals, fuse.ENOENT)

	renameOp := fuseops.RenameOp{
		OldParent: root.Id,
		NewParent: root.Id,
		OldName:   "file1",
		NewName:   "newfile",
	}

	err = s.fs.Rename(nil, &renameOp)
	t.Assert(err, IsNil)

	lookupOp1.Entry = fuseops.ChildInodeEntry{}
	lookupOp2.Entry = fuseops.ChildInodeEntry{}

	err = s.fs.LookUpInode(nil, &lookupOp1)
	t.Assert(err, Equals, fuse.ENOENT)

	err = s.fs.LookUpInode(nil, &lookupOp2)
	t.Assert(err, IsNil)
}

func (s *GoofysTest) anonymous(t *C) {
	// delete the original bucket
	s.deleteBucket(t)

	bucket := s.setupDefaultEnv(t, true)

	s.fs = NewGoofys(context.Background(), bucket, s.awsConfig, s.fs.flags)
	t.Assert(s.fs, NotNil)

	// should have auto-detected within NewGoofys, but doing this here to ensure
	// we are using anonymous credentials
	s.fs.awsConfig = selectTestConfig(t)
	s.fs.awsConfig.Credentials = credentials.AnonymousCredentials
	s.fs.sess = session.New(s.fs.awsConfig)
	s.fs.s3 = s.fs.newS3()
}

func (s *GoofysTest) disableS3() *s3.S3 {
	time.Sleep(1 * time.Second) // wait for any background goroutines to finish
	s3 := s.fs.s3
	s.fs.s3 = nil
	return s3
}

func (s *GoofysTest) TestWriteAnonymous(t *C) {
	s.anonymous(t)
	s.fs.flags.StatCacheTTL = 1 * time.Minute
	s.fs.flags.TypeCacheTTL = 1 * time.Minute

	fileName := "test"

	createOp := fuseops.CreateFileOp{
		Parent: s.getRoot(t).Id,
		Name:   fileName,
	}

	err := s.fs.CreateFile(s.ctx, &createOp)
	t.Assert(err, IsNil)

	err = s.fs.FlushFile(s.ctx, &fuseops.FlushFileOp{
		Handle: createOp.Handle,
		Inode:  createOp.Entry.Child,
	})
	t.Assert(err, Equals, syscall.EACCES)

	err = s.fs.ReleaseFileHandle(s.ctx, &fuseops.ReleaseFileHandleOp{Handle: createOp.Handle})
	t.Assert(err, IsNil)

	err = s.fs.LookUpInode(s.ctx, &fuseops.LookUpInodeOp{
		Parent: s.getRoot(t).Id,
		Name:   fileName,
	})
	t.Assert(err, Equals, fuse.ENOENT)
	// BUG! the file shouldn't exist, see test below for comment,
	// this behaves as expected only because we are bypassing
	// linux vfs in this test
}

func (s *GoofysTest) TestWriteAnonymousFuse(t *C) {
	s.anonymous(t)
	s.fs.flags.StatCacheTTL = 1 * time.Minute
	s.fs.flags.TypeCacheTTL = 1 * time.Minute

	mountPoint := "/tmp/mnt" + s.fs.bucket

	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	err := ioutil.WriteFile(mountPoint+"/test", []byte(""), 0600)
	t.Assert(err, NotNil)
	pathErr, ok := err.(*os.PathError)
	t.Assert(ok, Equals, true)
	t.Assert(pathErr.Err, Equals, syscall.EACCES)

	_, err = os.Stat(mountPoint + "/test")
	t.Assert(err, IsNil)
	// BUG! the file shouldn't exist, the condition below should hold instead
	// see comment in Goofys.FlushFile
	// pathErr, ok = err.(*os.PathError)
	// t.Assert(ok, Equals, true)
	// t.Assert(pathErr.Err, Equals, fuse.ENOENT)

	_, err = ioutil.ReadFile(mountPoint + "/test")
	t.Assert(err, NotNil)
	pathErr, ok = err.(*os.PathError)
	t.Assert(ok, Equals, true)
	t.Assert(pathErr.Err, Equals, fuse.ENOENT)

	// reading the file and getting ENOENT causes the kernel to
	// invalidate the entry, failing at open is not sufficient, we
	// have to fail at read (which means that if the application
	// uses splice(2) it won't get to us, so this wouldn't work
	_, err = os.Stat(mountPoint + "/test")
	t.Assert(err, NotNil)
	pathErr, ok = err.(*os.PathError)
	t.Assert(ok, Equals, true)
	t.Assert(pathErr.Err, Equals, fuse.ENOENT)
}

func (s *GoofysTest) TestWriteSyncWriteFuse(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket

	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	var f *os.File
	var n int
	var err error

	defer func() {
		if err != nil {
			f.Close()
		}
	}()

	f, err = os.Create(mountPoint + "/TestWriteSyncWrite")
	t.Assert(err, IsNil)

	n, err = f.Write([]byte("hello\n"))
	t.Assert(err, IsNil)
	t.Assert(n, Equals, 6)

	err = f.Sync()
	t.Assert(err, IsNil)

	n, err = f.Write([]byte("world\n"))
	t.Assert(err, IsNil)
	t.Assert(n, Equals, 6)

	err = f.Close()
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestIssue156(t *C) {
	_, err := s.LookUpInode(t, "\xae\x8a-")
	// S3Proxy and aws s3 return different errors
	// https://github.com/andrewgaul/s3proxy/issues/201
	t.Assert(err, NotNil)
}

func (s *GoofysTest) TestIssue162(t *C) {
	params := &s3.PutObjectInput{
		Bucket: &s.fs.bucket,
		Key:    aws.String("dir1/l├â┬╢r 006.jpg"),
		Body:   bytes.NewReader([]byte("foo")),
	}
	_, err := s.s3.PutObject(params)
	t.Assert(err, IsNil)

	dir, err := s.LookUpInode(t, "dir1")
	t.Assert(err, IsNil)

	err = dir.Rename("l├â┬╢r 006.jpg", dir, "myfile.jpg")
	t.Assert(err, IsNil)

	resp, err := s.s3.HeadObject(&s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: aws.String("dir1/myfile.jpg")})
	t.Assert(*resp.ContentLength, Equals, int64(3))
}

func (s *GoofysTest) TestXAttrGet(t *C) {
	file1, err := s.LookUpInode(t, "file1")
	t.Assert(err, IsNil)

	names, err := file1.ListXattr()
	t.Assert(err, IsNil)
	sort.Strings(names)
	t.Assert(names, DeepEquals, []string{"s3.etag", "s3.storage-class", "user.name"})

	_, err = file1.GetXattr("user.foobar")
	t.Assert(xattr.IsNotExist(err), Equals, true)

	value, err := file1.GetXattr("s3.etag")
	t.Assert(err, IsNil)
	// md5sum of "file1"
	t.Assert(string(value), Equals, "\"826e8142e6baabe8af779f5f490cf5f5\"")

	value, err = file1.GetXattr("user.name")
	t.Assert(err, IsNil)
	t.Assert(string(value), Equals, "file1+/#\x00")

	dir1, err := s.LookUpInode(t, "dir1")
	t.Assert(err, IsNil)

	// list dir1 to populate file3 in cache, then get file3's xattr
	lookup := fuseops.LookUpInodeOp{
		Parent: fuseops.RootInodeID,
		Name:   "dir1",
	}
	err = s.fs.LookUpInode(nil, &lookup)
	t.Assert(err, IsNil)

	s.readDirIntoCache(t, lookup.Entry.Child)

	dir1 = s.fs.inodes[lookup.Entry.Child]
	file3 := dir1.findChild("file3")
	t.Assert(file3, NotNil)
	t.Assert(file3.userMetadata, IsNil)

	value, err = file3.GetXattr("s3.etag")
	t.Assert(err, IsNil)
	// md5sum of "dir1/file3"
	t.Assert(string(value), Equals, "\"5cd67e0e59fb85be91a515afe0f4bb24\"")

	emptyDir2, err := s.LookUpInode(t, "empty_dir2")
	t.Assert(err, IsNil)

	names, err = emptyDir2.ListXattr()
	t.Assert(err, IsNil)
	sort.Strings(names)
	t.Assert(names, DeepEquals, []string{"s3.etag", "s3.storage-class", "user.name"})

	emptyDir, err := s.LookUpInode(t, "empty_dir")
	t.Assert(err, IsNil)

	value, err = emptyDir.GetXattr("s3.etag")
	t.Assert(err, IsNil)
	// dir blobs are empty
	t.Assert(string(value), Equals, "\"d41d8cd98f00b204e9800998ecf8427e\"")

	// implicit dir blobs don't have s3.etag at all
	names, err = dir1.ListXattr()
	t.Assert(err, IsNil)
	t.Assert(names, HasLen, 0)

	value, err = dir1.GetXattr("s3.etag")
	t.Assert(err, Equals, syscall.ENODATA)

	// s3proxy doesn't support storage class yet
	if hasEnv("AWS") {
		s.fs.flags.StorageClass = "STANDARD_IA"

		s.testWriteFile(t, "ia", 1, 128*1024)

		ia, err := s.LookUpInode(t, "ia")
		t.Assert(err, IsNil)

		names, err = ia.ListXattr()
		t.Assert(names, DeepEquals, []string{"s3.etag", "s3.storage-class"})

		value, err = ia.GetXattr("s3.storage-class")
		t.Assert(err, IsNil)
		t.Assert(string(value), Equals, "STANDARD_IA")
	}
}

func (s *GoofysTest) TestXAttrGetCached(t *C) {
	s.fs.flags.StatCacheTTL = 1 * time.Minute
	s.fs.flags.TypeCacheTTL = 1 * time.Minute
	s.readDirIntoCache(t, fuseops.RootInodeID)
	s.disableS3()

	in, err := s.LookUpInode(t, "file1")
	t.Assert(err, IsNil)
	t.Assert(in.userMetadata, IsNil)

	_, err = in.GetXattr("s3.etag")
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestXAttrCopied(t *C) {
	root := s.getRoot(t)

	err := root.Rename("file1", root, "file0")
	t.Assert(err, IsNil)

	in, err := s.LookUpInode(t, "file0")
	t.Assert(err, IsNil)

	_, err = in.GetXattr("user.name")
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestXAttrRemove(t *C) {
	in, err := s.LookUpInode(t, "file1")
	t.Assert(err, IsNil)

	_, err = in.GetXattr("user.name")
	t.Assert(err, IsNil)

	err = in.RemoveXattr("user.name")
	t.Assert(err, IsNil)

	_, err = in.GetXattr("user.name")
	t.Assert(err, Equals, syscall.ENODATA)
}

func (s *GoofysTest) TestXAttrSet(t *C) {
	in, err := s.LookUpInode(t, "file1")
	t.Assert(err, IsNil)

	err = in.SetXattr("user.bar", []byte("hello"), xattr.REPLACE)
	t.Assert(err, Equals, syscall.ENODATA)

	err = in.SetXattr("user.bar", []byte("hello"), xattr.CREATE)
	t.Assert(err, IsNil)

	err = in.SetXattr("user.bar", []byte("hello"), xattr.CREATE)
	t.Assert(err, Equals, syscall.EEXIST)

	in, err = s.LookUpInode(t, "file1")
	t.Assert(err, IsNil)

	value, err := in.GetXattr("user.bar")
	t.Assert(err, IsNil)
	t.Assert(string(value), Equals, "hello")

	value = []byte("file1+%/#\x00")

	err = in.SetXattr("user.bar", value, xattr.REPLACE)
	t.Assert(err, IsNil)

	in, err = s.LookUpInode(t, "file1")
	t.Assert(err, IsNil)

	value2, err := in.GetXattr("user.bar")
	t.Assert(err, IsNil)
	t.Assert(value2, DeepEquals, value)

	// setting with flag = 0 always works
	err = in.SetXattr("user.bar", []byte("world"), 0)
	t.Assert(err, IsNil)

	err = in.SetXattr("user.baz", []byte("world"), 0)
	t.Assert(err, IsNil)

	value, err = in.GetXattr("user.bar")
	t.Assert(err, IsNil)

	value2, err = in.GetXattr("user.baz")
	t.Assert(err, IsNil)

	t.Assert(value2, DeepEquals, value)
	t.Assert(string(value2), DeepEquals, "world")
}

func (s *GoofysTest) TestCreateRenameBeforeCloseFuse(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket

	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	from := mountPoint + "/newfile"
	to := mountPoint + "/newfile2"

	fh, err := os.Create(from)
	t.Assert(err, IsNil)
	defer func() {
		// close the file if the test failed so we can unmount
		if fh != nil {
			fh.Close()
		}
	}()

	_, err = fh.WriteString("hello world")
	t.Assert(err, IsNil)

	err = os.Rename(from, to)
	t.Assert(err, IsNil)

	err = fh.Close()
	t.Assert(err, IsNil)
	fh = nil

	_, err = os.Stat(from)
	t.Assert(err, NotNil)
	pathErr, ok := err.(*os.PathError)
	t.Assert(ok, Equals, true)
	t.Assert(pathErr.Err, Equals, fuse.ENOENT)

	content, err := ioutil.ReadFile(to)
	t.Assert(err, IsNil)
	t.Assert(string(content), Equals, "hello world")
}

func (s *GoofysTest) TestRenameBeforeCloseFuse(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket

	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	from := mountPoint + "/newfile"
	to := mountPoint + "/newfile2"

	err := ioutil.WriteFile(from, []byte(""), 0600)
	t.Assert(err, IsNil)

	fh, err := os.OpenFile(from, os.O_WRONLY, 0600)
	t.Assert(err, IsNil)
	defer func() {
		// close the file if the test failed so we can unmount
		if fh != nil {
			fh.Close()
		}
	}()

	_, err = fh.WriteString("hello world")
	t.Assert(err, IsNil)

	err = os.Rename(from, to)
	t.Assert(err, IsNil)

	err = fh.Close()
	t.Assert(err, IsNil)
	fh = nil

	_, err = os.Stat(from)
	t.Assert(err, NotNil)
	pathErr, ok := err.(*os.PathError)
	t.Assert(ok, Equals, true)
	t.Assert(pathErr.Err, Equals, fuse.ENOENT)

	content, err := ioutil.ReadFile(to)
	t.Assert(err, IsNil)
	t.Assert(string(content), Equals, "hello world")
}

func (s *GoofysTest) TestInodeInsert(t *C) {
	root := s.getRoot(t)

	in := NewInode(s.fs, root, aws.String("2"), aws.String("2"))
	in.Attributes = InodeAttributes{}
	root.insertChild(in)
	t.Assert(*root.dir.Children[0].Name, Equals, "2")

	in = NewInode(s.fs, root, aws.String("1"), aws.String("1"))
	in.Attributes = InodeAttributes{}
	root.insertChild(in)
	t.Assert(*root.dir.Children[0].Name, Equals, "1")
	t.Assert(*root.dir.Children[1].Name, Equals, "2")

	in = NewInode(s.fs, root, aws.String("4"), aws.String("4"))
	in.Attributes = InodeAttributes{}
	root.insertChild(in)
	t.Assert(*root.dir.Children[0].Name, Equals, "1")
	t.Assert(*root.dir.Children[1].Name, Equals, "2")
	t.Assert(*root.dir.Children[2].Name, Equals, "4")

	inode := root.findChild("1")
	t.Assert(inode, NotNil)
	t.Assert(*inode.Name, Equals, "1")

	inode = root.findChild("2")
	t.Assert(inode, NotNil)
	t.Assert(*inode.Name, Equals, "2")

	inode = root.findChild("4")
	t.Assert(inode, NotNil)
	t.Assert(*inode.Name, Equals, "4")

	inode = root.findChild("0")
	t.Assert(inode, IsNil)

	inode = root.findChild("3")
	t.Assert(inode, IsNil)

	root.removeChild(root.dir.Children[1])
	root.removeChild(root.dir.Children[0])
	root.removeChild(root.dir.Children[0])
	t.Assert(len(root.dir.Children), Equals, 0)
}

func (s *GoofysTest) TestReadDirSlurpSubtree(t *C) {
	s.fs.flags.TypeCacheTTL = 1 * time.Minute

	s.getRoot(t).dir.seqOpenDirScore = 2
	in, err := s.LookUpInode(t, "dir2")
	t.Assert(err, IsNil)

	s.readDirIntoCache(t, in.Id)

	in, err = s.LookUpInode(t, "dir2/dir3")
	t.Assert(err, IsNil)

	// reading dir2 should cause dir2/dir3 to have cached readdir
	s.disableS3()

	s.assertEntries(t, in, []string{"file4"})
}

func (s *GoofysTest) TestReadDirCached(t *C) {
	s.fs.flags.StatCacheTTL = 1 * time.Minute
	s.fs.flags.TypeCacheTTL = 1 * time.Minute

	s.getRoot(t).dir.seqOpenDirScore = 2
	s.readDirIntoCache(t, fuseops.RootInodeID)
	s.disableS3()

	dh := s.getRoot(t).OpenDir()

	entries := s.readDirFully(t, dh)
	dirs := make([]string, 0)
	files := make([]string, 0)
	noMoreDir := false

	for _, en := range entries {
		if en.Type == fuseutil.DT_Directory {
			t.Assert(noMoreDir, Equals, false)
			dirs = append(dirs, *en.Name)
		} else {
			files = append(files, *en.Name)
			noMoreDir = true
		}
	}

	t.Assert(dirs, DeepEquals, []string{"dir1", "dir2", "dir4", "empty_dir", "empty_dir2"})
	t.Assert(files, DeepEquals, []string{"file1", "file2", "zero"})
}

func (s *GoofysTest) TestReadDirLookUp(t *C) {
	s.getRoot(t).dir.seqOpenDirScore = 2
	for i := 0; i < 10; i++ {
		go s.readDirIntoCache(t, fuseops.RootInodeID)
		go func() {
			lookup := fuseops.LookUpInodeOp{
				Parent: fuseops.RootInodeID,
				Name:   "file1",
			}
			err := s.fs.LookUpInode(nil, &lookup)
			t.Assert(err, IsNil)
		}()
	}
}

func (s *GoofysTest) writeSeekWriteFuse(t *C, file string, fh *os.File, first string, second string, third string) {
	fi, err := os.Stat(file)
	t.Assert(err, IsNil)

	defer func() {
		// close the file if the test failed so we can unmount
		if fh != nil {
			fh.Close()
		}
	}()

	_, err = fh.WriteString(first)
	t.Assert(err, IsNil)

	off, err := fh.Seek(int64(len(second)), 1)
	t.Assert(err, IsNil)
	t.Assert(off, Equals, int64(len(first)+len(second)))

	_, err = fh.WriteString(third)
	t.Assert(err, IsNil)

	off, err = fh.Seek(int64(len(first)), 0)
	t.Assert(err, IsNil)
	t.Assert(off, Equals, int64(len(first)))

	_, err = fh.WriteString(second)
	t.Assert(err, IsNil)

	err = fh.Close()
	t.Assert(err, IsNil)
	fh = nil

	content, err := ioutil.ReadFile(file)
	t.Assert(err, IsNil)
	t.Assert(string(content), Equals, first+second+third)

	fi2, err := os.Stat(file)
	t.Assert(err, IsNil)
	t.Assert(fi.Mode(), Equals, fi2.Mode())
}

func (s *GoofysTest) TestWriteSeekWriteFuse(t *C) {
	if !isCatfs() {
		t.Skip("only works with CATFS=true")
	}

	mountPoint := "/tmp/mnt" + s.fs.bucket
	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	file := mountPoint + "/newfile"

	fh, err := os.Create(file)
	t.Assert(err, IsNil)

	s.writeSeekWriteFuse(t, file, fh, "hello", " ", "world")

	fh, err = os.OpenFile(file, os.O_WRONLY, 0600)
	t.Assert(err, IsNil)

	s.writeSeekWriteFuse(t, file, fh, "", "never", "minding")
}

func (s *GoofysTest) TestDirMtimeCreate(t *C) {
	root := s.getRoot(t)

	attr, _ := root.GetAttributes()
	m1 := attr.Mtime
	time.Sleep(time.Second)

	_, _ = root.Create("foo")
	attr2, _ := root.GetAttributes()
	m2 := attr2.Mtime

	t.Assert(m1.Before(m2), Equals, true)
}

func (s *GoofysTest) TestDirMtimeLs(t *C) {
	root := s.getRoot(t)

	attr, _ := root.GetAttributes()
	m1 := attr.Mtime
	time.Sleep(time.Second)

	params := &s3.PutObjectInput{
		Bucket: &s.fs.bucket,
		Key:    aws.String("newfile"),
		Body:   bytes.NewReader([]byte("foo")),
	}
	_, err := s.s3.PutObject(params)
	t.Assert(err, IsNil)

	s.readDirIntoCache(t, fuseops.RootInodeID)

	attr2, _ := root.GetAttributes()
	m2 := attr2.Mtime

	t.Assert(m1.Before(m2), Equals, true)
}

func (s *GoofysTest) TestRenameOverwrite(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket
	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	file := mountPoint + "/newfile"
	rename := mountPoint + "/file1"

	fh, err := os.Create(file)
	t.Assert(err, IsNil)

	err = fh.Close()
	t.Assert(err, IsNil)

	err = os.Rename(file, rename)
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestRead403(t *C) {
	s.fs.flags.StatCacheTTL = 1 * time.Minute
	s.fs.flags.TypeCacheTTL = 1 * time.Minute

	// cache the inode first so we don't get 403 when we lookup
	in, err := s.LookUpInode(t, "file1")
	t.Assert(err, IsNil)

	fh, err := in.OpenFile()
	t.Assert(err, IsNil)

	s.fs.awsConfig.Credentials = credentials.AnonymousCredentials
	s.fs.sess = session.New(s.fs.awsConfig)
	s.fs.s3 = s.fs.newS3()

	// fake enable read-ahead
	fh.seqReadAmount = uint64(READAHEAD_CHUNK)

	buf := make([]byte, 5)

	_, err = fh.ReadFile(0, buf)
	t.Assert(err, Equals, syscall.EACCES)

	// now that the S3 GET has failed, try again, see
	// https://github.com/kahing/goofys/pull/243
	_, err = fh.ReadFile(0, buf)
	t.Assert(err, Equals, syscall.EACCES)
}

func (s *GoofysTest) TestRmdirWithDiropen(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket
	s.fs.flags.StatCacheTTL = 1 * time.Minute
	s.fs.flags.TypeCacheTTL = 1 * time.Minute

	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	err := os.MkdirAll(mountPoint+"/dir2/dir4", 0700)
	t.Assert(err, IsNil)
	err = os.MkdirAll(mountPoint+"/dir2/dir5", 0700)
	t.Assert(err, IsNil)

	//1, open dir5
	dir := mountPoint + "/dir2/dir5"
	fh, err := os.Open(dir)
	t.Assert(err, IsNil)
	defer fh.Close()

	cmd1 := exec.Command("ls", mountPoint+"/dir2")
	//out, err := cmd.Output()
	out1, err1 := cmd1.Output()
	if err1 != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			panic(ee.Stderr)
		}
	}
	t.Assert(string(out1), DeepEquals, ""+"dir3\n"+"dir4\n"+"dir5\n")

	//2, rm -rf dir5
	cmd := exec.Command("rm", "-rf", dir)
	_, err = cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			panic(ee.Stderr)
		}
	}

	//3,  readdir dir2
	fh1, err := os.Open(mountPoint + "/dir2")
	t.Assert(err, IsNil)
	defer func() {
		// close the file if the test failed so we can unmount
		if fh1 != nil {
			fh1.Close()
		}
	}()

	names, err := fh1.Readdirnames(0)
	t.Assert(err, IsNil)
	t.Assert(names, DeepEquals, []string{"dir3", "dir4"})

	cmd = exec.Command("ls", mountPoint+"/dir2")
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			panic(ee.Stderr)
		}
	}

	t.Assert(string(out), DeepEquals, ""+"dir3\n"+"dir4\n")

	err = fh1.Close()
	t.Assert(err, IsNil)

	// 4,reset env
	err = fh.Close()
	t.Assert(err, IsNil)

	err = os.RemoveAll(mountPoint + "/dir2/dir4")
	t.Assert(err, IsNil)

}

func (s *GoofysTest) TestDirMTime(t *C) {
	s.fs.flags.StatCacheTTL = 1 * time.Minute
	s.fs.flags.TypeCacheTTL = 1 * time.Minute
	// enable cheap to ensure GET dir/ will come back before LIST dir/
	s.fs.flags.Cheap = true

	root := s.getRoot(t)
	t.Assert(time.Time{}.Before(root.Attributes.Mtime), Equals, true)

	dir1, err := s.LookUpInode(t, "dir1")
	t.Assert(err, IsNil)

	attr1, _ := dir1.GetAttributes()
	m1 := attr1.Mtime
	// dir1 doesn't have a dir blob, so should take root's mtime
	t.Assert(m1, Equals, root.Attributes.Mtime)

	time.Sleep(2 * time.Second)

	dir2, err := dir1.MkDir("dir2")
	t.Assert(err, IsNil)

	attr2, _ := dir2.GetAttributes()
	m2 := attr2.Mtime
	t.Assert(m1.Add(2*time.Second).Before(m2), Equals, true)

	// dir1 didn't have an explicit mtime, so it should update now
	// that we did a mkdir inside it
	attr1, _ = dir1.GetAttributes()
	m1 = attr1.Mtime
	t.Assert(m1, Equals, m2)

	// simulate forget inode so we will retrieve the inode again
	dir1.removeChild(dir2)

	dir2, err = dir1.LookUp("dir2")
	t.Assert(err, IsNil)

	// the new time comes from S3 which only has seconds
	// granularity
	attr2, _ = dir2.GetAttributes()
	t.Assert(m2, Not(Equals), attr2.Mtime)
	t.Assert(root.Attributes.Mtime.Add(time.Second).Before(attr2.Mtime), Equals, true)

	// different dir2
	dir2, err = s.LookUpInode(t, "dir2")
	t.Assert(err, IsNil)

	attr2, _ = dir2.GetAttributes()
	m2 = attr2.Mtime

	// this fails because we are listing dir/, which means we
	// don't actually see the dir blob dir2/dir3/ (it's returned
	// as common prefix), so we can't get dir3's mtime
	if false {
		// dir2/dir3/ exists and has mtime
		s.readDirIntoCache(t, dir2.Id)
		dir3, err := s.LookUpInode(t, "dir2/dir3")
		t.Assert(err, IsNil)

		attr3, _ := dir3.GetAttributes()
		// setupDefaultEnv is before mounting
		t.Assert(attr3.Mtime.Before(m2), Equals, true)
	}

	time.Sleep(time.Second)

	params := &s3.PutObjectInput{
		Bucket: &s.fs.bucket,
		Key:    aws.String("dir2/newfile"),
		Body:   bytes.NewReader([]byte("foo")),
	}
	_, err = s.s3.PutObject(params)
	t.Assert(err, IsNil)

	s.readDirIntoCache(t, dir2.Id)

	newfile, err := dir2.LookUp("newfile")
	t.Assert(err, IsNil)

	attr2New, _ := dir2.GetAttributes()
	// mtime should reflect that of the latest object
	// GCS can return nano second resolution so truncate to second for compare
	t.Assert(attr2New.Mtime.Unix(), Equals, newfile.Attributes.Mtime.Unix())
	t.Assert(m2.Before(attr2New.Mtime), Equals, true)
}

func (s *GoofysTest) TestDirMTimeNoTTL(t *C) {
	// enable cheap to ensure GET dir/ will come back before LIST dir/
	s.fs.flags.Cheap = true

	dir2, err := s.LookUpInode(t, "dir2")
	t.Assert(err, IsNil)

	attr2, _ := dir2.GetAttributes()
	m2 := attr2.Mtime

	// dir2/dir3/ exists and has mtime
	s.readDirIntoCache(t, dir2.Id)
	dir3, err := s.LookUpInode(t, "dir2/dir3")
	t.Assert(err, IsNil)

	attr3, _ := dir3.GetAttributes()
	// setupDefaultEnv is before mounting
	t.Assert(attr3.Mtime.Before(m2), Equals, true)
}

func (s *GoofysTest) TestIssue326(t *C) {
	root := s.getRoot(t)
	_, err := root.MkDir("folder@name.something")
	t.Assert(err, IsNil)
	_, err = root.MkDir("folder#1#")
	t.Assert(err, IsNil)

	s.readDirIntoCache(t, root.Id)
	t.Assert(*root.dir.Children[7].Name, Equals, "folder#1#")
	t.Assert(*root.dir.Children[8].Name, Equals, "folder@name.something")
}
