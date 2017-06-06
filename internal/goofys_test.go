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
			t.Log("Cound not connect: %v", err)
			time.Sleep(1 * time.Second)
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
}

func (s *GoofysTest) TearDownSuite(t *C) {
}

func (s *GoofysTest) setupEnv(t *C, bucket string, env map[string]io.ReadSeeker) {
	_, err := s.s3.CreateBucket(&s3.CreateBucketInput{
		Bucket: &bucket,
		//ACL: aws.String(s3.BucketCannedACLPrivate),
	})
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

func (s *GoofysTest) setupDefaultEnv(t *C) (bucket string) {
	s.env = map[string]io.ReadSeeker{
		"file1":           nil,
		"file2":           nil,
		"dir1/file3":      nil,
		"dir2/dir3/":      nil,
		"dir2/dir3/file4": nil,
		"empty_dir/":      nil,
		"zero":            bytes.NewReader([]byte{}),
	}

	bucket = RandStringBytesMaskImprSrc(16)
	s.setupEnv(t, bucket, s.env)
	return bucket
}

func (s *GoofysTest) SetUpTest(t *C) {
	bucket := s.setupDefaultEnv(t)

	s.ctx = context.Background()

	uid, gid := MyUserAndGroup()
	flags := &FlagStorage{
		StorageClass: "STANDARD",
		DirMode:      0700,
		FileMode:     0700,
		Uid:          uint32(uid),
		Gid:          uint32(gid),
	}
	s.fs = NewGoofys(bucket, s.awsConfig, flags)
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
	_, err := s.getRoot(t).GetAttributes(s.fs)
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

		parent, err = parent.LookUp(s.fs, dirName)
		if err != nil {
			return
		}
	}

	in, err = parent.LookUp(s.fs, name)
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
	inode, err := s.getRoot(t).LookUp(s.fs, "file1")
	t.Assert(err, IsNil)

	attr, err := inode.GetAttributes(s.fs)
	t.Assert(err, IsNil)
	t.Assert(attr.Size, Equals, uint64(len("file1")))
}

func (s *GoofysTest) readDirFully(t *C, dh *DirHandle) (entries []fuseutil.Dirent) {
	dh.mu.Lock()
	defer dh.mu.Unlock()

	en, err := dh.ReadDir(s.fs, fuseops.DirOffset(0))
	t.Assert(err, IsNil)
	t.Assert(en.Name, Equals, ".")

	en, err = dh.ReadDir(s.fs, fuseops.DirOffset(1))
	t.Assert(err, IsNil)
	t.Assert(en.Name, Equals, "..")

	for i := fuseops.DirOffset(2); ; i++ {
		en, err = dh.ReadDir(s.fs, i)
		t.Assert(err, IsNil)

		if en == nil {
			return
		}

		entries = append(entries, *en)
	}
}

func namesOf(entries []fuseutil.Dirent) (names []string) {
	for _, en := range entries {
		names = append(names, en.Name)
	}
	return
}

func (s *GoofysTest) assertEntries(t *C, in *Inode, names []string) {
	dh := in.OpenDir()
	defer dh.CloseDir()

	t.Assert(namesOf(s.readDirFully(t, dh)), DeepEquals, names)
}

func (s *GoofysTest) TestReadDir(t *C) {
	// test listing /
	dh := s.getRoot(t).OpenDir()
	defer dh.CloseDir()

	s.assertEntries(t, s.getRoot(t), []string{"dir1", "dir2", "empty_dir", "file1", "file2", "zero"})

	// test listing dir1/
	in, err := s.LookUpInode(t, "dir1")
	t.Assert(err, IsNil)
	s.assertEntries(t, in, []string{"file3"})

	// test listing dir2/
	in, err = s.LookUpInode(t, "dir2")
	t.Assert(err, IsNil)
	s.assertEntries(t, in, []string{"dir3"})

	// test listing dir2/dir3/
	in, err = in.LookUp(s.fs, "dir3")
	t.Assert(err, IsNil)
	s.assertEntries(t, in, []string{"file4"})
}

func (s *GoofysTest) TestReadFiles(t *C) {
	parent := s.getRoot(t)
	dh := parent.OpenDir()
	defer dh.CloseDir()

	var entries []*fuseutil.Dirent

	dh.mu.Lock()
	for i := fuseops.DirOffset(0); ; i++ {
		en, err := dh.ReadDir(s.fs, i)
		t.Assert(err, IsNil)

		if en == nil {
			break
		}

		entries = append(entries, en)
	}
	dh.mu.Unlock()

	for _, en := range entries {
		if en.Type == fuseutil.DT_File {
			in, err := parent.LookUp(s.fs, en.Name)
			t.Assert(err, IsNil)

			fh := in.OpenFile(s.fs)
			buf := make([]byte, 4096)

			nread, err := fh.ReadFile(s.fs, 0, buf)
			if en.Name == "zero" {
				t.Assert(nread, Equals, 0)
			} else {
				t.Assert(nread, Equals, len(en.Name))
				buf = buf[0:nread]
				t.Assert(string(buf), Equals, en.Name)
			}
		} else {

		}
	}
}

func (s *GoofysTest) TestReadOffset(t *C) {
	root := s.getRoot(t)
	f := "file1"

	in, err := root.LookUp(s.fs, f)
	t.Assert(err, IsNil)

	fh := in.OpenFile(s.fs)

	buf := make([]byte, 4096)

	nread, err := fh.ReadFile(s.fs, 1, buf)
	t.Assert(err, IsNil)
	t.Assert(nread, Equals, len(f)-1)
	t.Assert(string(buf[0:nread]), DeepEquals, f[1:])

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < 3; i++ {
		off := r.Int31n(int32(len(f)))
		nread, err = fh.ReadFile(s.fs, int64(off), buf)
		t.Assert(err, IsNil)
		t.Assert(nread, Equals, len(f)-int(off))
		t.Assert(string(buf[0:nread]), DeepEquals, f[off:])
	}
}

func (s *GoofysTest) TestCreateFiles(t *C) {
	fileName := "testCreateFile"

	_, fh := s.getRoot(t).Create(s.fs, fileName)

	err := fh.FlushFile(s.fs)
	t.Assert(err, IsNil)

	resp, err := s.s3.GetObject(&s3.GetObjectInput{Bucket: &s.fs.bucket, Key: &fileName})
	t.Assert(err, IsNil)
	t.Assert(*resp.ContentLength, DeepEquals, int64(0))
	defer resp.Body.Close()

	_, err = s.getRoot(t).LookUp(s.fs, fileName)
	t.Assert(err, IsNil)

	fileName = "testCreateFile2"
	s.testWriteFile(t, fileName, 1, 128*1024)

	inode, err := s.getRoot(t).LookUp(s.fs, fileName)
	t.Assert(err, IsNil)

	fh = inode.OpenFile(s.fs)
	err = fh.FlushFile(s.fs)
	t.Assert(err, IsNil)

	resp, err = s.s3.GetObject(&s3.GetObjectInput{Bucket: &s.fs.bucket, Key: &fileName})
	t.Assert(err, IsNil)
	t.Assert(*resp.ContentLength, Equals, int64(1))
	defer resp.Body.Close()
}

func (s *GoofysTest) TestUnlink(t *C) {
	fileName := "file1"

	err := s.getRoot(t).Unlink(s.fs, fileName)
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
	nread, err = r.fh.ReadFile(r.fs, r.offset, p)
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
		_, fh = s.getRoot(t).Create(s.fs, fileName)
	} else {
		in, err := s.getRoot(t).LookUp(s.fs, fileName)
		t.Assert(err, IsNil)

		fh = in.OpenFile(s.fs)
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

		err = fh.WriteFile(s.fs, nwritten, buf[:nread])
		t.Assert(err, IsNil)
		nwritten += int64(nread)
	}

	err := fh.FlushFile(s.fs)
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

	fh := in.OpenFile(s.fs)
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
	inode, err := s.getRoot(t).MkDir(s.fs, dirName)
	t.Assert(err, IsNil)
	t.Assert(*inode.FullName, Equals, dirName)

	_, err = s.LookUpInode(t, dirName)
	t.Assert(err, IsNil)

	fileName := "file"
	_, fh := inode.Create(s.fs, fileName)

	err = fh.FlushFile(s.fs)
	t.Assert(err, IsNil)

	_, err = s.LookUpInode(t, dirName+"/"+fileName)
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestRmDir(t *C) {
	root := s.getRoot(t)

	err := root.RmDir(s.fs, "dir1")
	t.Assert(err, Equals, fuse.ENOTEMPTY)

	err = root.RmDir(s.fs, "dir2")
	t.Assert(err, Equals, fuse.ENOTEMPTY)

	err = root.RmDir(s.fs, "empty_dir")
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

	err = root.Rename(s.fs, from, root, to)
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
	err := root.Rename(s.fs, from, root, to)
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestRename(t *C) {
	root := s.getRoot(t)

	from, to := "dir1", "new_dir"
	err := root.Rename(s.fs, from, root, to)
	t.Assert(err, Equals, fuse.ENOTEMPTY)

	dir2, err := root.LookUp(s.fs, "dir2")
	t.Assert(err, IsNil)

	from, to = "dir3", "new_dir"
	err = dir2.Rename(s.fs, from, root, to)
	t.Assert(err, Equals, fuse.ENOTEMPTY)

	from, to = "empty_dir", "dir1"
	err = root.Rename(s.fs, from, root, to)
	t.Assert(err, Equals, fuse.ENOTEMPTY)

	from, to = "empty_dir", "file1"
	err = root.Rename(s.fs, from, root, to)
	t.Assert(err, Equals, fuse.ENOTDIR)

	from, to = "file1", "empty_dir"
	err = root.Rename(s.fs, from, root, to)
	t.Assert(err, Equals, syscall.EISDIR)

	from, to = "empty_dir", "new_dir"
	err = root.Rename(s.fs, from, root, to)
	t.Assert(err, IsNil)

	from, to = "file1", "new_file"
	err = root.Rename(s.fs, from, root, to)
	t.Assert(err, IsNil)

	_, err = s.s3.HeadObject(&s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: &to})
	t.Assert(err, IsNil)

	_, err = s.s3.HeadObject(&s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: &from})
	t.Assert(mapAwsError(err), Equals, fuse.ENOENT)

	from, to = "file3", "new_file"
	dir, _ := s.LookUpInode(t, "dir1")
	err = dir.Rename(s.fs, from, root, to)
	t.Assert(err, IsNil)

	_, err = s.s3.HeadObject(&s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: &to})
	t.Assert(err, IsNil)

	_, err = s.s3.HeadObject(&s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: &from})
	t.Assert(mapAwsError(err), Equals, fuse.ENOENT)

	from, to = "no_such_file", "new_file"
	err = root.Rename(s.fs, from, root, to)
	t.Assert(err, Equals, fuse.ENOENT)

	// not really rename but can be used by rename
	from, to = s.fs.bucket+"/file2", "new_file"
	err = s.fs.copyObjectMultipart(int64(len(from)), from, to, "", nil)
	t.Assert(err, IsNil)
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
	prefix := env + "="
	for _, e := range os.Environ() {
		if strings.HasPrefix(e, prefix) {
			return true
		}
	}

	return false
}

func isTravis() bool {
	return hasEnv("TRAVIS")
}

func (s *GoofysTest) mount(t *C, mountPoint string) {
	server := fuseutil.NewFileSystemServer(s.fs)

	// Mount the file system.
	mountCfg := &fuse.MountConfig{
		FSName:                  s.fs.bucket,
		Options:                 s.fs.flags.MountOptions,
		ErrorLogger:             GetStdLogger(NewLogger("fuse"), logrus.ErrorLevel),
		DisableWritebackCaching: true,
	}

	_, err := fuse.Mount(mountPoint, server, mountCfg)
	t.Assert(err, IsNil)
}

func (s *GoofysTest) umount(t *C, mountPoint string) {
	err := fuse.Unmount(mountPoint)
	if err != nil {
		time.Sleep(1 * time.Second)
		err = fuse.Unmount(mountPoint)
		t.Assert(err, IsNil)
	}

	os.Remove(mountPoint)
}

func (s *GoofysTest) runFuseTest(t *C, mountPoint string, umount bool, cmdArgs ...string) {
	s.mount(t, mountPoint)

	if umount {
		defer func() {
			err := fuse.Unmount(mountPoint)
			t.Assert(err, IsNil)
		}()
	}

	cmd := exec.Command(cmdArgs[0], cmdArgs[1:]...)
	cmd.Env = append(cmd.Env, os.Environ()...)
	cmd.Env = append(cmd.Env, "TRAVIS=true")
	cmd.Env = append(cmd.Env, "FAST=true")

	if isTravis() {
		logger := NewLogger("test")
		lvl := logrus.InfoLevel
		logger.Formatter.(*logHandle).lvl = &lvl
		w := logger.Writer()

		cmd.Stdout = w
		cmd.Stderr = w
	}

	err := cmd.Run()
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestFuse(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket

	err := os.MkdirAll(mountPoint, 0700)
	t.Assert(err, IsNil)

	defer os.Remove(mountPoint)

	s.runFuseTest(t, mountPoint, true, "../test/fuse-test.sh", mountPoint)
}

func (s *GoofysTest) TestCheap(t *C) {
	s.fs.flags.Cheap = true
	s.TestLookUpInode(t)
	s.TestWriteLargeFile(t)
}

func (s *GoofysTest) TestBenchLs(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket

	err := os.MkdirAll(mountPoint, 0700)
	t.Assert(err, IsNil)

	defer os.Remove(mountPoint)

	s.runFuseTest(t, mountPoint, false, "../bench/bench.sh", "cat", mountPoint, "ls")
}

func (s *GoofysTest) TestBenchCreate(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket

	err := os.MkdirAll(mountPoint, 0700)
	t.Assert(err, IsNil)

	defer os.Remove(mountPoint)

	s.runFuseTest(t, mountPoint, false, "../bench/bench.sh", "cat", mountPoint, "create")
}

func (s *GoofysTest) TestBenchCreateParallel(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket

	err := os.MkdirAll(mountPoint, 0700)
	t.Assert(err, IsNil)

	defer os.Remove(mountPoint)

	s.runFuseTest(t, mountPoint, false, "../bench/bench.sh", "cat", mountPoint, "create_parallel")
}

func (s *GoofysTest) TestBenchIO(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket

	err := os.MkdirAll(mountPoint, 0700)
	t.Assert(err, IsNil)

	defer os.Remove(mountPoint)

	s.runFuseTest(t, mountPoint, false, "../bench/bench.sh", "cat", mountPoint, "md5")
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

func (s *GoofysTest) TestIssue69(t *C) {
	s.fs.flags.StatCacheTTL = 0

	mountPoint := "/tmp/mnt" + s.fs.bucket

	err := os.MkdirAll(mountPoint, 0700)
	t.Assert(err, IsNil)

	s.mount(t, mountPoint)

	defer func() {
		err := os.Chdir("/")
		t.Assert(err, IsNil)

		err = fuse.Unmount(mountPoint)
		t.Assert(err, IsNil)
	}()

	err = os.Chdir(mountPoint)
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

	err = root.Rename(s.fs, jpg, root, file)
	t.Assert(err, IsNil)

	resp, err = s.s3.HeadObject(&s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: &file})
	t.Assert(err, IsNil)
	if hasEnv("AWS") {
		t.Assert(*resp.ContentType, Equals, "binary/octet-stream")
	} else {
		// workaround s3proxy https://github.com/andrewgaul/s3proxy/issues/179
		t.Assert(*resp.ContentType, Equals, "application/unknown")
	}

	err = root.Rename(s.fs, file, root, jpg2)
	t.Assert(err, IsNil)

	resp, err = s.s3.HeadObject(&s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: &jpg2})
	t.Assert(err, IsNil)
	t.Assert(*resp.ContentType, Equals, "image/jpeg")
}

func (s *GoofysTest) TestBucketPrefixSlash(t *C) {
	s.fs = NewGoofys(s.fs.bucket+":dir2", s.awsConfig, s.fs.flags)
	t.Assert(s.fs.prefix, Equals, "dir2/")

	s.fs = NewGoofys(s.fs.bucket+":dir2///", s.awsConfig, s.fs.flags)
	t.Assert(s.fs.prefix, Equals, "dir2/")
}

func (s *GoofysTest) TestFuseWithPrefix(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket

	s.fs = NewGoofys(s.fs.bucket+":testprefix", s.awsConfig, s.fs.flags)

	err := os.MkdirAll(mountPoint, 0700)
	t.Assert(err, IsNil)

	defer os.Remove(mountPoint)

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

func (s *GoofysTest) TestUnlinkCache(t *C) {
	root := s.getRoot(t)
	s.fs.flags.StatCacheTTL = 60 * 1000 * 1000 * 1000

	lookupOp := fuseops.LookUpInodeOp{
		Parent: root.Id,
		Name:   "file1",
	}

	err := s.fs.LookUpInode(nil, &lookupOp)
	t.Assert(err, IsNil)

	unlinkOp := fuseops.UnlinkOp{Parent: root.Id, Name: "file1"}

	err = s.fs.Unlink(nil, &unlinkOp)
	t.Assert(err, IsNil)

	err = s.fs.LookUpInode(nil, &lookupOp)
	t.Assert(err, Equals, fuse.ENOENT)
}

func (s *GoofysTest) anonymous(t *C) {
	_, err := s.fs.s3.PutBucketAcl(&s3.PutBucketAclInput{
		ACL:    aws.String("public-read"),
		Bucket: &s.fs.bucket,
	})
	t.Assert(err, IsNil)

	s.fs.awsConfig = selectTestConfig(t)
	s.fs.awsConfig.Credentials = credentials.AnonymousCredentials
	s.fs.sess = session.New(s.fs.awsConfig)
	s.fs.s3 = s.fs.newS3()

	// wait for bucket acl to take effect
	for {
		time.Sleep(1 * time.Second)
		params := &s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: aws.String("file1")}
		_, err := s.s3.HeadObject(params)

		if err == nil {
			break
		}
	}
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

	err = s.fs.FlushFile(s.ctx, &fuseops.FlushFileOp{Handle: createOp.Handle})
	t.Assert(err, Equals, syscall.EACCES)

	err = s.fs.ReleaseFileHandle(s.ctx, &fuseops.ReleaseFileHandleOp{Handle: createOp.Handle})
	t.Assert(err, IsNil)

	err = s.fs.LookUpInode(s.ctx, &fuseops.LookUpInodeOp{
		Parent: s.getRoot(t).Id,
		Name:   fileName,
	})
	t.Assert(err, Equals, fuse.ENOENT)
}

func (s *GoofysTest) TestWriteAnonymousFuse(t *C) {
	s.anonymous(t)
	s.fs.flags.StatCacheTTL = 1 * time.Minute
	s.fs.flags.TypeCacheTTL = 1 * time.Minute

	mountPoint := "/tmp/mnt" + s.fs.bucket

	err := os.MkdirAll(mountPoint, 0700)
	t.Assert(err, IsNil)

	defer os.Remove(mountPoint)

	s.mount(t, mountPoint)
	defer func() {
		err := fuse.Unmount(mountPoint)
		t.Assert(err, IsNil)
	}()

	err = ioutil.WriteFile(mountPoint+"/test", []byte(""), 0600)
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

	// reading the file and getting ENOENT causes the kernel to invalidate the entry
	_, err = os.Stat(mountPoint + "/test")
	t.Assert(err, NotNil)
	pathErr, ok = err.(*os.PathError)
	t.Assert(ok, Equals, true)
	t.Assert(pathErr.Err, Equals, fuse.ENOENT)
}

func (s *GoofysTest) TestWriteSyncWrite(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket

	err := os.MkdirAll(mountPoint, 0700)
	t.Assert(err, IsNil)

	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	var f *os.File
	var n int

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

	err = dir.Rename(s.fs, "l├â┬╢r 006.jpg", dir, "myfile.jpg")
	t.Assert(err, IsNil)

	resp, err := s.s3.HeadObject(&s3.HeadObjectInput{Bucket: &s.fs.bucket, Key: aws.String("dir1/myfile.jpg")})
	t.Assert(*resp.ContentLength, Equals, int64(3))
}
