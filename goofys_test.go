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
	"bufio"
	"bytes"
	"net"
	"io"
	"math/rand"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	"github.com/minio/minio/pkg/auth"
	"github.com/minio/minio/pkg/server"
	"github.com/minio/minio/pkg/server/api"

	. "gopkg.in/check.v1"
)

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
	fs *Goofys
	ctx context.Context
	awsConfig *aws.Config
	s3 *s3.S3
	env map[string]io.ReadSeeker
}

type S3Proxy struct {
	jar string
	config string
	cmd *exec.Cmd
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

func (s *GoofysTest) waitFor(t *C, addr string) (err error) {
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

func (s *GoofysTest) setupMinio(t *C, addr string) (accessKey string, secretKey string) {
	accessKeyID, perr := auth.GenerateAccessKeyID()
	t.Assert(perr, IsNil)
	secretAccessKey, perr := auth.GenerateSecretAccessKey()
	t.Assert(perr, IsNil)

	accessKey = string(accessKeyID)
	secretKey = string(secretAccessKey)

	authConf := &auth.Config{}
	authConf.Users = make(map[string]*auth.User)
	authConf.Users[string(accessKeyID)] = &auth.User{
		Name:            "testuser",
		AccessKeyID:     accessKey,
		SecretAccessKey: secretKey,
	}
	auth.SetAuthConfigPath(filepath.Join(t.MkDir(), "users.json"))
	perr = auth.SaveConfig(authConf)
	t.Assert(perr, IsNil)

	go server.Start(api.Config{ Address: addr })

	err := s.waitFor(t, addr)
	t.Assert(err, IsNil)

	return
}

func (s *GoofysTest) SetUpSuite(t *C) {
	//addr := "play.minio.io:9000"
	addr := "127.0.0.1:9000"

	accessKey, secretKey := s.setupMinio(t, addr)

	s.awsConfig = &aws.Config{
		//Credentials: credentials.AnonymousCredentials,
		Credentials: credentials.NewStaticCredentials(accessKey, secretKey, ""),
		Region: aws.String("milkyway"),//aws.String("us-west-2"),
		Endpoint: aws.String(addr),
		DisableSSL: aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
		MaxRetries: aws.Int(0),
		Logger: t,
		LogLevel: aws.LogLevel(aws.LogDebug),
		//LogLevel: aws.LogLevel(aws.LogDebug | aws.LogDebugWithHTTPBody),
	}
	s.s3 = s3.New(s.awsConfig)

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
			r = bytes.NewReader([]byte(path))
		}

		params := &s3.PutObjectInput{
			Bucket: &bucket,
			Key: &path,
			Body: r,
		}


		_, err := s.s3.PutObject(params)
		t.Assert(err, IsNil)
	}

	// double check
	for path := range env {
		params := &s3.HeadObjectInput{ Bucket: &bucket, Key: &path }
		_, err := s.s3.HeadObject(params)
		t.Assert(err, IsNil)
	}

	t.Log("setupEnv done")
}


// from https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
func RandStringBytesMaskImprSrc(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	const (
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)
	src := rand.NewSource(time.Now().UnixNano())
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

func (s *GoofysTest) setupDefaultEnv(t *C) (bucket string) {
	s.env = map[string]io.ReadSeeker{
		"file1": nil,
		"file2": nil,
		"dir1/file3": nil,
		"dir2/dir3/file4": nil,
		"empty_dir/": nil,
	}

	bucket = RandStringBytesMaskImprSrc(16)
	s.setupEnv(t, bucket, s.env)
	return bucket
}

func (s *GoofysTest) SetUpTest(t *C) {
	bucket := s.setupDefaultEnv(t)

	s.fs = NewGoofys(bucket, s.awsConfig, currentUid(), currentGid())
	s.ctx = context.Background()
}

func (s *GoofysTest) TestGetRootInode(t *C) {
	root := s.fs.getInodeOrDie(fuseops.RootInodeID)
	t.Assert(root.Id, Equals, fuseops.InodeID(fuseops.RootInodeID))
}

func (s *GoofysTest) TestGetRootAttributes(t *C) {
	err := s.fs.GetInodeAttributes(s.ctx, &fuseops.GetInodeAttributesOp{
		Inode: fuseops.RootInodeID,
	})
	t.Assert(err, IsNil)
}

func (s *GoofysTest) ForgetInode(t *C, inode fuseops.InodeID) {
	err := s.fs.ForgetInode(s.ctx, &fuseops.ForgetInodeOp{ Inode: inode })
	t.Assert(err, IsNil)
}

func (s *GoofysTest) LookUpInode(t *C, dir string, name string) (fuseops.InodeID, error) {
	var parent fuseops.InodeID
	if len(dir) == 0 {
		parent = fuseops.RootInodeID
	} else {
		var err error
		idx := strings.LastIndex(dir, "/")
		if idx == -1 {
			parent, err = s.LookUpInode(t, "", dir)
		} else {
			dirName := dir[0:idx]
			baseName := dir[idx:]
			parent, err = s.LookUpInode(t, dirName, baseName)
		}

		if err != nil {
			return 0, err
		}
		defer s.ForgetInode(t, parent)
	}

	op := &fuseops.LookUpInodeOp{
		Parent: parent,
		Name: name,
	}
	err := s.fs.LookUpInode(s.ctx, op)
	return op.Entry.Child, err
}

func (s *GoofysTest) TestLookUpInode(t *C) {
	inode, err := s.LookUpInode(t, "", "file1")
	t.Assert(err, IsNil)

	defer s.ForgetInode(t, inode)

	_, err = s.LookUpInode(t, "", "fileNotFound")
	t.Assert(err, Equals, fuse.ENOENT)

	// XXX not supported yet
	// inode, err = s.LookUpInode(t, "dir1", "file3")
	// t.Assert(err, IsNil)

	// defer s.ForgetInode(t, inode)

	// inode, err = s.LookUpInode(t, "dir2/dir3", "file4")
	// t.Assert(err, IsNil)

	// defer s.ForgetInode(t, inode)

	// inode, err = s.LookUpInode(t, "", "empty_dir")
	// t.Assert(err, IsNil)

	// defer s.ForgetInode(t, inode)
}

func (s *GoofysTest) TestGetInodeAttributes(t *C) {
	inode, err := s.LookUpInode(t, "", "file1")
	t.Assert(err, IsNil)
	defer s.ForgetInode(t, inode)

	op := &fuseops.GetInodeAttributesOp{Inode: inode}
	err = s.fs.GetInodeAttributes(s.ctx, op)
	t.Assert(err, IsNil)
	t.Assert(op.Attributes.Size, Equals, uint64(len("file1")))
}

func (s *GoofysTest) OpenDir(t *C, inode fuseops.InodeID) (fuseops.HandleID, error) {
	op := &fuseops.OpenDirOp{Inode: inode}
	err := s.fs.OpenDir(s.ctx, op)
	return op.Handle, err
}

func ReadDirent(t *C, buf []byte) (*fuseutil.Dirent, []byte) {
	type fuse_dirent struct {
		ino     uint64
		off     uint64
		namelen uint32
		type_   uint32
		name    [0]byte
	}
	const direntAlignment = 8
	const direntSize = 8 + 8 + 4 + 4

	p := unsafe.Pointer(&buf[0])
	var consumed uint32

	de := (*fuse_dirent)(p)
	consumed += direntSize

	namebuf := make([]byte, de.namelen)
	copy(namebuf, buf[consumed:consumed + de.namelen])
	name := string(namebuf)
	consumed += de.namelen

	// Compute the number of bytes of padding we'll need to maintain alignment
	// for the next entry.
	var padLen int
	if len(name) % direntAlignment != 0 {
		padLen = direntAlignment - (len(name) % direntAlignment)
		consumed += uint32(padLen)
	}

	return &fuseutil.Dirent{
		Offset: fuseops.DirOffset(de.off),
		Inode: fuseops.InodeID(de.ino),
		Name: name,
		Type: fuseutil.DirentType(de.type_),
	}, buf[consumed:]
}

func (s *GoofysTest) ListDir(
	t *C,
	inode fuseops.InodeID,
	prefix string) (*map[string]*fuseutil.Dirent, *[]string, fuseops.HandleID) {

	dh, err := s.OpenDir(t, inode)
	t.Assert(err, IsNil)

	op := &fuseops.ReadDirOp{ Handle: dh, Inode: inode, Dst: make([]byte, 4096) }
	err = s.fs.ReadDir(s.ctx, op)

	// XXX verify the entries. here fuse is too lowlevel and this requires us to
	// deserialize op.Dst. See github.com/jacobsa/fuse/fuseutil/dirent.go
	buf := op.Dst[:op.BytesRead]
	res := make(map[string]*fuseutil.Dirent)
	keys := []string{}

	for len(buf) > 0 {
		var de *fuseutil.Dirent
		de, buf = ReadDirent(t, buf)
		t.Assert(de.Inode, Not(Equals), fuseops.InodeID(0))
		t.Assert(len(de.Name), Not(Equals), 0)

		name := prefix + de.Name

		res[name] = de
		keys = append(keys, name)
	}

	return &res, &keys, dh
}

func (s *GoofysTest) TestListDir(t *C) {
	// test listing /
	_, keys, dh := s.ListDir(t, fuseops.RootInodeID, "")
	defer s.fs.ReleaseDirHandle(s.ctx, &fuseops.ReleaseDirHandleOp{ Handle: dh })

	t.Assert(*keys, DeepEquals, []string{ "dir1", "dir2", "empty_dir", "file1", "file2" })

	// test listing dir1/
	lookup := &fuseops.LookUpInodeOp{ Parent: fuseops.RootInodeID, Name: "dir1" }
	err := s.fs.LookUpInode(s.ctx, lookup)
	t.Assert(err, IsNil)
	defer s.ForgetInode(t, lookup.Entry.Child)

	_, keys, dh = s.ListDir(t, lookup.Entry.Child, "dir1/")
	defer s.fs.ReleaseDirHandle(s.ctx, &fuseops.ReleaseDirHandleOp{ Handle: dh })
	t.Assert(*keys, DeepEquals, []string{ "dir1/file3" })

	// test listing dir2/
	lookup = &fuseops.LookUpInodeOp{ Parent: fuseops.RootInodeID, Name: "dir2" }
	err = s.fs.LookUpInode(s.ctx, lookup)
	t.Assert(err, IsNil)
	defer s.ForgetInode(t, lookup.Entry.Child)

	_, keys, dh = s.ListDir(t, lookup.Entry.Child, "dir2/")
	defer s.fs.ReleaseDirHandle(s.ctx, &fuseops.ReleaseDirHandleOp{ Handle: dh })
	t.Assert(*keys, DeepEquals, []string{ "dir2/dir3" })

	// test listing dir2/dir3/
	lookup = &fuseops.LookUpInodeOp{ Parent: lookup.Entry.Child, Name: "dir3" }
	err = s.fs.LookUpInode(s.ctx, lookup)
	t.Assert(err, IsNil)
	defer s.ForgetInode(t, lookup.Entry.Child)

	_, keys, dh = s.ListDir(t, lookup.Entry.Child, "dir2/dir3/")
	defer s.fs.ReleaseDirHandle(s.ctx, &fuseops.ReleaseDirHandleOp{ Handle: dh })
	t.Assert(*keys, DeepEquals, []string{ "dir2/dir3/file4" })
}

func (s *GoofysTest) ListDirRecursive(
	t *C,
	inode fuseops.InodeID,
	prefix string) (*map[string]*fuseutil.Dirent, *[]string) {

	res, keys, dh := s.ListDir(t, inode, prefix)
	defer s.fs.ReleaseDirHandle(s.ctx, &fuseops.ReleaseDirHandleOp{ Handle: dh })

	nkeys := len(*keys)
	for i := 0; i < nkeys; i++ {
		path := (*keys)[i]
		de := (*res)[path]

		switch de.Type {
		case fuseutil.DT_Directory:
			lookup := &fuseops.LookUpInodeOp{ Parent: inode, Name: de.Name }
			err := s.fs.LookUpInode(s.ctx, lookup)
			t.Assert(err, IsNil)
			defer s.ForgetInode(t, lookup.Entry.Child)

			t.Logf("Recursing into %v %v ", prefix + de.Name + "/", de.Inode)
			sub, subkeys := s.ListDirRecursive(t, lookup.Entry.Child, prefix + de.Name + "/")
			tmp := append(*keys, (*subkeys)...)
			keys = &tmp

			t.Log(*subkeys)
			if de.Name == "dir2" {
				t.Assert((*sub)["dir2/dir3"], NotNil)
			}

			for p, v := range *sub {
				t.Assert(p, Matches, prefix + de.Name + ".*")
				(*res)[p] = v
			}
		}
	}

	return res, keys
}

func (s *GoofysTest) TestListDirRecursive(t *C) {
	res, keys := s.ListDirRecursive(t, fuseops.RootInodeID, "")

	for expected := range s.env {
		if expected[len(expected) - 1] == '/' {
			expected = expected[0 : len(expected) - 1]
		}

		t.Assert((*res)[expected], NotNil)
	}

	t.Assert(*keys, DeepEquals, []string{ "dir1", "dir2", "empty_dir", "file1", "file2", "dir1/file3", "dir2/dir3", "dir2/dir3/file4" })
}

func (s *GoofysTest) OpenFile(t *C, inode fuseops.InodeID) (fuseops.HandleID, error) {
	op := &fuseops.OpenFileOp{ Inode: inode }
	err := s.fs.OpenFile(s.ctx, op)
	return op.Handle, err
}

func (s *GoofysTest) TestReadFiles(t *C) {
	res, _ := s.ListDirRecursive(t, fuseops.RootInodeID, "")

	for path, de := range *res {
		fh, err := s.OpenFile(t, de.Inode)
		t.Assert(err, IsNil)
		defer s.fs.ReleaseFileHandle(s.ctx, &fuseops.ReleaseFileHandleOp{ Handle: fh })

		op := &fuseops.ReadFileOp{ Handle: fh, Dst: make([]byte, 4096) }
		err = s.fs.ReadFile(s.ctx, op)
		t.Assert(err, IsNil)

		t.Assert(op.BytesRead, Equals, len(path))
		t.Assert(string(op.Dst), Equals, path)
	}
}
