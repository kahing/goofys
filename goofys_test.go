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
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"

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
			r = bytes.NewReader([]byte(""))
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
	env := map[string]io.ReadSeeker{
		"file1": nil,
		"file2": nil,
		"dir1/file3": nil,
		"dir2/dir3/file4": nil,
		"empty_dir/": nil,
	}

	bucket = RandStringBytesMaskImprSrc(16)
	s.setupEnv(t, bucket, env)
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

func (s *GoofysTest) TestLookUpInode(t *C) {
	op := &fuseops.LookUpInodeOp{
		Parent: fuseops.RootInodeID,
		Name: "file1",
	}
	err := s.fs.LookUpInode(s.ctx, op)
	t.Assert(err, IsNil)
	defer s.fs.ForgetInode(s.ctx, &fuseops.ForgetInodeOp{ Inode: op.Entry.Child })

	op = &fuseops.LookUpInodeOp{
		Parent: fuseops.RootInodeID,
		Name: "fileNotFound",
	}
	err = s.fs.LookUpInode(s.ctx, op)

	t.Assert(err, Equals, fuse.ENOENT)
}
