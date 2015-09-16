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
	"log"
	"fmt"
	"net"
	"net/http"
	"io"
	"os"
	"os/exec"
	"os/user"
	"strconv"
	"testing"
	"time"

	"golang.org/x/net/context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jacobsa/fuse/fuseops"

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
	dir string
	s3proxy S3Proxy
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

func (s3proxy *S3Proxy) Download(t *C) (err error) {
	const url = "https://github.com/andrewgaul/s3proxy/releases/download/s3proxy-1.4.0/s3proxy"
	const size = 7690941
	fileName := "s3proxy"
	s3proxy.jar = fileName

	info, err := os.Stat(fileName)
	if err == nil {
		if info.Size() == size {
			t.Logf("Already downloaded %v", fileName)
			return
		}
	}

	log.Printf("Downloading %v", fileName)

	output, err := os.Create(fileName)
	if err != nil {
		t.Log("Error while creating", fileName, "-", err)
		return
	}

	defer output.Close()

	response, err := http.Get(url)
	if err != nil {
		t.Log("Error while downloading", url, "-", err)
		return
	}
	defer response.Body.Close()

	_, err = io.Copy(output, response.Body)
	if err != nil {
		t.Log("Error while downloading", url, "-", err)
		return
	}

	t.Logf("Downloaded %v", fileName)
	return
}

func (s3proxy *S3Proxy) GenerateConfig(t *C) {
	dir := t.MkDir()
	s3proxy.config = fmt.Sprintf("%s/%s", dir, "s3proxy.properties")
	out, err := os.Create(s3proxy.config)
	if err != nil {
		t.Log("Error while creating", s3proxy.config, "-", err)
		return
	}

	defer out.Close()
	out.Write([]byte("s3proxy.endpoint=http://127.0.0.1:8080\n"))
	out.Write([]byte("s3proxy.authorization=none\n"))
	out.Write([]byte("jclouds.provider=transient\n"))
	out.Write([]byte("jclouds.identity=local-identity\n"))
	out.Write([]byte("jclouds.credential=local-credential\n"))

	return
}

func logOutput(t *C, tag string, r io.ReadCloser) {
	in := bufio.NewScanner(r)

	for in.Scan() {
		t.Log(tag, in.Text())
	}
}

func (s3proxy *S3Proxy) Exec(t *C) (err error) {
	cmd := exec.Command("java", "-jar", s3proxy.jar, "--properties", s3proxy.config)
	log.Println("Executing", cmd.Args)
	pout, err := cmd.StdoutPipe()
	t.Assert(err, IsNil)

	perr, err := cmd.StderrPipe()
	t.Assert(err, IsNil)

	cmd.Start()

	go logOutput(t, "s3proxy", pout)
	go logOutput(t, "s3proxy.err", perr)

	// wait for it to listen on port
	for i := 0; i < 10; i++ {
		conn, err := net.Dial("tcp", "127.0.0.1:8080")
		if err == nil {
			// we are done!
			conn.Close()
			return err
		} else {
			t.Log("Cound not connect: %v", err)
			time.Sleep(1 * time.Second)
		}
	}

	cmd.Process.Kill()
	return
}

func (s *GoofysTest) SetUpSuite(t *C) {
	err := s.s3proxy.Download(t)
	t.Assert(err, IsNil)

	s.s3proxy.GenerateConfig(t)

	err = s.s3proxy.Exec(t)
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TearDownSuite(t *C) {
	if s.s3proxy.cmd != nil {
		s.s3proxy.cmd.Process.Kill()
	}
}

func (s *GoofysTest) SetUpTest(t *C) {
	s.fs = NewGoofys("goofys", currentUid(), currentGid())
	s.ctx = context.Background()
	s.dir = t.MkDir()
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
