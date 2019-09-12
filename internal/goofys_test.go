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
	. "github.com/kahing/goofys/api/common"

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

	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/corehandlers"
	"github.com/aws/aws-sdk-go/aws/credentials"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
	azureauth "github.com/Azure/go-autorest/autorest/azure/auth"

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
	cloud     StorageBackend
	emulator  bool
	azurite   bool

	removeBucket []StorageBackend

	env map[string]*string
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

func (s *GoofysTest) selectTestConfig(t *C, flags *FlagStorage) (conf S3Config) {
	(&conf).Init()

	if hasEnv("AWS") {
		conf.Region = "us-west-2"
		conf.Profile = os.Getenv("AWS")
	} else if hasEnv("GCS") {
		conf.Region = "us-west1"
		conf.Profile = os.Getenv("GCS")
		flags.Endpoint = "http://storage.googleapis.com"
	} else if hasEnv("MINIO") {
		conf.Region = "us-east-1"
		conf.AccessKey = "Q3AM3UQ867SPQQA43P2F"
		conf.SecretKey = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
		flags.Endpoint = "https://play.minio.io:9000"
	} else {
		s.emulator = true

		conf.Region = "us-west-2"
		conf.AccessKey = "foo"
		conf.SecretKey = "bar"
		flags.Endpoint = "http://127.0.0.1:8080"
	}

	return
}

func (s *GoofysTest) waitForEmulator(t *C) {
	if s.emulator {
		addr := "127.0.0.1:8080"

		err := waitFor(t, addr)
		t.Assert(err, IsNil)
	}
}

func (s *GoofysTest) SetUpSuite(t *C) {
}

func (s *GoofysTest) deleteBucket(t *C, cloud StorageBackend) {
	param := &ListBlobsInput{}

	for {
		resp, err := cloud.ListBlobs(param)
		t.Assert(err, IsNil)

		keys := make([]string, 0)
		for _, o := range resp.Items {
			keys = append(keys, *o.Key)
		}

		if len(keys) != 0 {
			_, err = cloud.DeleteBlobs(&DeleteBlobsInput{Items: keys})
			t.Assert(err, IsNil)
		}

		if resp.IsTruncated {
			param.ContinuationToken = resp.NextContinuationToken
		} else {
			break
		}
	}

	_, err := cloud.RemoveBucket(&RemoveBucketInput{})
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TearDownTest(t *C) {
	for _, cloud := range s.removeBucket {
		s.deleteBucket(t, cloud)
	}
	s.removeBucket = nil
}

func (s *GoofysTest) removeBlob(cloud StorageBackend, t *C, blobPath string) {
	params := &DeleteBlobInput{
		Key: blobPath,
	}
	_, err := cloud.DeleteBlob(params)
	t.Assert(err, IsNil)
}

func (s *GoofysTest) setupBlobs(cloud StorageBackend, t *C, env map[string]*string) {
	var wg sync.WaitGroup

	var globalErr error
	for path, c := range env {
		wg.Add(1)
		go func(path string, content *string) {
			dir := false
			if content == nil {
				if strings.HasSuffix(path, "/") {
					if cloud.Capabilities().DirBlob {
						path = strings.TrimRight(path, "/")
					}
					dir = true
					content = PString("")
				} else {
					content = &path
				}
			}
			defer wg.Done()

			params := &PutBlobInput{
				Key:  path,
				Body: bytes.NewReader([]byte(*content)),
				Size: PUInt64(uint64(len(*content))),
				Metadata: map[string]*string{
					"name": aws.String(path + "+/#%00"),
				},
				DirBlob: dir,
			}

			_, err := cloud.PutBlob(params)
			if err != nil {
				globalErr = err
			}
			t.Assert(err, IsNil)
		}(path, c)
	}
	wg.Wait()
	t.Assert(globalErr, IsNil)

	// double check
	for path, c := range env {
		wg.Add(1)
		go func(path string, content *string) {
			defer wg.Done()
			params := &HeadBlobInput{Key: path}
			res, err := cloud.HeadBlob(params)
			t.Assert(err, IsNil)
			if content != nil {
				t.Assert(res.Size, Equals, uint64(len(*content)))
			} else if strings.HasSuffix(path, "/") || path == "zero" {
				t.Assert(res.Size, Equals, uint64(0))
			} else {
				t.Assert(res.Size, Equals, uint64(len(path)))
			}
		}(path, c)
	}
	wg.Wait()
	t.Assert(globalErr, IsNil)
}

func (s *GoofysTest) setupEnv(t *C, env map[string]*string, public bool) {
	if public {
		if s3, ok := s.cloud.(*S3Backend); ok {
			s3.config.ACL = "public-read"
		}
	}

	_, err := s.cloud.MakeBucket(&MakeBucketInput{})
	t.Assert(err, IsNil)

	if !s.emulator {
		time.Sleep(time.Second)
	}

	s.setupBlobs(s.cloud, t, env)

	t.Log("setupEnv done")
}

func (s *GoofysTest) setupDefaultEnv(t *C, public bool) {
	s.env = map[string]*string{
		"file1":           nil,
		"file2":           nil,
		"dir1/file3":      nil,
		"dir2/dir3/":      nil,
		"dir2/dir3/file4": nil,
		"dir4/":           nil,
		"dir4/file5":      nil,
		"empty_dir/":      nil,
		"empty_dir2/":     nil,
		"zero":            PString(""),
	}

	s.setupEnv(t, s.env, public)
}

func (s *GoofysTest) SetUpTest(t *C) {
	bucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	uid, gid := MyUserAndGroup()
	flags := &FlagStorage{
		DirMode:  0700,
		FileMode: 0700,
		Uid:      uint32(uid),
		Gid:      uint32(gid),
	}

	cloud := os.Getenv("CLOUD")

	if cloud == "s3" {
		s.waitForEmulator(t)

		conf := s.selectTestConfig(t, flags)
		flags.Backend = &conf

		s3, err := NewS3(bucket, flags, &conf)
		t.Assert(err, IsNil)

		s.cloud = s3
		s3.aws = hasEnv("AWS")

		if !hasEnv("MINIO") {
			s3.Handlers.Sign.Clear()
			s3.Handlers.Sign.PushBack(SignV2)
			s3.Handlers.Sign.PushBackNamed(corehandlers.BuildContentLengthHandler)
		}
		_, err = s3.ListBuckets(nil)
		t.Assert(err, IsNil)

	} else if cloud == "gcs" {
		conf := s.selectTestConfig(t, flags)
		flags.Backend = &conf

		var err error
		s.cloud, err = NewGCS3(bucket, flags, &conf)
		t.Assert(s.cloud, NotNil)
		t.Assert(err, IsNil)
	} else if cloud == "azblob" {
		config, err := AzureBlobConfig(os.Getenv("ENDPOINT"), "", "blob")
		t.Assert(err, IsNil)

		if config.Endpoint == AzuriteEndpoint {
			s.azurite = true
			s.emulator = true
			s.waitForEmulator(t)
		}

		// Azurite's SAS is buggy, ex: https://github.com/Azure/Azurite/issues/216
		if os.Getenv("SAS_EXPIRE") != "" {
			expire, err := time.ParseDuration(os.Getenv("SAS_EXPIRE"))
			t.Assert(err, IsNil)

			config.TokenRenewBuffer = expire / 2
			credential, err := azblob.NewSharedKeyCredential(config.AccountName, config.AccountKey)
			t.Assert(err, IsNil)

			// test sas token config
			config.SasToken = func() (string, error) {
				sasQueryParams, err := azblob.AccountSASSignatureValues{
					Protocol:   azblob.SASProtocolHTTPSandHTTP,
					StartTime:  time.Now().UTC().Add(-1 * time.Hour),
					ExpiryTime: time.Now().UTC().Add(expire),
					Services:   azblob.AccountSASServices{Blob: true}.String(),
					ResourceTypes: azblob.AccountSASResourceTypes{
						Service:   true,
						Container: true,
						Object:    true,
					}.String(),
					Permissions: azblob.AccountSASPermissions{
						Read:   true,
						Write:  true,
						Delete: true,
						List:   true,
						Create: true,
					}.String(),
				}.NewSASQueryParameters(credential)
				if err != nil {
					return "", err
				}
				return sasQueryParams.Encode(), nil
			}
		}

		flags.Backend = &config

		s.cloud, err = NewAZBlob(bucket, &config)
		t.Assert(err, IsNil)
		t.Assert(s.cloud, NotNil)
	} else if cloud == "adlv1" {
		cred := azureauth.NewClientCredentialsConfig(
			os.Getenv("ADLV1_CLIENT_ID"),
			os.Getenv("ADLV1_CLIENT_CREDENTIAL"),
			os.Getenv("ADLV1_TENANT_ID"))
		auth, err := cred.Authorizer()
		t.Assert(err, IsNil)

		config := ADLv1Config{
			Endpoint:   os.Getenv("ENDPOINT"),
			Authorizer: auth,
		}
		config.Init()

		flags.Backend = &config

		s.cloud, err = NewADLv1(bucket, flags, &config)
		t.Assert(err, IsNil)
		t.Assert(s.cloud, NotNil)
	} else if cloud == "adlv2" {
		var err error
		var auth autorest.Authorizer

		if os.Getenv("AZURE_STORAGE_ACCOUNT") != "" && os.Getenv("AZURE_STORAGE_KEY") != "" {
			auth = &AZBlobConfig{
				AccountName: os.Getenv("AZURE_STORAGE_ACCOUNT"),
				AccountKey:  os.Getenv("AZURE_STORAGE_KEY"),
			}
		} else {
			cred := azureauth.NewClientCredentialsConfig(
				os.Getenv("ADLV2_CLIENT_ID"),
				os.Getenv("ADLV2_CLIENT_CREDENTIAL"),
				os.Getenv("ADLV2_TENANT_ID"))
			cred.Resource = azure.PublicCloud.ResourceIdentifiers.Storage
			auth, err = cred.Authorizer()
			t.Assert(err, IsNil)
		}

		config := ADLv2Config{
			Endpoint:   os.Getenv("ENDPOINT"),
			Authorizer: auth,
		}

		flags.Backend = &config

		s.cloud, err = NewADLv2(bucket, flags, &config)
		t.Assert(err, IsNil)
		t.Assert(s.cloud, NotNil)
	} else {
		t.Fatal("Unsupported backend")
	}

	s.removeBucket = append(s.removeBucket, s.cloud)
	s.setupDefaultEnv(t, false)

	s.fs = NewGoofys(context.Background(), bucket, flags)
	t.Assert(s.fs, NotNil)

	s.ctx = context.Background()

	if hasEnv("GCS") {
		flags.Endpoint = "http://storage.googleapis.com"
	}
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

func (s *GoofysTest) TestSetup(t *C) {
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

func (s *GoofysTest) TestPanicWrapper(t *C) {
	fs := FusePanicLogger{s.fs}
	err := fs.GetInodeAttributes(nil, &fuseops.GetInodeAttributesOp{
		Inode: 1234,
	})
	t.Assert(err, Equals, fuse.EIO)
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
	t.Assert(en.Name, Equals, ".")

	en, err = dh.ReadDir(fuseops.DirOffset(1))
	t.Assert(err, IsNil)
	t.Assert(en, NotNil)
	t.Assert(en.Name, Equals, "..")

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
		names = append(names, en.Name)
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

func (s *GoofysTest) TestReadDirWithExternalChanges(t *C) {
	s.fs.flags.TypeCacheTTL = time.Second

	dir1, err := s.LookUpInode(t, "dir1")
	t.Assert(err, IsNil)

	defaultEntries := []string{
		"dir1", "dir2", "dir4", "empty_dir",
		"empty_dir2", "file1", "file2", "zero"}
	s.assertEntries(t, s.getRoot(t), defaultEntries)
	// dir1 has file3 and nothing else.
	s.assertEntries(t, dir1, []string{"file3"})

	// Do the following 'external' changes in s3 without involving goofys.
	// - Remove file1, add file3.
	// - Remove dir1/file3. Given that dir1 has just this one file,
	//   we are effectively removing dir1 as well.
	s.removeBlob(s.cloud, t, "file1")
	s.setupBlobs(s.cloud, t, map[string]*string{"file3": nil})
	s.removeBlob(s.cloud, t, "dir1/file3")

	time.Sleep(s.fs.flags.TypeCacheTTL)
	// newEntries = `defaultEntries` - dir1 - file1 + file3.
	newEntries := []string{
		"dir2", "dir4", "empty_dir", "empty_dir2",
		"file2", "file3", "zero"}
	if s.cloud.Capabilities().DirBlob {
		// dir1 is not automatically deleted
		newEntries = append([]string{"dir1"}, newEntries...)
	}
	s.assertEntries(t, s.getRoot(t), newEntries)
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
	in, err = s.LookUpInode(t, "dir2/dir3")
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
			in, err := parent.LookUp(en.Name)
			t.Assert(err, IsNil)

			fh, err := in.OpenFile(fuseops.OpMetadata{uint32(os.Getpid())})
			t.Assert(err, IsNil)

			buf := make([]byte, 4096)

			nread, err := fh.ReadFile(0, buf)
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

	in, err := root.LookUp(f)
	t.Assert(err, IsNil)

	fh, err := in.OpenFile(fuseops.OpMetadata{uint32(os.Getpid())})
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

	_, fh := s.getRoot(t).Create(fileName, fuseops.OpMetadata{uint32(os.Getpid())})

	err := fh.FlushFile()
	t.Assert(err, IsNil)

	resp, err := s.cloud.GetBlob(&GetBlobInput{Key: fileName})
	t.Assert(err, IsNil)
	t.Assert(resp.HeadBlobOutput.Size, DeepEquals, uint64(0))
	defer resp.Body.Close()

	_, err = s.getRoot(t).LookUp(fileName)
	t.Assert(err, IsNil)

	fileName = "testCreateFile2"
	s.testWriteFile(t, fileName, 1, 128*1024)

	inode, err := s.getRoot(t).LookUp(fileName)
	t.Assert(err, IsNil)

	fh, err = inode.OpenFile(fuseops.OpMetadata{uint32(os.Getpid())})
	t.Assert(err, IsNil)

	err = fh.FlushFile()
	t.Assert(err, IsNil)

	resp, err = s.cloud.GetBlob(&GetBlobInput{Key: fileName})
	t.Assert(err, IsNil)
	// ADLv1 doesn't return size when we do a GET
	if _, adlv1 := s.cloud.(*ADLv1); !adlv1 {
		t.Assert(resp.HeadBlobOutput.Size, Equals, uint64(1))
	}
	defer resp.Body.Close()
}

func (s *GoofysTest) TestUnlink(t *C) {
	fileName := "file1"

	err := s.getRoot(t).Unlink(fileName)
	t.Assert(err, IsNil)

	// make sure that it's gone from s3
	_, err = s.cloud.GetBlob(&GetBlobInput{Key: fileName})
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
	root := s.getRoot(t)

	lookup := fuseops.LookUpInodeOp{
		Parent: root.Id,
		Name:   fileName,
	}
	err := s.fs.LookUpInode(nil, &lookup)
	if err != nil {
		if err == fuse.ENOENT {
			create := fuseops.CreateFileOp{
				Parent: root.Id,
				Name:   fileName,
			}
			err = s.fs.CreateFile(nil, &create)
			t.Assert(err, IsNil)

			fh = s.fs.fileHandles[create.Handle]
		} else {
			t.Assert(err, IsNil)
		}
	} else {
		in := s.fs.inodes[lookup.Entry.Child]
		fh, err = in.OpenFile(fuseops.OpMetadata{uint32(os.Getpid())})
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

	err = fh.FlushFile()
	t.Assert(err, IsNil)

	resp, err := s.cloud.HeadBlob(&HeadBlobInput{Key: fileName})
	t.Assert(err, IsNil)
	t.Assert(resp.Size, Equals, uint64(size+offset))

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
	if _, ok := s.cloud.(*ADLv1); ok {
		s.fs.bufferPool.maxBuffers = 4
	} else {
		s.fs.bufferPool.maxBuffers = 2
	}
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

	fh, err := in.OpenFile(fuseops.OpMetadata{uint32(os.Getpid())})
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
	_, fh := inode.Create(fileName, fuseops.OpMetadata{uint32(os.Getpid())})

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
	if _, ok := s.cloud.(*ADLv1); ok {
		t.Skip("ADLv1 doesn't support metadata")
	}
	root := s.getRoot(t)

	from, to := "file1", "new_file"

	metadata := make(map[string]*string)
	metadata["foo"] = aws.String("bar")

	_, err := s.cloud.CopyBlob(&CopyBlobInput{
		Source:      from,
		Destination: from,
		Metadata:    metadata,
	})
	t.Assert(err, IsNil)

	err = root.Rename(from, root, to)
	t.Assert(err, IsNil)

	resp, err := s.cloud.HeadBlob(&HeadBlobInput{Key: to})
	t.Assert(err, IsNil)
	t.Assert(resp.Metadata["foo"], NotNil)
	t.Assert(*resp.Metadata["foo"], Equals, "bar")
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

func (s *GoofysTest) TestBackendListPrefix(t *C) {
	res, err := s.cloud.ListBlobs(&ListBlobsInput{
		Prefix:    PString("random"),
		Delimiter: PString("/"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Equals, 0)
	t.Assert(len(res.Items), Equals, 0)

	res, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix:    PString("empty_dir"),
		Delimiter: PString("/"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Not(Equals), 0)
	t.Assert(*res.Prefixes[0].Prefix, Equals, "empty_dir/")
	t.Assert(len(res.Items), Equals, 0)

	res, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix:    PString("empty_dir/"),
		Delimiter: PString("/"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Equals, 0)
	t.Assert(len(res.Items), Equals, 1)
	t.Assert(*res.Items[0].Key, Equals, "empty_dir/")

	res, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix:    PString("file1"),
		Delimiter: PString("/"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Equals, 0)
	t.Assert(len(res.Items), Equals, 1)
	t.Assert(*res.Items[0].Key, Equals, "file1")

	res, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix:    PString("file1/"),
		Delimiter: PString("/"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Equals, 0)
	t.Assert(len(res.Items), Equals, 0)

	res, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix:    PString("dir2/"),
		Delimiter: PString("/"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Equals, 1)
	t.Assert(*res.Prefixes[0].Prefix, Equals, "dir2/dir3/")
	if s.cloud.Capabilities().DirBlob {
		t.Assert(len(res.Items), Equals, 1)
		t.Assert(*res.Items[0].Key, Equals, "dir2/")
	} else {
		t.Assert(len(res.Items), Equals, 0)
	}

	res, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix:    PString("dir2/dir3/"),
		Delimiter: PString("/"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Equals, 0)
	t.Assert(len(res.Items), Equals, 2)
	t.Assert(*res.Items[0].Key, Equals, "dir2/dir3/")
	t.Assert(*res.Items[1].Key, Equals, "dir2/dir3/file4")

	res, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix: PString("dir2/"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Equals, 0)
	t.Assert(len(res.Items), Equals, 2)
	t.Assert(*res.Items[0].Key, Equals, "dir2/dir3/")
	t.Assert(*res.Items[1].Key, Equals, "dir2/dir3/file4")

	res, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix: PString("dir2/dir3/file4"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Equals, 0)
	t.Assert(len(res.Items), Equals, 1)
	t.Assert(*res.Items[0].Key, Equals, "dir2/dir3/file4")
}

func (s *GoofysTest) TestRenameDir(t *C) {
	s.fs.flags.StatCacheTTL = 0

	root := s.getRoot(t)

	err := root.Rename("empty_dir", root, "dir1")
	t.Assert(err, Equals, fuse.ENOTEMPTY)

	err = root.Rename("empty_dir", root, "new_dir")
	t.Assert(err, IsNil)

	dir2, err := s.LookUpInode(t, "dir2")
	t.Assert(err, IsNil)
	t.Assert(dir2, NotNil)

	_, err = s.LookUpInode(t, "new_dir2")
	t.Assert(err, Equals, fuse.ENOENT)

	err = s.fs.Rename(nil, &fuseops.RenameOp{
		OldParent: root.Id,
		NewParent: root.Id,
		OldName:   "dir2",
		NewName:   "new_dir2",
	})
	t.Assert(err, IsNil)

	_, err = s.LookUpInode(t, "dir2/dir3")
	t.Assert(err, Equals, fuse.ENOENT)

	_, err = s.LookUpInode(t, "dir2/dir3/file4")
	t.Assert(err, Equals, fuse.ENOENT)

	new_dir2, err := s.LookUpInode(t, "new_dir2")
	t.Assert(err, IsNil)
	t.Assert(new_dir2, NotNil)
	t.Assert(dir2.Id, Equals, new_dir2.Id)

	old, err := s.LookUpInode(t, "new_dir2/dir3/file4")
	t.Assert(err, IsNil)
	t.Assert(old, NotNil)

	err = s.fs.Rename(nil, &fuseops.RenameOp{
		OldParent: root.Id,
		NewParent: root.Id,
		OldName:   "new_dir2",
		NewName:   "new_dir3",
	})
	t.Assert(err, IsNil)

	new, err := s.LookUpInode(t, "new_dir3/dir3/file4")
	t.Assert(err, IsNil)
	t.Assert(new, NotNil)
	t.Assert(old.Id, Equals, new.Id)

	_, err = s.LookUpInode(t, "new_dir2/dir3")
	t.Assert(err, Equals, fuse.ENOENT)

	_, err = s.LookUpInode(t, "new_dir2/dir3/file4")
	t.Assert(err, Equals, fuse.ENOENT)
}

func (s *GoofysTest) TestRename(t *C) {
	root := s.getRoot(t)

	from, to := "empty_dir", "file1"
	err := root.Rename(from, root, to)
	t.Assert(err, Equals, fuse.ENOTDIR)

	from, to = "file1", "empty_dir"
	err = root.Rename(from, root, to)
	t.Assert(err, Equals, syscall.EISDIR)

	from, to = "file1", "new_file"
	err = root.Rename(from, root, to)
	t.Assert(err, IsNil)

	_, err = s.cloud.HeadBlob(&HeadBlobInput{Key: to})
	t.Assert(err, IsNil)

	_, err = s.cloud.HeadBlob(&HeadBlobInput{Key: from})
	t.Assert(mapAwsError(err), Equals, fuse.ENOENT)

	from, to = "file3", "new_file2"
	dir, _ := s.LookUpInode(t, "dir1")
	err = dir.Rename(from, root, to)
	t.Assert(err, IsNil)

	_, err = s.cloud.HeadBlob(&HeadBlobInput{Key: to})
	t.Assert(err, IsNil)

	_, err = s.cloud.HeadBlob(&HeadBlobInput{Key: from})
	t.Assert(mapAwsError(err), Equals, fuse.ENOENT)

	from, to = "no_such_file", "new_file"
	err = root.Rename(from, root, to)
	t.Assert(err, Equals, fuse.ENOENT)

	if s3, ok := s.cloud.(*S3Backend); ok {
		if !hasEnv("GCS") {
			// not really rename but can be used by rename
			from, to = s.fs.bucket+"/file2", "new_file"
			_, err = s3.copyObjectMultipart(int64(len("file2")), from, to, "", nil, nil, nil)
			t.Assert(err, IsNil)
		}
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
	if fuseLog.Level == logrus.DebugLevel {
		mountCfg.DebugLogger = GetStdLogger(fuseLog, logrus.DebugLevel)
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
	var err error
	for i := 0; i < 10; i++ {
		err = fuse.Unmount(mountPoint)
		if err != nil {
			time.Sleep(100 * time.Millisecond)
		} else {
			break
		}
	}
	t.Assert(err, IsNil)

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
	cmd.Env = append(cmd.Env, "FAST=true")
	cmd.Env = append(cmd.Env, "CLEANUP=false")

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
	if s.cloud.Capabilities().DirBlob {
		t.Skip("only for backends without dir blob")
	}

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
	s.fs.flags.TypeCacheTTL = 1 * time.Minute
	s.fs.flags.StatCacheTTL = 1 * time.Minute
	mountPoint := "/tmp/mnt" + s.fs.bucket
	s.runFuseTest(t, mountPoint, false, "../bench/bench.sh", "cat", mountPoint, "ls")
}

func (s *GoofysTest) TestBenchCreate(t *C) {
	s.fs.flags.TypeCacheTTL = 1 * time.Minute
	s.fs.flags.StatCacheTTL = 1 * time.Minute
	mountPoint := "/tmp/mnt" + s.fs.bucket
	s.runFuseTest(t, mountPoint, false, "../bench/bench.sh", "cat", mountPoint, "create")
}

func (s *GoofysTest) TestBenchCreateParallel(t *C) {
	s.fs.flags.TypeCacheTTL = 1 * time.Minute
	s.fs.flags.StatCacheTTL = 1 * time.Minute
	mountPoint := "/tmp/mnt" + s.fs.bucket
	s.runFuseTest(t, mountPoint, false, "../bench/bench.sh", "cat", mountPoint, "create_parallel")
}

func (s *GoofysTest) TestBenchIO(t *C) {
	s.fs.flags.TypeCacheTTL = 1 * time.Minute
	s.fs.flags.StatCacheTTL = 1 * time.Minute
	mountPoint := "/tmp/mnt" + s.fs.bucket
	s.runFuseTest(t, mountPoint, false, "../bench/bench.sh", "cat", mountPoint, "io")
}

func (s *GoofysTest) TestBenchFindTree(t *C) {
	s.fs.flags.TypeCacheTTL = 1 * time.Minute
	s.fs.flags.StatCacheTTL = 1 * time.Minute
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
	mime := s.fs.flags.GetMimeType("foo.css")
	t.Assert(mime, IsNil)

	s.fs.flags.UseContentType = true

	mime = s.fs.flags.GetMimeType("foo.css")
	t.Assert(mime, NotNil)
	t.Assert(*mime, Equals, "text/css")

	mime = s.fs.flags.GetMimeType("foo")
	t.Assert(mime, IsNil)

	mime = s.fs.flags.GetMimeType("foo.")
	t.Assert(mime, IsNil)

	mime = s.fs.flags.GetMimeType("foo.unknownExtension")
	t.Assert(mime, IsNil)
}

func (s *GoofysTest) TestPutMimeType(t *C) {
	if _, ok := s.cloud.(*ADLv1); ok {
		// ADLv1 doesn't support content-type
		t.Skip("ADLv1 doesn't support content-type")
	}

	s.fs.flags.UseContentType = true

	root := s.getRoot(t)
	jpg := "test.jpg"
	jpg2 := "test2.jpg"
	file := "test"

	s.testWriteFile(t, jpg, 10, 128)

	resp, err := s.cloud.HeadBlob(&HeadBlobInput{Key: jpg})
	t.Assert(err, IsNil)
	t.Assert(*resp.ContentType, Equals, "image/jpeg")

	err = root.Rename(jpg, root, file)
	t.Assert(err, IsNil)

	resp, err = s.cloud.HeadBlob(&HeadBlobInput{Key: file})
	t.Assert(err, IsNil)
	t.Assert(*resp.ContentType, Equals, "image/jpeg")

	err = root.Rename(file, root, jpg2)
	t.Assert(err, IsNil)

	resp, err = s.cloud.HeadBlob(&HeadBlobInput{Key: jpg2})
	t.Assert(err, IsNil)
	t.Assert(*resp.ContentType, Equals, "image/jpeg")
}

func (s *GoofysTest) TestBucketPrefixSlash(t *C) {
	s.fs = NewGoofys(context.Background(), s.fs.bucket+":dir2", s.fs.flags)
	t.Assert(s.getRoot(t).dir.mountPrefix, Equals, "dir2/")

	s.fs = NewGoofys(context.Background(), s.fs.bucket+":dir2///", s.fs.flags)
	t.Assert(s.getRoot(t).dir.mountPrefix, Equals, "dir2/")
}

func (s *GoofysTest) TestFuseWithPrefix(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket

	s.fs = NewGoofys(context.Background(), s.fs.bucket+":testprefix", s.fs.flags)

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
	// On azure this fails because we re-create the bucket with
	// the same name right away. And well anonymous access is not
	// implemented yet in our azure backend anyway
	if _, ok := s.cloud.(*S3Backend); !ok {
		t.Skip("only for S3")
	}

	s.deleteBucket(t, s.cloud)

	s.setupDefaultEnv(t, true)

	s.fs = NewGoofys(context.Background(), s.fs.bucket, s.fs.flags)
	t.Assert(s.fs, NotNil)

	// should have auto-detected by S3 backend
	cloud := s.getRoot(t).dir.cloud
	t.Assert(cloud, NotNil)
	s3, ok := cloud.(*S3Backend)
	t.Assert(ok, Equals, true)

	if s3.config.Profile != "" {
		t.Skip("anonymous access is disabled with profile")
	}
	t.Assert(s3.awsConfig.Credentials, Equals, credentials.AnonymousCredentials)
}

func (s *GoofysTest) disableS3() {
	time.Sleep(1 * time.Second) // wait for any background goroutines to finish
	s.fs.inodes[fuseops.RootInodeID].dir.cloud = nil
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
	if s.azurite {
		t.Skip("https://github.com/Azure/Azurite/issues/221")
	}

	params := &PutBlobInput{
		Key:  "dir1/l├â┬╢r 006.jpg",
		Body: bytes.NewReader([]byte("foo")),
		Size: PUInt64(3),
	}
	_, err := s.cloud.PutBlob(params)
	t.Assert(err, IsNil)

	dir, err := s.LookUpInode(t, "dir1")
	t.Assert(err, IsNil)

	err = dir.Rename("l├â┬╢r 006.jpg", dir, "myfile.jpg")
	t.Assert(err, IsNil)

	resp, err := s.cloud.HeadBlob(&HeadBlobInput{Key: "dir1/myfile.jpg"})
	t.Assert(resp.Size, Equals, uint64(3))
}

func (s *GoofysTest) TestXAttrGet(t *C) {
	if _, ok := s.cloud.(*ADLv1); ok {
		t.Skip("ADLv1 doesn't support metadata")
	}

	_, checkETag := s.cloud.(*S3Backend)

	file1, err := s.LookUpInode(t, "file1")
	t.Assert(err, IsNil)

	names, err := file1.ListXattr()
	t.Assert(err, IsNil)
	sort.Strings(names)
	t.Assert(names, DeepEquals, []string{"s3.etag", "s3.storage-class", "user.name"})

	_, err = file1.GetXattr("user.foobar")
	// xattr.IsNotExist seems broken on recent version of macOS
	t.Assert(err, Equals, syscall.ENODATA)

	if checkETag {
		value, err := file1.GetXattr("s3.etag")
		t.Assert(err, IsNil)
		// md5sum of "file1"
		t.Assert(string(value), Equals, "\"826e8142e6baabe8af779f5f490cf5f5\"")
	}

	value, err := file1.GetXattr("user.name")
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

	if checkETag {
		value, err = file3.GetXattr("s3.etag")
		t.Assert(err, IsNil)
		// md5sum of "dir1/file3"
		t.Assert(string(value), Equals, "\"5cd67e0e59fb85be91a515afe0f4bb24\"")
	}

	// ensure that we get the dir blob instead of list
	s.fs.flags.Cheap = true

	emptyDir2, err := s.LookUpInode(t, "empty_dir2")
	t.Assert(err, IsNil)

	names, err = emptyDir2.ListXattr()
	t.Assert(err, IsNil)
	sort.Strings(names)
	t.Assert(names, DeepEquals, []string{"s3.etag", "s3.storage-class", "user.name"})

	emptyDir, err := s.LookUpInode(t, "empty_dir")
	t.Assert(err, IsNil)

	if checkETag {
		value, err = emptyDir.GetXattr("s3.etag")
		t.Assert(err, IsNil)
		// dir blobs are empty
		t.Assert(string(value), Equals, "\"d41d8cd98f00b204e9800998ecf8427e\"")
	}

	if !s.cloud.Capabilities().DirBlob {
		// implicit dir blobs don't have s3.etag at all
		names, err = dir1.ListXattr()
		t.Assert(err, IsNil)
		t.Assert(names, HasLen, 0)

		value, err = dir1.GetXattr("s3.etag")
		t.Assert(err, Equals, syscall.ENODATA)
	}

	// s3proxy doesn't support storage class yet
	if hasEnv("AWS") {
		cloud := s.getRoot(t).dir.cloud
		s3, ok := cloud.(*S3Backend)
		t.Assert(ok, Equals, true)
		s3.config.StorageClass = "STANDARD_IA"

		s.testWriteFile(t, "ia", 1, 128*1024)

		ia, err := s.LookUpInode(t, "ia")
		t.Assert(err, IsNil)

		names, err = ia.ListXattr()
		t.Assert(names, DeepEquals, []string{"s3.etag", "s3.storage-class"})

		value, err = ia.GetXattr("s3.storage-class")
		t.Assert(err, IsNil)
		// smaller than 128KB falls back to standard
		t.Assert(string(value), Equals, "STANDARD")

		s.testWriteFile(t, "ia", 128*1024, 128*1024)
		time.Sleep(100 * time.Millisecond)

		names, err = ia.ListXattr()
		t.Assert(names, DeepEquals, []string{"s3.etag", "s3.storage-class"})

		value, err = ia.GetXattr("s3.storage-class")
		t.Assert(err, IsNil)
		t.Assert(string(value), Equals, "STANDARD_IA")
	}
}

func (s *GoofysTest) TestClientForkExec(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket
	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)
	file := mountPoint + "/TestClientForkExec"

	// Create new file.
	fh, err := os.OpenFile(file, os.O_CREATE|os.O_RDWR, 0600)
	t.Assert(err, IsNil)
	defer func() { // Defer close file if it's not already closed.
		if fh != nil {
			fh.Close()
		}
	}()
	// Write to file.
	_, err = fh.WriteString("1.1;")
	t.Assert(err, IsNil)
	// The `Command` is run via fork+exec.
	// So all the file descriptors are copied over to the child process.
	// The child process 'closes' the files before exiting. This should
	// not result in goofys failing file operations invoked from the test.
	someCmd := exec.Command("echo", "hello")
	err = someCmd.Run()
	t.Assert(err, IsNil)
	// One more write.
	_, err = fh.WriteString("1.2;")
	t.Assert(err, IsNil)
	// Close file.
	err = fh.Close()
	t.Assert(err, IsNil)
	fh = nil
	// Check file content.
	content, err := ioutil.ReadFile(file)
	t.Assert(err, IsNil)
	t.Assert(string(content), Equals, "1.1;1.2;")

	// Repeat the same excercise, but now with an existing file.
	fh, err = os.OpenFile(file, os.O_RDWR, 0600)
	// Write to file.
	_, err = fh.WriteString("2.1;")
	// fork+exec.
	someCmd = exec.Command("echo", "hello")
	err = someCmd.Run()
	t.Assert(err, IsNil)
	// One more write.
	_, err = fh.WriteString("2.2;")
	t.Assert(err, IsNil)
	// Close file.
	err = fh.Close()
	t.Assert(err, IsNil)
	fh = nil
	// Verify that the file is updated as per the new write.
	content, err = ioutil.ReadFile(file)
	t.Assert(err, IsNil)
	t.Assert(string(content), Equals, "2.1;2.2;")
}

func (s *GoofysTest) TestXAttrGetCached(t *C) {
	if _, ok := s.cloud.(*ADLv1); ok {
		t.Skip("ADLv1 doesn't support metadata")
	}

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
	if _, ok := s.cloud.(*ADLv1); ok {
		t.Skip("ADLv1 doesn't support metadata")
	}

	root := s.getRoot(t)

	err := root.Rename("file1", root, "file0")
	t.Assert(err, IsNil)

	in, err := s.LookUpInode(t, "file0")
	t.Assert(err, IsNil)

	_, err = in.GetXattr("user.name")
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestXAttrRemove(t *C) {
	if _, ok := s.cloud.(*ADLv1); ok {
		t.Skip("ADLv1 doesn't support metadata")
	}

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
	if _, ok := s.cloud.(*ADLv1); ok {
		t.Skip("ADLv1 doesn't support metadata")
	}

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
	if s.azurite {
		// Azurite returns 400 when copy source doesn't exist
		// https://github.com/Azure/Azurite/issues/219
		// so our code to ignore ENOENT fails
		t.Skip("https://github.com/Azure/Azurite/issues/219")
	}

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

	in := NewInode(s.fs, root, aws.String("2"))
	in.Attributes = InodeAttributes{}
	root.insertChild(in)
	t.Assert(*root.dir.Children[2].Name, Equals, "2")

	in = NewInode(s.fs, root, aws.String("1"))
	in.Attributes = InodeAttributes{}
	root.insertChild(in)
	t.Assert(*root.dir.Children[2].Name, Equals, "1")
	t.Assert(*root.dir.Children[3].Name, Equals, "2")

	in = NewInode(s.fs, root, aws.String("4"))
	in.Attributes = InodeAttributes{}
	root.insertChild(in)
	t.Assert(*root.dir.Children[2].Name, Equals, "1")
	t.Assert(*root.dir.Children[3].Name, Equals, "2")
	t.Assert(*root.dir.Children[4].Name, Equals, "4")

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

	root.removeChild(root.dir.Children[3])
	root.removeChild(root.dir.Children[2])
	root.removeChild(root.dir.Children[2])
	t.Assert(len(root.dir.Children), Equals, 2)
}

func (s *GoofysTest) TestReadDirSlurpHeuristic(t *C) {
	if _, ok := s.cloud.(*S3Backend); !ok {
		t.Skip("only for S3")
	}
	s.fs.flags.TypeCacheTTL = 1 * time.Minute

	s.setupBlobs(s.cloud, t, map[string]*string{"dir2isafile": nil})

	root := s.getRoot(t).dir
	t.Assert(root.seqOpenDirScore, Equals, uint8(0))
	s.assertEntries(t, s.getRoot(t), []string{
		"dir1", "dir2", "dir2isafile", "dir4", "empty_dir",
		"empty_dir2", "file1", "file2", "zero"})

	dir1, err := s.LookUpInode(t, "dir1")
	t.Assert(err, IsNil)
	dh1 := dir1.OpenDir()
	defer dh1.CloseDir()
	score := root.seqOpenDirScore

	dir2, err := s.LookUpInode(t, "dir2")
	t.Assert(err, IsNil)
	dh2 := dir2.OpenDir()
	defer dh2.CloseDir()
	t.Assert(root.seqOpenDirScore, Equals, score+1)

	dir3, err := s.LookUpInode(t, "dir4")
	t.Assert(err, IsNil)
	dh3 := dir3.OpenDir()
	defer dh3.CloseDir()
	t.Assert(root.seqOpenDirScore, Equals, score+2)
}

func (s *GoofysTest) TestReadDirSlurpSubtree(t *C) {
	if _, ok := s.cloud.(*S3Backend); !ok {
		t.Skip("only for S3")
	}
	s.fs.flags.TypeCacheTTL = 1 * time.Minute
	s.fs.flags.StatCacheTTL = 1 * time.Minute

	s.getRoot(t).dir.seqOpenDirScore = 2
	in, err := s.LookUpInode(t, "dir2")
	t.Assert(err, IsNil)
	t.Assert(s.getRoot(t).dir.seqOpenDirScore, Equals, uint8(2))

	s.readDirIntoCache(t, in.Id)
	// should have incremented the score
	t.Assert(s.getRoot(t).dir.seqOpenDirScore, Equals, uint8(3))

	// reading dir2 should cause dir2/dir3 to have cached readdir
	s.disableS3()

	in, err = s.LookUpInode(t, "dir2/dir3")
	t.Assert(err, IsNil)

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
			dirs = append(dirs, en.Name)
		} else {
			files = append(files, en.Name)
			noMoreDir = true
		}
	}

	t.Assert(dirs, DeepEquals, []string{"dir1", "dir2", "dir4", "empty_dir", "empty_dir2"})
	t.Assert(files, DeepEquals, []string{"file1", "file2", "zero"})
}

func (s *GoofysTest) TestReadDirLookUp(t *C) {
	s.getRoot(t).dir.seqOpenDirScore = 2

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			s.readDirIntoCache(t, fuseops.RootInodeID)
			wg.Done()
		}()
		go func() {
			lookup := fuseops.LookUpInodeOp{
				Parent: fuseops.RootInodeID,
				Name:   "file1",
			}
			err := s.fs.LookUpInode(nil, &lookup)
			t.Assert(err, IsNil)
			wg.Done()
		}()
	}
	wg.Wait()
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

	_, _ = root.Create("foo", fuseops.OpMetadata{uint32(os.Getpid())})
	attr2, _ := root.GetAttributes()
	m2 := attr2.Mtime

	t.Assert(m1.Before(m2), Equals, true)
}

func (s *GoofysTest) TestDirMtimeLs(t *C) {
	root := s.getRoot(t)

	attr, _ := root.GetAttributes()
	m1 := attr.Mtime
	time.Sleep(time.Second)

	params := &PutBlobInput{
		Key:  "newfile",
		Body: bytes.NewReader([]byte("foo")),
		Size: PUInt64(3),
	}
	_, err := s.cloud.PutBlob(params)
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
	// anonymous only works in S3 for now
	cloud := s.getRoot(t).dir.cloud
	s3, ok := cloud.(*S3Backend)
	if !ok {
		t.Skip("only for S3")
	}

	s.fs.flags.StatCacheTTL = 1 * time.Minute
	s.fs.flags.TypeCacheTTL = 1 * time.Minute

	// cache the inode first so we don't get 403 when we lookup
	in, err := s.LookUpInode(t, "file1")
	t.Assert(err, IsNil)

	fh, err := in.OpenFile(fuseops.OpMetadata{uint32(os.Getpid())})
	t.Assert(err, IsNil)

	s3.awsConfig.Credentials = credentials.AnonymousCredentials
	s3.newS3()

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

	file1, err := s.LookUpInode(t, "dir1")
	t.Assert(err, IsNil)

	// take mtime from a blob as init time because when we test against
	// real cloud, server time can be way off from local time
	initTime := file1.Attributes.Mtime

	dir1, err := s.LookUpInode(t, "dir1")
	t.Assert(err, IsNil)

	attr1, _ := dir1.GetAttributes()
	m1 := attr1.Mtime
	if !s.cloud.Capabilities().DirBlob {
		// dir1 doesn't have a dir blob, so should take root's mtime
		t.Assert(m1, Equals, root.Attributes.Mtime)
	}

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

	// we never added the inode so this will do the lookup again
	dir2, err = dir1.LookUp("dir2")
	t.Assert(err, IsNil)

	// the new time comes from S3 which only has seconds
	// granularity
	attr2, _ = dir2.GetAttributes()
	t.Assert(m2, Not(Equals), attr2.Mtime)
	t.Assert(initTime.Add(time.Second).Before(attr2.Mtime), Equals, true)

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

	params := &PutBlobInput{
		Key:  "dir2/newfile",
		Body: bytes.NewReader([]byte("foo")),
		Size: PUInt64(3),
	}
	_, err = s.cloud.PutBlob(params)
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
	if s.cloud.Capabilities().DirBlob {
		t.Skip("Tests for behavior without dir blob")
	}
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
	// setupDefaultEnv is before mounting but we can't really
	// compare the time here since dir3 is s3 server time and dir2
	// is local time
	t.Assert(attr3.Mtime, Not(Equals), m2)
}

func (s *GoofysTest) TestIssue326(t *C) {
	root := s.getRoot(t)
	_, err := root.MkDir("folder@name.something")
	t.Assert(err, IsNil)
	_, err = root.MkDir("folder#1#")
	t.Assert(err, IsNil)

	s.readDirIntoCache(t, root.Id)
	s.assertEntries(t, root, []string{"dir1", "dir2", "dir4", "empty_dir", "empty_dir2",
		"file1", "file2", "folder#1#", "folder@name.something", "zero"})
}

func (s *GoofysTest) TestSlurpFileAndDir(t *C) {
	if _, ok := s.cloud.(*S3Backend); !ok {
		t.Skip("only for S3")
	}
	prefix := "TestSlurpFileAndDir/"
	// fileAndDir is both a file and a directory, and we are
	// slurping them together as part of our listing optimization
	blobs := []string{
		prefix + "fileAndDir",
		prefix + "fileAndDir/a",
	}

	for _, b := range blobs {
		params := &PutBlobInput{
			Key:  b,
			Body: bytes.NewReader([]byte("foo")),
			Size: PUInt64(3),
		}
		_, err := s.cloud.PutBlob(params)
		t.Assert(err, IsNil)
	}

	s.fs.flags.TypeCacheTTL = 1 * time.Minute
	s.fs.flags.StatCacheTTL = 1 * time.Minute

	in, err := s.LookUpInode(t, prefix[0:len(prefix)-1])
	t.Assert(err, IsNil)
	t.Assert(in.dir, NotNil)

	s.getRoot(t).dir.seqOpenDirScore = 2
	s.readDirIntoCache(t, in.Id)

	// should have slurped these
	in = in.findChild("fileAndDir")
	t.Assert(in, NotNil)
	t.Assert(in.dir, NotNil)

	in = in.findChild("a")
	t.Assert(in, NotNil)

	// because of slurping we've decided that this is a directory,
	// lookup must _not_ talk to S3 again because otherwise we may
	// decide it's a file again because of S3 race
	s.disableS3()
	in, err = s.LookUpInode(t, prefix+"fileAndDir")
	t.Assert(err, IsNil)

	s.assertEntries(t, in, []string{"a"})
}

func (s *GoofysTest) TestAzureDirBlob(t *C) {
	if _, ok := s.cloud.(*AZBlob); !ok {
		t.Skip("only for Azure blob")
	}

	fakedir := []string{"dir2", "dir3"}

	for _, d := range fakedir {
		params := &PutBlobInput{
			Key:  "azuredir/" + d,
			Body: bytes.NewReader([]byte("")),
			Metadata: map[string]*string{
				AzureDirBlobMetadataKey: PString("true"),
			},
			Size: PUInt64(0),
		}
		_, err := s.cloud.PutBlob(params)
		t.Assert(err, IsNil)
	}

	defer func() {
		// because our listing changes dir3 to dir3/, test
		// cleanup could not delete the blob so we wneed to
		// clean up
		for _, d := range fakedir {
			_, err := s.cloud.DeleteBlob(&DeleteBlobInput{Key: "azuredir/" + d})
			t.Assert(err, IsNil)
		}
	}()

	s.setupBlobs(s.cloud, t, map[string]*string{
		// "azuredir/dir" would have gone here
		"azuredir/dir3,/":           nil,
		"azuredir/dir3/file1":       nil,
		"azuredir/dir345_is_a_file": nil,
	})

	head, err := s.cloud.HeadBlob(&HeadBlobInput{Key: "azuredir/dir3"})
	t.Assert(err, IsNil)
	t.Assert(head.IsDirBlob, Equals, true)

	head, err = s.cloud.HeadBlob(&HeadBlobInput{Key: "azuredir/dir345_is_a_file"})
	t.Assert(err, IsNil)
	t.Assert(head.IsDirBlob, Equals, false)

	list, err := s.cloud.ListBlobs(&ListBlobsInput{Prefix: PString("azuredir/")})
	t.Assert(err, IsNil)

	// for flat listing, we rename `dir3` to `dir3/` and add it to Items,
	// `dir3` normally sorts before `dir3./`, but after the rename `dir3/` should
	// sort after `dir3./`
	t.Assert(len(list.Items), Equals, 5)
	t.Assert(*list.Items[0].Key, Equals, "azuredir/dir2/")
	t.Assert(*list.Items[1].Key, Equals, "azuredir/dir3,/")
	t.Assert(*list.Items[2].Key, Equals, "azuredir/dir3/")
	t.Assert(*list.Items[3].Key, Equals, "azuredir/dir3/file1")
	t.Assert(*list.Items[4].Key, Equals, "azuredir/dir345_is_a_file")
	t.Assert(sort.IsSorted(sortBlobItemOutput(list.Items)), Equals, true)

	list, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix:    PString("azuredir/"),
		Delimiter: PString("/"),
	})
	t.Assert(err, IsNil)

	// for delimited listing, we remove `dir3` from items and add `dir3/` to prefixes,
	// which should already be there
	t.Assert(len(list.Items), Equals, 1)
	t.Assert(*list.Items[0].Key, Equals, "azuredir/dir345_is_a_file")

	t.Assert(len(list.Prefixes), Equals, 3)
	t.Assert(*list.Prefixes[0].Prefix, Equals, "azuredir/dir2/")
	t.Assert(*list.Prefixes[1].Prefix, Equals, "azuredir/dir3,/")
	t.Assert(*list.Prefixes[2].Prefix, Equals, "azuredir/dir3/")

	// finally check that we are reading them in correctly
	in, err := s.LookUpInode(t, "azuredir")
	t.Assert(err, IsNil)

	s.assertEntries(t, in, []string{"dir2", "dir3", "dir3,", "dir345_is_a_file"})
}

func (s *GoofysTest) TestReadDirLarge(t *C) {
	root := s.getRoot(t)
	root.dir.mountPrefix = "empty_dir"

	blobs := make(map[string]*string)
	expect := make([]string, 0)
	for i := 0; i < 998; i++ {
		blobs[fmt.Sprintf("empty_dir/%04vd/%v", i, i)] = nil
		expect = append(expect, fmt.Sprintf("%04vd", i))
	}
	blobs["empty_dir/0998f"] = nil
	blobs["empty_dir/0999f"] = nil
	blobs["empty_dir/1000f"] = nil
	expect = append(expect, "0998f")
	expect = append(expect, "0999f")
	expect = append(expect, "1000f")

	for i := 1001; i < 1003; i++ {
		blobs[fmt.Sprintf("empty_dir/%04vd/%v", i, i)] = nil
		expect = append(expect, fmt.Sprintf("%04vd", i))
	}

	s.setupBlobs(s.cloud, t, blobs)

	dh := root.OpenDir()
	defer dh.CloseDir()

	children := namesOf(s.readDirFully(t, dh))
	sort.Strings(children)

	t.Assert(children, DeepEquals, expect)
}

func (s *GoofysTest) newBackend(t *C, bucket string, createBucket bool) (cloud StorageBackend) {
	var err error
	switch s.cloud.(type) {
	case *S3Backend:
		config, _ := s.fs.flags.Backend.(*S3Config)
		cloud, err = NewS3(bucket, s.fs.flags, config)
		t.Assert(err, IsNil)

		if s3, ok := cloud.(*S3Backend); ok {
			s3.aws = hasEnv("AWS")

			if !hasEnv("MINIO") {
				s3.Handlers.Sign.Clear()
				s3.Handlers.Sign.PushBack(SignV2)
				s3.Handlers.Sign.PushBackNamed(corehandlers.BuildContentLengthHandler)
			}
		}
	case *GCS3:
		config, _ := s.fs.flags.Backend.(*S3Config)
		cloud, err = NewGCS3(bucket, s.fs.flags, config)
		t.Assert(err, IsNil)
	case *AZBlob:
		config, _ := s.fs.flags.Backend.(*AZBlobConfig)
		cloud, err = NewAZBlob(bucket, config)
		t.Assert(err, IsNil)
	case *ADLv1:
		config, _ := s.fs.flags.Backend.(*ADLv1Config)
		cloud, err = NewADLv1(bucket, s.fs.flags, config)
		t.Assert(err, IsNil)
	case *ADLv2:
		config, _ := s.fs.flags.Backend.(*ADLv2Config)
		cloud, err = NewADLv2(bucket, s.fs.flags, config)
		t.Assert(err, IsNil)
	default:
		t.Fatal("unknown backend")
	}

	if createBucket {
		_, err = cloud.MakeBucket(&MakeBucketInput{})
		t.Assert(err, IsNil)

		s.removeBucket = append(s.removeBucket, cloud)
	}

	return
}

func (s *GoofysTest) TestVFS(t *C) {
	bucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	cloud2 := s.newBackend(t, bucket, true)

	// "mount" this 2nd cloud
	in, err := s.LookUpInode(t, "dir4")
	t.Assert(in, NotNil)
	t.Assert(err, IsNil)

	in.dir.cloud = cloud2
	in.dir.mountPrefix = "cloud2Prefix/"

	rootCloud, rootPath := in.cloud()
	t.Assert(rootCloud, NotNil)
	t.Assert(rootCloud == cloud2, Equals, true)
	t.Assert(rootPath, Equals, "cloud2Prefix")

	// the mount would shadow dir4/file5
	_, err = in.LookUp("file5")
	t.Assert(err, Equals, fuse.ENOENT)

	_, fh := in.Create("testfile", fuseops.OpMetadata{uint32(os.Getpid())})
	err = fh.FlushFile()
	t.Assert(err, IsNil)

	resp, err := cloud2.GetBlob(&GetBlobInput{Key: "cloud2Prefix/testfile"})
	t.Assert(err, IsNil)
	defer resp.Body.Close()

	err = s.getRoot(t).Rename("file1", in, "file2")
	t.Assert(err, Equals, syscall.EINVAL)

	_, err = in.MkDir("subdir")
	t.Assert(err, IsNil)

	subdirKey := "cloud2Prefix/subdir"
	if !cloud2.Capabilities().DirBlob {
		subdirKey += "/"
	}

	_, err = cloud2.HeadBlob(&HeadBlobInput{Key: subdirKey})
	t.Assert(err, IsNil)

	subdir, err := s.LookUpInode(t, "dir4/subdir")
	t.Assert(err, IsNil)
	t.Assert(subdir, NotNil)
	t.Assert(subdir.dir, NotNil)
	t.Assert(subdir.dir.cloud, IsNil)

	subdirCloud, subdirPath := subdir.cloud()
	t.Assert(subdirCloud, NotNil)
	t.Assert(subdirCloud == cloud2, Equals, true)
	t.Assert(subdirPath, Equals, "cloud2Prefix/subdir")

	// create another file inside subdir to make sure that our
	// mount check is correct for dir inside the root
	_, fh = subdir.Create("testfile2", fuseops.OpMetadata{uint32(os.Getpid())})
	err = fh.FlushFile()
	t.Assert(err, IsNil)

	resp, err = cloud2.GetBlob(&GetBlobInput{Key: "cloud2Prefix/subdir/testfile2"})
	t.Assert(err, IsNil)
	defer resp.Body.Close()

	err = subdir.Rename("testfile2", in, "testfile2")
	t.Assert(err, IsNil)

	_, err = cloud2.GetBlob(&GetBlobInput{Key: "cloud2Prefix/subdir/testfile2"})
	t.Assert(err, Equals, fuse.ENOENT)

	resp, err = cloud2.GetBlob(&GetBlobInput{Key: "cloud2Prefix/testfile2"})
	t.Assert(err, IsNil)
	defer resp.Body.Close()

	err = in.Rename("testfile2", subdir, "testfile2")
	t.Assert(err, IsNil)

	_, err = cloud2.GetBlob(&GetBlobInput{Key: "cloud2Prefix/testfile2"})
	t.Assert(err, Equals, fuse.ENOENT)

	resp, err = cloud2.GetBlob(&GetBlobInput{Key: "cloud2Prefix/subdir/testfile2"})
	t.Assert(err, IsNil)
	defer resp.Body.Close()
}

func (s *GoofysTest) TestMountsList(t *C) {
	s.fs.flags.TypeCacheTTL = 1 * time.Minute
	s.fs.flags.StatCacheTTL = 1 * time.Minute

	bucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	cloud := s.newBackend(t, bucket, true)

	root := s.getRoot(t)
	rootCloud := root.dir.cloud

	s.fs.MountAll([]*Mount{
		&Mount{"dir4/cloud1", cloud, "", false},
	})

	in, err := s.LookUpInode(t, "dir4")
	t.Assert(in, NotNil)
	t.Assert(err, IsNil)
	t.Assert(int(in.Id), Equals, 2)

	s.readDirIntoCache(t, in.Id)
	// ensure that listing is listing mounts and root bucket in one go
	root.dir.cloud = nil

	s.assertEntries(t, in, []string{"cloud1", "file5"})

	c1, err := s.LookUpInode(t, "dir4/cloud1")
	t.Assert(err, IsNil)
	t.Assert(*c1.Name, Equals, "cloud1")
	t.Assert(c1.dir.cloud == cloud, Equals, true)
	t.Assert(int(c1.Id), Equals, 3)

	// pretend we've passed the normal cache ttl
	s.fs.flags.TypeCacheTTL = 0
	s.fs.flags.StatCacheTTL = 0

	// listing root again should not overwrite the mounts
	root.dir.cloud = rootCloud

	s.readDirIntoCache(t, in.Parent.Id)
	s.assertEntries(t, in, []string{"cloud1", "file5"})

	c1, err = s.LookUpInode(t, "dir4/cloud1")
	t.Assert(err, IsNil)
	t.Assert(*c1.Name, Equals, "cloud1")
	t.Assert(c1.dir.cloud == cloud, Equals, true)
	t.Assert(int(c1.Id), Equals, 3)
}

func (s *GoofysTest) TestMountsNewDir(t *C) {
	bucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	cloud := s.newBackend(t, bucket, true)

	_, err := s.LookUpInode(t, "dir5")
	t.Assert(err, NotNil)
	t.Assert(err, Equals, fuse.ENOENT)

	s.fs.MountAll([]*Mount{
		&Mount{"dir5/cloud1", cloud, "", false},
	})

	in, err := s.LookUpInode(t, "dir5")
	t.Assert(err, IsNil)
	t.Assert(in.isDir(), Equals, true)

	c1, err := s.LookUpInode(t, "dir5/cloud1")
	t.Assert(err, IsNil)
	t.Assert(c1.isDir(), Equals, true)
	t.Assert(c1.dir.cloud, Equals, cloud)
}

func (s *GoofysTest) TestMountsNewMounts(t *C) {
	bucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	cloud := s.newBackend(t, bucket, true)

	// "mount" this 2nd cloud
	in, err := s.LookUpInode(t, "dir4")
	t.Assert(in, NotNil)
	t.Assert(err, IsNil)

	s.fs.MountAll([]*Mount{
		&Mount{"dir4/cloud1", cloud, "", false},
	})

	s.readDirIntoCache(t, in.Id)

	c1, err := s.LookUpInode(t, "dir4/cloud1")
	t.Assert(err, IsNil)
	t.Assert(*c1.Name, Equals, "cloud1")
	t.Assert(c1.dir.cloud == cloud, Equals, true)

	_, err = s.LookUpInode(t, "dir4/cloud2")
	t.Assert(err, Equals, fuse.ENOENT)

	s.fs.MountAll([]*Mount{
		&Mount{"dir4/cloud1", cloud, "", false},
		&Mount{"dir4/cloud2", cloud, "cloudprefix", false},
	})

	c2, err := s.LookUpInode(t, "dir4/cloud2")
	t.Assert(err, IsNil)
	t.Assert(*c2.Name, Equals, "cloud2")
	t.Assert(c2.dir.cloud == cloud, Equals, true)
	t.Assert(c2.dir.mountPrefix, Equals, "cloudprefix")
}

func (s *GoofysTest) TestMountsError(t *C) {
	bucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	var cloud StorageBackend
	if s3, ok := s.cloud.(*S3Backend); ok {
		// S3Backend currently doesn't detect bucket doesn't exist
		flags := *s3.flags
		config := *s3.config
		flags.Endpoint = "0.0.0.0:0"
		var err error
		cloud, err = NewS3(bucket, &flags, &config)
		t.Assert(err, IsNil)
	} else if _, ok := s.cloud.(*ADLv1); ok {
		cloud = s.newBackend(t, bucket, true)
		adlCloud, _ := cloud.(*ADLv1)
		account := adlCloud.account
		adlCloud.account = ""
		defer func() { adlCloud.account = account }()
	} else if _, ok := s.cloud.(*ADLv2); ok {
		// ADLv2 currently doesn't detect bucket doesn't exist
		cloud = s.newBackend(t, bucket, true)
		adlCloud, _ := cloud.(*ADLv2)
		account := adlCloud.client.BaseClient.AccountName
		adlCloud.client.BaseClient.AccountName = ""
		defer func() { adlCloud.client.BaseClient.AccountName = account }()
	} else {
		cloud = s.newBackend(t, bucket, false)
	}

	s.fs.MountAll([]*Mount{
		&Mount{"dir4/newerror", StorageBackendInitError{
			fmt.Errorf("foo"),
		}, "errprefix1", false},
		&Mount{"dir4/initerror", &StorageBackendInitWrapper{
			StorageBackend: cloud,
			initKey:        "foobar",
		}, "errprefix2", false},
	})

	errfile, err := s.LookUpInode(t, "dir4/newerror/"+INIT_ERR_BLOB)
	t.Assert(err, IsNil)
	t.Assert(errfile.isDir(), Equals, false)

	_, err = s.LookUpInode(t, "dir4/newerror/not_there")
	t.Assert(err, Equals, fuse.ENOENT)

	errfile, err = s.LookUpInode(t, "dir4/initerror/"+INIT_ERR_BLOB)
	t.Assert(err, IsNil)
	t.Assert(errfile.isDir(), Equals, false)

	_, err = s.LookUpInode(t, "dir4/initerror/not_there")
	t.Assert(err, Equals, fuse.ENOENT)
}

func (s *GoofysTest) TestMountsMultiLevel(t *C) {
	s.fs.flags.TypeCacheTTL = 1 * time.Minute

	bucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	cloud := s.newBackend(t, bucket, true)

	s.fs.MountAll([]*Mount{
		&Mount{"dir4/sub/dir", cloud, "", false},
	})

	sub, err := s.LookUpInode(t, "dir4/sub")
	t.Assert(err, IsNil)
	t.Assert(sub.isDir(), Equals, true)

	s.assertEntries(t, sub, []string{"dir"})
}

func (s *GoofysTest) TestMountsNested(t *C) {
	bucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	cloud := s.newBackend(t, bucket, true)
	s.testMountsNested(t, cloud, []*Mount{
		&Mount{"dir5/in/a/dir", cloud, "a/dir/", false},
		&Mount{"dir5/in/", cloud, "b/", false},
	})
}

// test that mount order doesn't matter for nested mounts
func (s *GoofysTest) TestMountsNestedReversed(t *C) {
	bucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	cloud := s.newBackend(t, bucket, true)
	s.testMountsNested(t, cloud, []*Mount{
		&Mount{"dir5/in/", cloud, "b/", false},
		&Mount{"dir5/in/a/dir", cloud, "a/dir/", false},
	})
}

func (s *GoofysTest) testMountsNested(t *C, cloud StorageBackend,
	mounts []*Mount) {

	_, err := s.LookUpInode(t, "dir5")
	t.Assert(err, NotNil)
	t.Assert(err, Equals, fuse.ENOENT)

	s.fs.MountAll(mounts)

	in, err := s.LookUpInode(t, "dir5")
	t.Assert(err, IsNil)

	s.readDirIntoCache(t, in.Id)

	// make sure all the intermediate dirs never expire
	time.Sleep(time.Second)
	dir_in, err := s.LookUpInode(t, "dir5/in")
	t.Assert(err, IsNil)
	t.Assert(*dir_in.Name, Equals, "in")

	s.readDirIntoCache(t, dir_in.Id)

	dir_a, err := s.LookUpInode(t, "dir5/in/a")
	t.Assert(err, IsNil)
	t.Assert(*dir_a.Name, Equals, "a")

	s.assertEntries(t, dir_a, []string{"dir"})

	dir_dir, err := s.LookUpInode(t, "dir5/in/a/dir")
	t.Assert(err, IsNil)
	t.Assert(*dir_dir.Name, Equals, "dir")
	t.Assert(dir_dir.dir.cloud == cloud, Equals, true)

	_, fh := dir_in.Create("testfile", fuseops.OpMetadata{uint32(os.Getpid())})
	err = fh.FlushFile()
	t.Assert(err, IsNil)

	resp, err := cloud.GetBlob(&GetBlobInput{Key: "b/testfile"})
	t.Assert(err, IsNil)
	defer resp.Body.Close()

	_, fh = dir_dir.Create("testfile", fuseops.OpMetadata{uint32(os.Getpid())})
	err = fh.FlushFile()
	t.Assert(err, IsNil)

	resp, err = cloud.GetBlob(&GetBlobInput{Key: "a/dir/testfile"})
	t.Assert(err, IsNil)
	defer resp.Body.Close()

	s.assertEntries(t, in, []string{"in"})
}

func verifyFileData(t *C, mountPoint string, path string, content *string) {
	if !strings.HasSuffix(mountPoint, "/") {
		mountPoint = mountPoint + "/"
	}
	path = mountPoint + path
	data, err := ioutil.ReadFile(path)
	comment := Commentf("failed while verifying %v", path)
	if content != nil {
		t.Assert(err, IsNil, comment)
		t.Assert(strings.TrimSpace(string(data)), Equals, *content, comment)
	} else {
		t.Assert(err, Not(IsNil), comment)
		t.Assert(strings.Contains(err.Error(), "no such file or directory"), Equals, true, comment)
	}
}

func (s *GoofysTest) TestNestedMountUnmountSimple(t *C) {
	childBucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	childCloud := s.newBackend(t, childBucket, true)

	parFileContent := "parent"
	childFileContent := "child"
	parEnv := map[string]*string{
		"childmnt/x/in_child_and_par": &parFileContent,
		"childmnt/x/in_par_only":      &parFileContent,
		"nonchildmnt/something":       &parFileContent,
	}
	childEnv := map[string]*string{
		"x/in_child_only":    &childFileContent,
		"x/in_child_and_par": &childFileContent,
	}
	s.setupBlobs(s.cloud, t, parEnv)
	s.setupBlobs(childCloud, t, childEnv)

	rootMountPath := "/tmp/fusetesting/" + RandStringBytesMaskImprSrc(16)
	s.mount(t, rootMountPath)
	defer s.umount(t, rootMountPath)
	// Files under /tmp/fusetesting/ should all be from goofys root.
	verifyFileData(t, rootMountPath, "childmnt/x/in_par_only", &parFileContent)
	verifyFileData(t, rootMountPath, "childmnt/x/in_child_and_par", &parFileContent)
	verifyFileData(t, rootMountPath, "nonchildmnt/something", &parFileContent)
	verifyFileData(t, rootMountPath, "childmnt/x/in_child_only", nil)

	childMount := &Mount{"childmnt", childCloud, "", false}
	s.fs.Mount(childMount)
	// Now files under /tmp/fusetesting/childmnt should be from childBucket
	verifyFileData(t, rootMountPath, "childmnt/x/in_par_only", nil)
	verifyFileData(t, rootMountPath, "childmnt/x/in_child_and_par", &childFileContent)
	verifyFileData(t, rootMountPath, "childmnt/x/in_child_only", &childFileContent)
	// /tmp/fusetesting/nonchildmnt should be from parent bucket.
	verifyFileData(t, rootMountPath, "nonchildmnt/something", &parFileContent)

	s.fs.Unmount(childMount.name)
	// Child is unmounted. So files under /tmp/fusetesting/ should all be from goofys root.
	verifyFileData(t, rootMountPath, "childmnt/x/in_par_only", &parFileContent)
	verifyFileData(t, rootMountPath, "childmnt/x/in_child_and_par", &parFileContent)
	verifyFileData(t, rootMountPath, "nonchildmnt/something", &parFileContent)
	verifyFileData(t, rootMountPath, "childmnt/x/in_child_only", nil)
}

func (s *GoofysTest) TestUnmountBucketWithChild(t *C) {
	// This bucket will be mounted at ${goofysroot}/c
	cBucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	cCloud := s.newBackend(t, cBucket, true)

	// This bucket will be mounted at ${goofysroot}/c/c
	ccBucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	ccCloud := s.newBackend(t, ccBucket, true)

	pFileContent := "parent"
	cFileContent := "child"
	ccFileContent := "childchild"
	pEnv := map[string]*string{
		"c/c/x/foo": &pFileContent,
	}
	cEnv := map[string]*string{
		"c/x/foo": &cFileContent,
	}
	ccEnv := map[string]*string{
		"x/foo": &ccFileContent,
	}

	s.setupBlobs(s.cloud, t, pEnv)
	s.setupBlobs(cCloud, t, cEnv)
	s.setupBlobs(ccCloud, t, ccEnv)

	rootMountPath := "/tmp/fusetesting/" + RandStringBytesMaskImprSrc(16)
	s.mount(t, rootMountPath)
	defer s.umount(t, rootMountPath)
	// c/c/foo should come from root mount.
	verifyFileData(t, rootMountPath, "c/c/x/foo", &pFileContent)

	cMount := &Mount{"c", cCloud, "", false}
	s.fs.Mount(cMount)
	// c/c/foo should come from "c" mount.
	verifyFileData(t, rootMountPath, "c/c/x/foo", &cFileContent)

	ccMount := &Mount{"c/c", ccCloud, "", false}
	s.fs.Mount(ccMount)
	// c/c/foo should come from "c/c" mount.
	verifyFileData(t, rootMountPath, "c/c/x/foo", &ccFileContent)

	s.fs.Unmount(cMount.name)
	// c/c/foo should still come from "c/c" mount.
	verifyFileData(t, rootMountPath, "c/c/x/foo", &ccFileContent)
}

func (s *GoofysTest) TestRmImplicitDir(t *C) {
	mountPoint := "/tmp/mnt" + s.fs.bucket

	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	defer os.Chdir("/")

	dir, err := os.Open(mountPoint + "/dir2")
	t.Assert(err, IsNil)
	defer dir.Close()

	err = dir.Chdir()
	t.Assert(err, IsNil)

	err = os.RemoveAll(mountPoint + "/dir2")
	t.Assert(err, IsNil)

	root, err := os.Open(mountPoint)
	t.Assert(err, IsNil)
	defer root.Close()

	files, err := root.Readdirnames(0)
	t.Assert(err, IsNil)
	t.Assert(files, DeepEquals, []string{
		"dir1", "dir4", "empty_dir", "empty_dir2", "file1", "file2", "zero",
	})
}
