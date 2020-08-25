package internal

import (
	"github.com/kahing/goofys/api/common"

	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"
	"syscall"

	"github.com/jacobsa/fuse"
	"golang.org/x/oauth2/google"
	"golang.org/x/sync/errgroup"
	. "gopkg.in/check.v1"
)

type GCSTestSpec struct {
	objCount           int    // number of non-prefix blobs uploaded to the bucket
	prefixCount        int    // number of prefix blobs uploaded to the bucket
	existingObjKey     string // test setup will add a blob with this name
	existingObjContent string
	existingObjLen     uint64
	defaultObjLen      uint64 // blobs created in the setup will be of this length
	nonPrefixObjects   []string
	prefixObjects      []string
	prefixes           []string           // these are the prefixes of the prefixObjects
	metadata           map[string]*string // default Metadata for all blobs
	contentType        *string            // default Content-Type for all blobs
	// will be reset in SetUpTest, optionally modified on each test, and cleaned up in TearDownTest
	additionalBlobs []string
}

type GCSBackendTest struct {
	gcsBackend *GCSBackend
	bucketName string
	testSpec   GCSTestSpec
	chunkSize  int
}

var _ = Suite(&GCSBackendTest{})

func (s *GCSBackendTest) getGCSTestConfig() (*common.GCSConfig, error) {
	config := common.NewGCSConfig()
	config.ChunkSize = s.chunkSize
	return config, nil
}

func (s *GCSBackendTest) getGCSTestBackend(gcsBucket string) (*GCSBackend, error) {
	config, err := s.getGCSTestConfig()
	if err != nil {
		return nil, err
	}
	spec, err := ParseBucketSpec(gcsBucket)
	if err != nil {
		return nil, err
	}
	gcsBackend, err := NewGCS(spec.Bucket, config)

	return gcsBackend, err
}

func (s *GCSBackendTest) SetUpSuite(c *C) {
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		c.Skip("Skipping because GOOGLE_APPLICATION_CREDENTIALS variable is unset.")
	}

	bktName := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	s.bucketName = bktName
	s.testSpec = GCSTestSpec{
		objCount:      10,
		prefixCount:   5,
		defaultObjLen: 16,
		metadata: map[string]*string{
			"foo": PString("bar"),
			"bar": PString("baz"),
		},
		contentType: PString("text/plain"),
	}
	s.chunkSize = 1 * 1024 * 1024
	s.gcsBackend, _ = s.getGCSTestBackend(fmt.Sprintf("gs://%s", bktName))

	_, err := s.gcsBackend.MakeBucket(&MakeBucketInput{})
	c.Assert(err, IsNil)

	errGroup, _ := errgroup.WithContext(context.Background())
	objLen := s.testSpec.defaultObjLen

	// create object without prefix
	// files is stored as 00_blob, 01_blob, ... 09_blob (10 items)
	for i := 0; i < s.testSpec.objCount; i++ {
		objKey := fmt.Sprintf("%02d_blob", i)
		objContent := RandStringBytesMaskImprSrc(int(objLen))

		errGroup.Go(func() error {
			_, err := s.gcsBackend.PutBlob(&PutBlobInput{
				Key:         objKey,
				Body:        bytes.NewReader([]byte(objContent)),
				Metadata:    s.testSpec.metadata,
				ContentType: s.testSpec.contentType,
			})
			c.Assert(err, IsNil)

			// verify object attr after a put blob
			objAttr, err := s.gcsBackend.HeadBlob(&HeadBlobInput{Key: objKey})
			c.Assert(err, IsNil)
			c.Assert(NilStr(objAttr.Key), Equals, objKey)
			c.Assert(objAttr.Size, Equals, uint64(int(objLen)))
			c.Assert(NilStr(objAttr.ContentType), Equals, NilStr(s.testSpec.contentType))
			c.Assert(objAttr.Metadata, DeepEquals, s.testSpec.metadata)

			return err
		})

		// set the existing object to be the first object
		if s.testSpec.existingObjKey == "" {
			s.testSpec.existingObjKey = objKey
			s.testSpec.existingObjContent = objContent
			s.testSpec.existingObjLen = objLen
		}
		s.testSpec.nonPrefixObjects = append(s.testSpec.nonPrefixObjects, objKey)
	}

	// create object with prefix
	// file is stored as 00_prefix/blob, 01_prefix/blob, ..., 04_prefix/blob (5 items)
	for i := 0; i < s.testSpec.prefixCount; i++ {
		objKey := fmt.Sprintf("%02d_prefix/blob", i)
		objContent := RandStringBytesMaskImprSrc(int(objLen))
		errGroup.Go(func() error {
			_, err := s.gcsBackend.PutBlob(&PutBlobInput{
				Key:         objKey,
				Body:        bytes.NewReader([]byte(objContent)),
				Metadata:    s.testSpec.metadata,
				ContentType: s.testSpec.contentType,
			})
			c.Assert(err, IsNil)

			objAttr, err := s.gcsBackend.HeadBlob(&HeadBlobInput{Key: objKey})
			c.Assert(err, IsNil)
			c.Assert(NilStr(objAttr.Key), Equals, objKey)
			c.Assert(objAttr.Size, Equals, uint64(int(objLen)))
			c.Assert(NilStr(objAttr.ContentType), Equals, NilStr(s.testSpec.contentType))
			c.Assert(objAttr.Metadata, DeepEquals, s.testSpec.metadata)
			return err
		})

		s.testSpec.prefixObjects = append(s.testSpec.prefixObjects, objKey)
		dir, _ := path.Split(objKey)
		s.testSpec.prefixes = append(s.testSpec.prefixes, dir)
	}

	err = errGroup.Wait()
	c.Assert(err, IsNil)
}

func (s *GCSBackendTest) TearDownSuite(c *C) {
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		c.Skip("Skipping because GOOGLE_APPLICATION_CREDENTIALS variable is unset.")
	}
	out, err := s.gcsBackend.ListBlobs(&ListBlobsInput{})
	c.Assert(err, IsNil)

	var items []string
	for _, item := range out.Items {
		items = append(items, NilStr(item.Key))
	}

	_, err = s.gcsBackend.DeleteBlobs(&DeleteBlobsInput{Items: items})
	c.Assert(err, IsNil)

	_, err = s.gcsBackend.RemoveBucket(&RemoveBucketInput{})
	c.Assert(err, IsNil)
}

func (s *GCSBackendTest) SetUpTest(c *C) {
	s.testSpec.additionalBlobs = []string{}
}

func (s *GCSBackendTest) TearDownTest(c *C) {
	_, err := s.gcsBackend.DeleteBlobs(&DeleteBlobsInput{s.testSpec.additionalBlobs})
	// successful deletion should be: nil error or not found
	c.Assert(err == nil || err == syscall.ENOENT, Equals, true)
}

func (s *GCSBackendTest) TestGCSBackend_Init_Authenticated(c *C) {
	// No error when accessing existing private bucket.
	err := s.gcsBackend.Init(s.bucketName)
	c.Assert(err, IsNil)

	// Not Found error when accessing nonexistent bucket.
	randBktName := RandStringBytesMaskImprSrc(16)
	gcsBackend, err := s.getGCSTestBackend(randBktName)
	c.Assert(err, IsNil)
	err = gcsBackend.Init(RandStringBytesMaskImprSrc(16))
	c.Assert(err, ErrorMatches, fmt.Sprintf("bucket %s does not exist", randBktName))

	// No error when accessing public bucket.
	gcsBackend, err = s.getGCSTestBackend("gcp-public-data-nexrad-l2")
	c.Assert(err, IsNil)
	err = gcsBackend.Init(RandStringBytesMaskImprSrc(16))
	c.Assert(err, IsNil)
}

func (s *GCSBackendTest) TestGCSBackend_Init_Unauthenticated(c *C) {
	defaultCredentials := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	defer func() {
		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", defaultCredentials)
	}()
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")

	credentials, err := google.FindDefaultCredentials(context.Background())
	if credentials != nil {
		c.Skip("Skipping this test because credentials still exist in the environment.")
	}

	// Access error on existing private bucket.
	gcsBackend, err := s.getGCSTestBackend(s.bucketName)
	c.Assert(err, IsNil)
	err = gcsBackend.Init(RandStringBytesMaskImprSrc(15))
	c.Assert(err, Equals, syscall.EACCES)

	// Not Found error when accessing nonexistent bucket.
	randBktName := RandStringBytesMaskImprSrc(16)
	gcsBackend, err = s.getGCSTestBackend(randBktName)
	c.Assert(err, IsNil)
	err = gcsBackend.Init(RandStringBytesMaskImprSrc(16))
	c.Assert(err, ErrorMatches, fmt.Sprintf("bucket %s does not exist", randBktName))

	// No error when accessing public bucket.
	gcsBackend, err = s.getGCSTestBackend("gcp-public-data-nexrad-l2")
	c.Assert(err, IsNil)
	err = gcsBackend.Init(RandStringBytesMaskImprSrc(16))
	c.Assert(err, IsNil)
}

func (s *GCSBackendTest) TestGCSBackend_Init_Authenticated_ReadOnlyAccess(c *C) {
	// READONLY_GOOGLE_APPLICATION_CREDENTIALS is a credential that is authorized to read-only access to the bucket
	if os.Getenv("READONLY_GOOGLE_APPLICATION_CREDENTIALS") == "" {
		c.Skip("Skipping because READONLY_GOOGLE_APPLICATION_CREDENTIALS is unset.")
	}

	defaultCredentials := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	defer func() {
		os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", defaultCredentials)
	}()
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", os.Getenv("READONLY_GOOGLE_APPLICATION_CREDENTIALS"))

	// No error when accessing existing private bucket.
	gcsBackend, err := s.getGCSTestBackend(s.bucketName)
	c.Assert(err, IsNil)
	err = gcsBackend.Init(s.bucketName)
	c.Assert(err, IsNil)

	// Access error when trying to modify object.
	_, err = gcsBackend.DeleteBlob(&DeleteBlobInput{Key: s.testSpec.existingObjKey})
	c.Assert(err, Equals, syscall.EACCES)

	// Not Found error when accessing nonexistent bucket.
	randBktName := RandStringBytesMaskImprSrc(16)
	gcsBackend, err = s.getGCSTestBackend(randBktName)
	c.Assert(err, IsNil)
	err = gcsBackend.Init(RandStringBytesMaskImprSrc(16))
	c.Assert(err, ErrorMatches, fmt.Sprintf("bucket %s does not exist", randBktName))

	// No error when accessing public bucket.
	gcsBackend, err = s.getGCSTestBackend("gcp-public-data-nexrad-l2")
	c.Assert(err, IsNil)
	err = gcsBackend.Init(RandStringBytesMaskImprSrc(16))
	c.Assert(err, IsNil)
}

func (s *GCSBackendTest) TestGCSBackend_HeadBlob_NotExist(c *C) {
	objKey := RandStringBytesMaskImprSrc(16)
	_, err := s.gcsBackend.HeadBlob(&HeadBlobInput{Key: objKey})
	c.Assert(err, Equals, fuse.ENOENT)
}

func (s *GCSBackendTest) TestGCSBackend_GetBlob_NotExist(c *C) {
	objKey := RandStringBytesMaskImprSrc(16)
	_, err := s.gcsBackend.GetBlob(&GetBlobInput{Key: objKey})
	c.Assert(err, Equals, fuse.ENOENT)
}

func (s *GCSBackendTest) TestGCSBackend_GetFullBlob(c *C) {
	fullBlob, err := s.gcsBackend.GetBlob(&GetBlobInput{Key: s.testSpec.existingObjKey})
	c.Assert(err, IsNil)
	defer fullBlob.Body.Close()

	actualContent, err := ioutil.ReadAll(fullBlob.Body)
	c.Assert(err, IsNil)
	c.Assert(string(actualContent), Equals, s.testSpec.existingObjContent)
}

func (s *GCSBackendTest) TestGCSBackend_GetPartialBlob(c *C) {
	var startOffset uint64 = 4
	var count uint64 = 8
	partialBlob, err := s.gcsBackend.GetBlob(&GetBlobInput{
		Key:   s.testSpec.existingObjKey,
		Start: startOffset,
		Count: count,
	})
	c.Assert(err, IsNil)
	defer partialBlob.Body.Close()

	actualContent, err := ioutil.ReadAll(partialBlob.Body)
	c.Assert(err, IsNil)
	c.Assert(string(actualContent), Equals, s.testSpec.existingObjContent[startOffset:startOffset+count])
}

func (s *GCSBackendTest) TestGCSBackend_PutBlob_NilBody(c *C) {
	objKey := RandStringBytesMaskImprSrc(int(s.testSpec.defaultObjLen))
	s.addToAdditionalBlobs(objKey)

	_, err := s.gcsBackend.PutBlob(&PutBlobInput{Key: objKey, Body: nil})
	c.Assert(err, IsNil)
}

func (s *GCSBackendTest) addToAdditionalBlobs(objKey string) {
	s.testSpec.additionalBlobs = append(s.testSpec.additionalBlobs, objKey)
}

func (s *GCSBackendTest) TestGCSBackend_GetGzipEncodedBlob(c *C) {
	objKey := "gzipObj"
	s.addToAdditionalBlobs(objKey)
	originalContent := RandStringBytesMaskImprSrc(int(s.testSpec.defaultObjLen))

	// Goofys cannot upload a file with content-encoding: gzip. So we will use GCS sdk directly to create such blob
	s.writeGzipEncodedFile(c, objKey, originalContent)

	gzipBlob, err := s.gcsBackend.GetBlob(&GetBlobInput{Key: objKey})
	c.Assert(err, IsNil)
	defer gzipBlob.Body.Close()

	actualContent, err := ioutil.ReadAll(gzipBlob.Body)
	c.Assert(string(actualContent), Equals, originalContent)
}

func (s *GCSBackendTest) writeGzipEncodedFile(c *C, objKey string, content string) {
	writer := s.gcsBackend.bucket.Object(objKey).NewWriter(context.Background())
	writer.ContentEncoding = "gzip"
	_, err := writer.Write([]byte(content))
	defer writer.Close()
	c.Assert(err, IsNil)
}

func (s *GCSBackendTest) TestGCSBackend_ListBlobs_NoPrefixNoDelim(c *C) {
	// list all objects in bucket as items, no prefix because delim is unset
	listOutputs, err := s.gcsBackend.ListBlobs(&ListBlobsInput{})
	c.Assert(err, IsNil)

	actualOutputs := s.extractAllNamesFromListOutputs(listOutputs)
	s.checkListOutputs(c, actualOutputs, s.testSpec.nonPrefixObjects, s.testSpec.prefixObjects)
	c.Assert(listOutputs.NextContinuationToken, IsNil)
	c.Assert(listOutputs.IsTruncated, Equals, false)
}

func (s *GCSBackendTest) TestGCSBackend_ListBlobs_NoPrefixWithDelim(c *C) {
	// list all objects but separates items and prefixes
	listOutputs, err := s.gcsBackend.ListBlobs(&ListBlobsInput{Delimiter: PString("/")})
	c.Assert(err, IsNil)
	// should contain non prefix objects: 00_blob, ..., 09_blob and prefixes: 00_prefix/, ..., 04_prefix/
	actualOutputs := s.extractAllNamesFromListOutputs(listOutputs)
	s.checkListOutputs(c, actualOutputs, s.testSpec.nonPrefixObjects, s.testSpec.prefixes)
	c.Assert(listOutputs.NextContinuationToken, IsNil)
	c.Assert(listOutputs.IsTruncated, Equals, false)
}

func (s *GCSBackendTest) TestGCSBackend_ListBlobs_WithPrefixNoDelim(c *C) {
	// list all objects that have a matching prefix as items
	listOutputs, err := s.gcsBackend.ListBlobs(&ListBlobsInput{Prefix: PString("00")})
	c.Assert(err, IsNil)
	// should contain all items under prefix: 00_blob & 00_prefix/blob
	actualOutputs := s.extractAllNamesFromListOutputs(listOutputs)
	s.checkListOutputs(c, actualOutputs, s.testSpec.nonPrefixObjects[:1], s.testSpec.prefixObjects[:1])
	c.Assert(listOutputs.NextContinuationToken, IsNil)
	c.Assert(listOutputs.IsTruncated, Equals, false)
}

func (s *GCSBackendTest) TestGCSBackend_ListBlobs_WithPrefixWithDelim(c *C) {
	// lists objects that have a matching prefix and separates non prefix & prefixes
	listOutputs, err := s.gcsBackend.ListBlobs(&ListBlobsInput{
		Prefix:    PString("00"),
		Delimiter: PString("/"),
	})
	c.Assert(err, IsNil)
	// should contain 00_blob and 00_prefix/
	actualOutputs := s.extractAllNamesFromListOutputs(listOutputs)
	s.checkListOutputs(c, actualOutputs, s.testSpec.nonPrefixObjects[:1], s.testSpec.prefixes[:1])
	c.Assert(listOutputs.NextContinuationToken, IsNil)
	c.Assert(listOutputs.IsTruncated, Equals, false)
}

func (s *GCSBackendTest) TestGCSBackend_ListBlobs_StartAfter(c *C) {
	cutoffKey := "04_prefix/blob"
	// list results all come right on or after StartAfter in lexicographical order
	listOutputs, err := s.gcsBackend.ListBlobs(&ListBlobsInput{
		Delimiter:  PString("/"),
		StartAfter: PString(cutoffKey),
	})
	c.Assert(err, IsNil)

	actualOutputs := s.extractAllNamesFromListOutputs(listOutputs)
	// contain elements from 05_blob and 04_prefix/
	s.checkListOutputs(c, actualOutputs, s.testSpec.nonPrefixObjects[5:],
		s.testSpec.prefixes[len(s.testSpec.prefixes)-1:])

	// assert all items are larger or equal to StartAfter prefix (because we used delimiter) in lexicographical order
	cutOffPrefix, _ := path.Split(cutoffKey)
	for _, item := range actualOutputs {
		c.Assert(item >= cutOffPrefix, Equals, true)
	}
}

func (s *GCSBackendTest) TestGCSBackend_ListBlobs_MaxKeysPaginate(c *C) {
	totalKeys := s.testSpec.objCount + s.testSpec.prefixCount
	maxKeys := 1

	var nextContToken *string
	var actualOutputs []string

	// iterate over pages of maxKeys
	for i := 0; i < totalKeys-1; i++ {
		listOutputs, err := s.gcsBackend.ListBlobs(&ListBlobsInput{
			Delimiter:         PString("/"),
			MaxKeys:           PUInt32(uint32(maxKeys)),
			ContinuationToken: nextContToken,
		})
		c.Assert(err, IsNil)
		c.Assert(len(listOutputs.Items)+len(listOutputs.Prefixes), Equals, maxKeys)
		c.Assert(listOutputs.NextContinuationToken, NotNil)
		nextContToken = listOutputs.NextContinuationToken
		actualOutputs = append(actualOutputs, s.extractAllNamesFromListOutputs(listOutputs)...)
	}
	// remaining object in the last page
	listOutputs, err := s.gcsBackend.ListBlobs(&ListBlobsInput{
		Delimiter:         PString("/"),
		MaxKeys:           PUInt32(uint32(maxKeys)),
		ContinuationToken: nextContToken,
	})
	actualOutputs = append(actualOutputs, s.extractAllNamesFromListOutputs(listOutputs)...)
	c.Assert(err, IsNil)
	c.Assert(len(listOutputs.Items)+len(listOutputs.Prefixes) <= maxKeys, Equals, true)
	c.Assert(listOutputs.NextContinuationToken, IsNil)
	s.checkListOutputs(c, actualOutputs, s.testSpec.nonPrefixObjects, s.testSpec.prefixes)
}

func (s *GCSBackendTest) extractAllNamesFromListOutputs(listOutputs *ListBlobsOutput) []string {
	var actualOutputs []string
	for _, item := range listOutputs.Items {
		actualOutputs = append(actualOutputs, NilStr(item.Key))
	}
	for _, prefix := range listOutputs.Prefixes {
		actualOutputs = append(actualOutputs, NilStr(prefix.Prefix))
	}
	return actualOutputs
}

// This check the actual outputs against list of expected outputs
func (s *GCSBackendTest) checkListOutputs(c *C, actualOutputs []string, expectedOutputsArgs ...[]string) {
	var expectedResults []string
	for _, expectedOutputs := range expectedOutputsArgs {
		expectedResults = append(expectedResults, expectedOutputs...)
	}
	sort.Strings(expectedResults)
	sort.Strings(actualOutputs)

	c.Assert(actualOutputs, DeepEquals, expectedResults)
}

func (s *GCSBackendTest) TestGCSBackend_CopyBlob_PreserveMetadata(c *C) {
	srcKey := s.testSpec.existingObjKey
	destKey := RandStringBytesMaskImprSrc(16)
	s.addToAdditionalBlobs(destKey)

	srcAttr, err := s.gcsBackend.HeadBlob(&HeadBlobInput{Key: srcKey})
	c.Assert(err, IsNil)

	_, err = s.gcsBackend.CopyBlob(&CopyBlobInput{
		Source:      srcKey,
		Destination: destKey,
	})
	c.Assert(err, IsNil)

	destAttr, err := s.gcsBackend.HeadBlob(&HeadBlobInput{Key: destKey})
	c.Assert(err, IsNil)
	c.Assert(NilStr(destAttr.ContentType), Equals, NilStr(srcAttr.ContentType))
	c.Assert(destAttr.Metadata, DeepEquals, srcAttr.Metadata)
	c.Assert(srcAttr.ETag, Not(Equals), destAttr.ETag)

	destOut, err := s.gcsBackend.GetBlob(&GetBlobInput{Key: srcKey})
	c.Assert(err, IsNil)
	defer destOut.Body.Close()
	destContent, err := ioutil.ReadAll(destOut.Body)
	c.Assert(string(destContent), Equals, s.testSpec.existingObjContent)
}

func (s *GCSBackendTest) TestGCSBackend_MultipartUpload_BeginAddCommit(c *C) {
	objKey := RandStringBytesMaskImprSrc(16)
	s.addToAdditionalBlobs(objKey)

	commitInput, err := s.gcsBackend.MultipartBlobBegin(&MultipartBlobBeginInput{
		Key: objKey,
	})
	c.Assert(err, IsNil)
	c.Assert(commitInput.NumParts, Equals, uint32(0))

	// We will have numFullChunks+1 chunks. That last chunk is of size lastChunkSize
	var numFullChunks = 2
	lastChunkSize := s.chunkSize - 1
	fileSize := uint64((numFullChunks * s.chunkSize) + lastChunkSize)

	// generate data to simulate MPU behavior
	buf := new(bytes.Buffer)
	data := io.LimitReader(&SeqReader{}, int64(fileSize))
	_, err = buf.ReadFrom(data)
	c.Assert(err, IsNil)
	reader := bytes.NewReader(buf.Bytes())

	// add the full chunks
	for i := 0; i < numFullChunks; i++ {
		sectionReader := io.NewSectionReader(reader, int64(i*s.chunkSize), int64(s.chunkSize))
		addOut, err := s.gcsBackend.MultipartBlobAdd(&MultipartBlobAddInput{
			Commit:     commitInput,
			PartNumber: uint32(i + 1),
			Body:       sectionReader,
			Size:       uint64(s.chunkSize),
		})
		c.Assert(err, IsNil)
		c.Assert(addOut, NotNil)
	}

	// add the remaining chunk
	sectionReader := io.NewSectionReader(reader, int64(numFullChunks*s.chunkSize), int64(lastChunkSize))
	addOut, err := s.gcsBackend.MultipartBlobAdd(&MultipartBlobAddInput{
		Commit:     commitInput,
		PartNumber: uint32(numFullChunks + 1),
		Body:       sectionReader,
		Size:       uint64(lastChunkSize),
	})
	c.Assert(addOut, NotNil)
	c.Assert(err, IsNil)

	commitOut, err := s.gcsBackend.MultipartBlobCommit(commitInput)
	c.Assert(err, IsNil)
	c.Assert(commitOut.ETag, NotNil)

	_, err = s.gcsBackend.HeadBlob(&HeadBlobInput{Key: objKey})
	c.Assert(err, IsNil)

	// assert uploaded content is correct
	blobOutput, err := s.gcsBackend.GetBlob(&GetBlobInput{Key: objKey})
	c.Assert(err, IsNil)
	defer blobOutput.Body.Close()

	_, err = reader.Seek(0, io.SeekStart) // seek to the beginning of the file
	c.Assert(err, IsNil)
	_, err = CompareReader(reader, blobOutput.Body, 0) // assert content
	c.Assert(err, IsNil)
}

func (s *GCSBackendTest) TestGCSBackend_MultipartUpload_Abort(c *C) {
	src := RandStringBytesMaskImprSrc(16)
	commitInput, err := s.gcsBackend.MultipartBlobBegin(&MultipartBlobBeginInput{Key: src})
	c.Assert(err, IsNil)

	addOut, err := s.gcsBackend.MultipartBlobAdd(&MultipartBlobAddInput{
		Commit:     commitInput,
		PartNumber: 1,
		Body:       bytes.NewReader([]byte(src)),
		Size:       uint64(len(src)),
		Last:       false,
	})
	c.Assert(err, IsNil)
	c.Assert(addOut, NotNil)

	_, err = s.gcsBackend.MultipartBlobAbort(commitInput)
	c.Assert(err, IsNil)

	_, err = s.gcsBackend.HeadBlob(&HeadBlobInput{Key: src})
	c.Assert(err, Equals, fuse.ENOENT)
}
