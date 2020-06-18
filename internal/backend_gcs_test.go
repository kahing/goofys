package internal

import (
	"bytes"
	"cloud.google.com/go/storage"
	"fmt"
	"io/ioutil"
	"os"
	"time"

	"github.com/kahing/goofys/api/common"
	"github.com/spf13/viper"
	"google.golang.org/api/googleapi"
	. "gopkg.in/check.v1"
)

type GcsBackendTest struct {
	gcsBackend *GCSBackend
	gcsBackendWithTimeout *GCSBackend
}

var _ = Suite(&GcsBackendTest{})

func getGcsConfig(bucket string, flags *common.FlagStorage) (*common.GCSConfig, error) {
	spec, err := ParseBucketSpec(bucket)
	if err != nil {
		return nil, err
	}
	config, err := common.NewGCSConfig("", spec.Bucket, flags)
	if err != nil {
		return nil, err
	}

	return config, nil
}

func getGcsBackend(timeout time.Duration) (*GCSBackend, error) {
	config, err := getGcsConfig(viper.GetString("goofys.gcs.bucketWithPermission"), &common.FlagStorage{
		HTTPTimeout: timeout * time.Second,
	})
	if err != nil {
		return nil, err
	}
	gcsBackend, err := NewGCS(config)

	return gcsBackend, err
}

func (s *GcsBackendTest) SetUpSuite(c *C) {
	viper.SetConfigType("yaml")
	viper.SetConfigFile(os.ExpandEnv(os.Getenv("CONFIG_FILE")))

	err := viper.ReadInConfig() // Find and read the config file
	c.Assert(err, IsNil, Commentf("ERROR: reading config file: %s \n", err))
	s.gcsBackend, _ = getGcsBackend(0)
	s.gcsBackendWithTimeout, _ = getGcsBackend(1)
}

func (s *GcsBackendTest) TestGCSConfig_WithCredentials(c *C) {
	conf, err := common.NewGCSConfig("", viper.GetString("goofys.gcs.bucketName"), nil)
	c.Assert(err, IsNil)
	c.Assert(conf.Credentials.ProjectID, NotNil)
}

func (s *GcsBackendTest) TestGCSBackend_CreateBackend(c *C) {
	config, _ := getGcsConfig(viper.GetString("goofys.gcs.bucketWithPermission"), nil)
	gcsBackend, err := NewGCS(config)
	c.Assert(err, IsNil)
	c.Assert(gcsBackend, NotNil)
}

func (s *GcsBackendTest) TestGCSBackend_HeadBlob(c *C) {
	testCases := []struct {
		input   string
		isError bool
	}{
		{
			input:   "tmpfile866376544", // direct object
			isError: false,
		},
		{
			input:   RandStringBytesMaskImprSrc(32), // random likely to not object
			isError: true,
		},
	}
	for _, tc := range testCases {
		blobOut, err := s.gcsBackend.HeadBlob(&HeadBlobInput{Key: tc.input})
		if tc.isError {
			c.Assert(err, ErrorMatches, "no such file or directory")
		} else {
			c.Assert(err, IsNil)
			c.Assert(blobOut.BlobItemOutput.Key, NotNil)
			c.Assert(blobOut.LastModified, NotNil)
			c.Assert(blobOut.Size > 0, Equals, true)
		}
	}
}

func (s *GcsBackendTest) TestGCSBackend_GetBlob(c *C) {
	objName := "tmpfile866376544"
	blobOutput, err := s.gcsBackend.GetBlob(&GetBlobInput{
		Key: objName,
	})
	c.Assert(err, IsNil)
	data, err := ioutil.ReadAll(blobOutput.Body)
	c.Assert(err, IsNil)
	c.Assert(data, NotNil)
	//fmt.Println(string(data))
}

func (s *GcsBackendTest) TestGCSBackend_GetBlob_Timeout(c *C) {
	objName := "tmpfile866376544"
	_, err := s.gcsBackendWithTimeout.GetBlob(&GetBlobInput{
		Key: objName,
	})
	c.Assert(err, ErrorMatches, "operation timed out" )
}

func (s *GcsBackendTest) TestGCSBackend_PutBlob(c *C){
	objKey := RandStringBytesMaskImprSrc(16)
	_, err := s.gcsBackend.HeadBlob(&HeadBlobInput{Key: objKey})
	c.Assert(err, ErrorMatches, "no such file or directory")

	putOut, err := s.gcsBackend.PutBlob(&PutBlobInput{
		Key:  objKey,
		Body: bytes.NewReader([]byte(objKey)),
		Size: PUInt64(uint64(len(objKey))),
	})
	c.Assert(err, IsNil)
	c.Assert(putOut, NotNil)

	blobOut, err := s.gcsBackend.HeadBlob(&HeadBlobInput{Key: objKey})
	c.Assert(err, IsNil)
	c.Assert(blobOut, NotNil)
}

func (s *GcsBackendTest) TestGCSBackend_DeleteBlob(c *C) {
	objKey := RandStringBytesMaskImprSrc(16)

	// Put some object
	_, err := s.gcsBackend.PutBlob(&PutBlobInput{
		Key:  objKey,
		Body: bytes.NewReader([]byte(objKey)),
		Size: PUInt64(uint64(len(objKey))),
	})
	blobOut, err := s.gcsBackend.HeadBlob(&HeadBlobInput{Key: objKey})
	c.Assert(err, IsNil)
	c.Assert(blobOut, NotNil)

	// Delete some object
	deleteOut, err := s.gcsBackend.DeleteBlob(&DeleteBlobInput{
		Key: objKey,
	})
	c.Assert(err, IsNil)
	c.Assert(deleteOut, NotNil)

	_, err = s.gcsBackend.HeadBlob(&HeadBlobInput{Key: objKey})
	c.Assert(err, ErrorMatches, "no such file or directory")
}

func (s *GcsBackendTest) TestGCSBackend_DeleteBlobs(c *C){
	// Put many objects
	N := 4
	var keys []string

	for i:=0; i < N; i++ {
		objKey := RandStringBytesMaskImprSrc(16)

		// Put some object
		_, err := s.gcsBackend.PutBlob(&PutBlobInput{
			Key:  objKey,
			Body: bytes.NewReader([]byte(objKey)),
			Size: PUInt64(uint64(len(objKey))),
		})
		c.Assert(err, IsNil)
		keys = append(keys, objKey)
	}
	c.Assert(len(keys), Equals, N)

	// Delete many objects at once
	res, err := s.gcsBackend.DeleteBlobs(&DeleteBlobsInput{
		Items: keys,
	})
	c.Assert(err, IsNil)
	c.Assert(res, NotNil)
}

func printGcsError(err error) {
	if e, ok := err.(*googleapi.Error); ok {
		fmt.Println(e.Code, e.Body, e.Details, e.Message)
	}
	if err == storage.ErrBucketNotExist {
		fmt.Println("bucket don't exist")
	}
	if err == storage.ErrObjectNotExist {
		fmt.Println("object don't exist")
	}
}
