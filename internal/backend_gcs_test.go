package internal

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"os"

	"github.com/kahing/goofys/api/common"
	"github.com/spf13/viper"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	. "gopkg.in/check.v1"
	"io/ioutil"
)

type GcsBackendTest struct {
	gcsBackend *GCSBackend
}

var _ = Suite(&GcsBackendTest{})

func getGcsConfig(bucket string) (*common.GCSConfig, error) {
	spec, err := ParseBucketSpec(bucket)
	if err != nil {
		return nil, err
	}
	config, err := common.NewGCSConfig("", spec.Bucket, "")
	if err != nil {
		return nil, err
	}

	if config.Prefix != "" {
		config.Prefix = spec.Prefix
	}
	return config, nil
}

func getGcsBackend() (*GCSBackend, error) {
	config, err := getGcsConfig(viper.GetString("goofys.gcs.bucketWithPermission"))
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
	s.gcsBackend, _ = getGcsBackend()
}

func (s *GcsBackendTest) TestGCSConfig_WithCredentials(c *C) {
	conf, err := common.NewGCSConfig("", viper.GetString("goofys.gcs.bucketName"), "")
	c.Assert(err, IsNil)
	c.Assert(conf.Credentials.ProjectID, NotNil)
}

func (s *GcsBackendTest) TestGCSBackend_CreateBackend(c *C) {
	config, _ := getGcsConfig(viper.GetString("goofys.gcs.bucketWithPermission"))
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
			input:   s.gcsBackend.config.Prefix + (RandStringBytesMaskImprSrc(32)), // random likely to not object
			isError: true,
		},
	}
	for _, tc := range testCases {
		blobOut, err := s.gcsBackend.HeadBlob(&HeadBlobInput{Key: tc.input})
		if tc.isError {
			c.Assert(err, ErrorMatches, "storage: object doesn't exist")
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

func (s *GcsBackendTest) TestGCS_ListObjects(c *C) {
	bkt := s.gcsBackend.client.Bucket(s.gcsBackend.Bucket())

	testCases := []struct {
		prefix     string
		delim      string
		isNoPrefix bool
		isNoItems  bool
	}{
		{
			"", // no prefix
			"", // no delim
			true,
			false,
		},
		{
			"",  // no prefix
			"/", // with delim
			false,
			false,
		},
		{
			"dir1/", // with prefix
			"",      // no delim
			true,
			false,
		},
		{
			"dir1", // with prefix
			"",     // no delim
			true,
			false,
		},
		{
			"dir1/", // with prefix
			"/",     // with delim
			false,
			false,
		},
		{
			"dir1", // with prefix
			"/",    // with delim
			false,
			true,
		},
		{
			"dir2/", // with prefix
			"/",     // with delim
			true,
			true,
		},
		{
			"nonexistent", // with nonexistent prefix
			"",            // no delim
			true,
			true,
		},
	}

	for _, tc := range testCases {
		query := storage.Query{}
		if tc.prefix != "" {
			query.Prefix = tc.prefix
		}

		if tc.delim != "" {
			query.Delimiter = tc.delim
		}

		//fmt.Printf("===LIST OBJECTS ===\n"+
		//	"Input = prefix: %s, delim: %s\n", tc.prefix, tc.delim)

		it := bkt.Objects(context.Background(), &query)

		var prefixes []BlobPrefixOutput
		var items []BlobItemOutput

		for {
			attrs, err := it.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				log.Fatal(err)
			}
			//fmt.Printf("attr.Prefix: %s, attr.Name: %s\n", attrs.Prefix, attrs.Name)
			if attrs.Prefix != "" {
				prefixes = append(prefixes, BlobPrefixOutput{&attrs.Prefix})
			}
			if attrs.Name != "" {
				items = append(items, BlobItemOutput{
					Key:          &attrs.Name,
					ETag:         &attrs.Etag,
					LastModified: &attrs.Updated,
					Size:         uint64(attrs.Size),
					StorageClass: &attrs.StorageClass,
				})
			}
		}
		//fmt.Printf("Prefixes: %v, Length: %v\n", prefixes, len(prefixes))
		//fmt.Printf("Items: %v, Length: %v\n", items, len(items))
		c.Assert(len(prefixes) == 0, Equals, tc.isNoPrefix)
		c.Assert(len(items) == 0, Equals, tc.isNoItems)
	}
}

func (s *GcsBackendTest) TestGCS_BlobExist(c *C) {
	testCases := []struct {
		input   string
		isError bool
	}{
		{
			"tmpfile866376544", // direct object
			false,
		},
		{
			"dir1/dir2/", // directory only
			true,
		},
		{
			"dir1/dir2/tmpfile640609998", // object under some directory
			false,
		},
	}
	bkt := s.gcsBackend.client.Bucket(s.gcsBackend.Bucket())
	_, err := bkt.Attrs(context.Background())
	c.Assert(err, IsNil)

	for _, tc := range testCases {
		obj, err := bkt.Object(tc.input).Attrs(context.Background())
		if tc.isError {
			c.Assert(err, ErrorMatches, "storage: object doesn't exist")
			//printGcsError(err)
		} else {
			c.Assert(err, IsNil)
			//fmt.Println(obj.Prefix, obj.Name, obj.Metadata, obj.Size)
			c.Assert(obj.Name, NotNil)
		}
	}
}

func (s *GcsBackendTest) TestGCS_BlobDoesNotExist(c *C) {
	bkt := s.gcsBackend.client.Bucket(s.gcsBackend.Bucket())
	randomObjectName := s.gcsBackend.config.Prefix + (RandStringBytesMaskImprSrc(32))
	_, err := bkt.Object(randomObjectName).Attrs(context.Background())
	c.Assert(err, Equals, storage.ErrObjectNotExist)
	_, err = bkt.Attrs(context.Background())
	c.Assert(err, IsNil)
}
