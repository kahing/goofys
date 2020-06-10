package internal

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"io/ioutil"
	"os"
	"testing"
)

func getGcsBackend() (*GCSBackend, error) {
	config, _ := getGcsConfig(viper.GetString("goofys.gcs.bucketWithPermission"))
	gcsBackend, err := NewGCS(config)

	return gcsBackend, err
}

var gcsBackend *GCSBackend;

func TestGCS_BlobExist(t *testing.T) {
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
	bkt := gcsBackend.client.Bucket(gcsBackend.Bucket())
	_, err := bkt.Attrs(context.Background())
	assert.Nil(t, err)

	for _, tc := range testCases {
		obj, err := bkt.Object(tc.input).Attrs(context.Background())
		if tc.isError {
			assert.Error(t, err)
			printGcsError(err)
		} else {
			assert.Nil(t, err)
			fmt.Println(obj.Prefix, obj.Name, obj.Metadata, obj.Size)
		}
	}
}

func TestGCS_BlobDoesNotExist(t *testing.T) {
	bkt := gcsBackend.client.Bucket(gcsBackend.Bucket())
	randomObjectName := gcsBackend.config.Prefix + (RandStringBytesMaskImprSrc(32))
	_, err := bkt.Object(randomObjectName).Attrs(context.Background())
	if err == storage.ErrObjectNotExist {
		fmt.Println("Object not exist")
	}
	_, err = bkt.Attrs(context.Background())
	assert.Nil(t, err)
}

func TestGCS_ReadOnlyBlobDoesNotExist(t *testing.T) {
	gcsBackend, setEnvFunc, _ := getReadOnlyBackend()
	defer setEnvFunc()

	bkt := gcsBackend.client.Bucket(gcsBackend.Bucket())
	randomObjectName := gcsBackend.config.Prefix + (RandStringBytesMaskImprSrc(32))
	_, err := bkt.Object(randomObjectName).Attrs(context.Background())
	if err == storage.ErrObjectNotExist {
		fmt.Println("Object not exist")
	}
	_, err = bkt.Attrs(context.Background())
	assert.Nil(t, err)
}

func getReadOnlyBackend() (*GCSBackend, func(), error) {
	env := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", viper.GetString("goofys.gcs.readOnlyCredentials"))
	gcsBackend, _ := getGcsBackend()

	return gcsBackend, func() { os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", env) }, nil
}

func TestGCS_ReadOnlyBucketDoesNotExist(t *testing.T) {
	gcsBackend, setEnvFunc, _ := getReadOnlyBackend()
	defer setEnvFunc()

	bkt := gcsBackend.client.Bucket(viper.GetString("goofys.gcs.bucketWithoutPermission"))
	randomObjectName := gcsBackend.config.Prefix + (RandStringBytesMaskImprSrc(32))
	_, err := bkt.Object(randomObjectName).Attrs(context.Background())
	if err == storage.ErrObjectNotExist {
		fmt.Println("Object not exist")
	}
	_, err = bkt.Attrs(context.Background())
	assert.Error(t, err)
}

func TestGCS_BucketNoPermission(t *testing.T) {
	bktName := viper.GetString("goofys.gcs.bucketWithoutPermission")

	bkt := gcsBackend.client.Bucket(bktName)
	randomObjectName := gcsBackend.config.Prefix + (RandStringBytesMaskImprSrc(32))
	_, err := bkt.Object(randomObjectName).Attrs(context.Background())
	assert.Error(t, err)
	printGcsError(err)

	//_, err = bkt.Attrs(context.Background())
	//assert.Error(t, err)
	//printGcsError(err)
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

func TestGCS_BucketNotExist(t *testing.T) {
	bktName := viper.GetString("goofys.gcs.bucketDontExist")
	bkt := gcsBackend.client.Bucket(bktName)
	randomObjectName := gcsBackend.config.Prefix + (RandStringBytesMaskImprSrc(32))
	_, err := bkt.Object(randomObjectName).Attrs(context.Background())
	assert.Error(t, err)
	printGcsError(err)

	_, err = bkt.Attrs(context.Background())
	assert.Error(t, err)
	printGcsError(err)
}

func TestGCS_ListObjects(t *testing.T){
	bkt := gcsBackend.client.Bucket(gcsBackend.Bucket())

	testCases := []struct {
		prefix   string
		delim    string
		noResults bool
	}{
		{
			"", // no prefix
			"", // no delim
			false,
		},
		{
			"", // no prefix
			"/", // with delim
			false,
		},
		{
			"dir1/", // with prefix
			"", // no delim
			false,
		},
		{
			"dir1", // with prefix
			"", // no delim
			false,
		},
		{
			"dir1/", // with prefix
			"/", // with delim
			false,
		},
		{
			"dir1", // with prefix
			"/", // with delim
			false,
		},
		{
			"dir2/", // with prefix
			"/", // with delim
			true,
		},
		{
			"nonexistent", // with nonexistent prefix
			"", // no delim
			true,
		},
	}

	for _, tc := range testCases {
		query := storage.Query{}
		if tc.prefix != "" {
			query.Prefix = tc.prefix
		}

		if tc.delim != ""{
			query.Delimiter = tc.delim
		}

		fmt.Printf("===LIST OBJECTS ===\n" +
			"Input = prefix: %s, delim: %s\n", tc.prefix, tc.delim)

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
			fmt.Printf("attr.Prefix: %s, attr.Name: %s\n", attrs.Prefix, attrs.Name)
			if attrs.Prefix != "" {
				prefixes = append(prefixes, BlobPrefixOutput{&attrs.Prefix})
			}
			if attrs.Name != ""{
				items = append(items, BlobItemOutput{
					Key: &attrs.Name,
					ETag: &attrs.Etag,
					LastModified: &attrs.Updated,
					Size: uint64(attrs.Size),
					StorageClass: &attrs.StorageClass,
				})
			}
		}
		fmt.Printf("Prefixes: %v, Length: %v\n", prefixes, len(prefixes))
		fmt.Printf("Items: %v, Length: %v\n", items, len(items))
	}
}

func TestGCS_GetBlob(t *testing.T){
	objName := "tmpfile866376544"
	rc, err := gcsBackend.client.Bucket(gcsBackend.Bucket()).Object(objName).NewReader(context.Background())
	if err != nil {
		printGcsError(err)
	}
	defer rc.Close()
	data, err := ioutil.ReadAll(rc)
	if err != nil {
		printGcsError(err)
	}
	fmt.Println(string(data))
}