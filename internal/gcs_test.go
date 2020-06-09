package internal

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
	"os"
	"testing"
)

func getGcsBackend() (*GCSBackend, error) {
	config, _ := getGcsConfig(viper.GetString("goofys.gcs.bucketWithPermission"))
	gcsBackend, err := NewGCS(config)

	return gcsBackend, err
}

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
	gcsBackend, _ := getGcsBackend()
	bkt := gcsBackend.client.Bucket(gcsBackend.Bucket())
	_, err := bkt.Attrs(context.Background())
	assert.Nil(t, err)

	for _, tc := range testCases {
		obj, err := bkt.Object(tc.input).Attrs(context.Background())
		if tc.isError {
			assert.Error(t, err)
			printError(err)
		} else {
			assert.Nil(t, err)
			fmt.Println(obj.Prefix, obj.Name, obj.Metadata, obj.Size)
		}
	}
}

func TestGCS_BlobDoesNotExist(t *testing.T) {
	gcsBackend, _ := getGcsBackend()
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
	env := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", viper.GetString("goofys.gcs.readOnlyCredentials"))
	defer func() { os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", env) }()

	gcsBackend, _ := getGcsBackend()
	bkt := gcsBackend.client.Bucket(gcsBackend.Bucket())
	randomObjectName := gcsBackend.config.Prefix + (RandStringBytesMaskImprSrc(32))
	_, err := bkt.Object(randomObjectName).Attrs(context.Background())
	if err == storage.ErrObjectNotExist {
		fmt.Println("Object not exist")
	}
	_, err = bkt.Attrs(context.Background())
	assert.Nil(t, err)
}

func TestGCS_ReadOnlyBucketDoesNotExist(t *testing.T) {
	env := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", viper.GetString("goofys.gcs.readOnlyCredentials"))
	defer func() { os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", env) }()

	gcsBackend, _ := getGcsBackend()
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
	gcsBackend, _ := getGcsBackend()
	bktName := viper.GetString("goofys.gcs.bucketWithoutPermission")

	bkt := gcsBackend.client.Bucket(bktName)
	randomObjectName := gcsBackend.config.Prefix + (RandStringBytesMaskImprSrc(32))
	_, err := bkt.Object(randomObjectName).Attrs(context.Background())
	assert.Error(t, err)
	printError(err)

	//_, err = bkt.Attrs(context.Background())
	//assert.Error(t, err)
	//printError(err)
}

func printError(err error) {
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
	gcsBackend, _ := getGcsBackend()
	bktName := viper.GetString("goofys.gcs.bucketDontExist")
	bkt := gcsBackend.client.Bucket(bktName)
	randomObjectName := gcsBackend.config.Prefix + (RandStringBytesMaskImprSrc(32))
	_, err := bkt.Object(randomObjectName).Attrs(context.Background())
	assert.Error(t, err)
	printError(err)

	_, err = bkt.Attrs(context.Background())
	assert.Error(t, err)
	printError(err)
}

func TestGCS_ListObjects(t *testing.T){
	gcsBackend, _ := getGcsBackend()
	bkt := gcsBackend.client.Bucket(gcsBackend.Bucket())
	it := bkt.Objects(context.Background(), nil)
	pi := it.PageInfo()
	fmt.Println(pi)
	fmt.Println(pi.MaxSize)
	fmt.Println(pi.Remaining())
	fmt.Println(pi.Token)
}