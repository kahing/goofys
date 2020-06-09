package internal

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/deka108/goplay/pkg/env"
	"github.com/kahing/goofys/api/common"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"google.golang.org/api/googleapi"
	"os"
	"testing"
)

type TestConfig struct {
	goofysBucket string
	object string
}

func LoadConfig(){
	viper.SetConfigType("yaml")
	viper.SetConfigFile(env.GetEnv("CONFIG_FILE", true))

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil { // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
}

var testConfig TestConfig

func TestMain(m *testing.M) {
	log.Println("Do stuff BEFORE the tests!")
	LoadConfig()
	testConfig = TestConfig{
		goofysBucket: viper.GetString("goofys.gcs.bucket"),
		object: viper.GetString("goofys.gcs.object"),
	}
	exitVal := m.Run()
	log.Println("Do stuff AFTER the tests!")

	os.Exit(exitVal)
}

func TestGCSConfig_WithoutCredentials(t *testing.T){
	val := os.Getenv("GOOGLE_APPLICATION_CREDENTIALS")
	os.Unsetenv("GOOGLE_APPLICATION_CREDENTIALS")
	_, err := common.NewGCSConfig()
	if assert.Error(t, err) {
		fmt.Println(err)
	}
	os.Setenv("GOOGLE_APPLICATION_CREDENTIALS", val)
}

func TestGCSConfig_WithCredentials(t *testing.T){
	_, err := common.NewGCSConfig()
	assert.Nil(t, err)
}

func getGcsConfig(bucket string) (*common.GCSConfig, error) {
	spec, err := ParseBucketSpec(bucket)
	if err != nil {
		return nil, err
	}
	config, err := common.NewGCSConfig()
	if err != nil {
		return nil, err
	}

	config.Bucket = spec.Bucket
	if config.Prefix != "" {
		config.Prefix = spec.Prefix
	}
	return config, nil
}


func TestGCSBucket_CreateBackend(t *testing.T){
	config, _ := getGcsConfig(testConfig.goofysBucket)
	gcsBackend, err := NewGCS(config)

	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(gcsBackend)
}

func getGcsBackend() (*GCSBackend, error) {
	config, _ := getGcsConfig(testConfig.goofysBucket)
	gcsBackend, err := NewGCS(config)

	return gcsBackend, err
}

func TestGCSBackend_BlobDoesNotExist(t *testing.T) {
	gcsBackend, _ := getGcsBackend()
	bkt := gcsBackend.client.Bucket(gcsBackend.Bucket())
	//_, err := bkt.Attrs(context.Background())
	//printError(err)
	randomObjectName := gcsBackend.config.Prefix + (RandStringBytesMaskImprSrc(32))
	_, err := bkt.Object(randomObjectName).Attrs(context.Background())
	if err == storage.ErrObjectNotExist {
		fmt.Println("Not exist")
	}
}

func TestGCSBackend_BlobExist(t *testing.T) {
	gcsBackend, _ := getGcsBackend()
	bkt := gcsBackend.client.Bucket(gcsBackend.Bucket())
	_, err := bkt.Object(testConfig.object).Attrs(context.Background())
	assert.Error(t, err)
}

func TestGCSBackend_BucketNoPermission(t *testing.T) {
	gcsBackend, _ := getGcsBackend()
	bktName := viper.GetString("goofys.gcs.bucketWithoutPermission")
	bkt := gcsBackend.client.Bucket(bktName)
	//_, err := bkt.Attrs(context.Background())
	//printError(err)
	randomObjectName := gcsBackend.config.Prefix + (RandStringBytesMaskImprSrc(32))
	_, err := bkt.Object(randomObjectName).Attrs(context.Background())
	printError(err)
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

func TestGCSBackend_BucketNotExist(t *testing.T) {
	gcsBackend, _ := getGcsBackend()
	bktName := viper.GetString("goofys.gcs.bucketDontExist")
	bkt := gcsBackend.client.Bucket(bktName)
	//_, err := bkt.Attrs(context.Background())
	//printError(err)
	randomObjectName := gcsBackend.config.Prefix + (RandStringBytesMaskImprSrc(32))
	_, err := bkt.Object(randomObjectName).Attrs(context.Background())
	printError(err)
}