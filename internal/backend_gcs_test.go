package internal

import (
	"fmt"
	"github.com/deka108/goplay/pkg/env"
	"github.com/kahing/goofys/api/common"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"os"
	"testing"
)


func LoadConfig(){
	viper.SetConfigType("yaml")
	viper.SetConfigFile(env.GetEnv("CONFIG_FILE", true))

	err := viper.ReadInConfig() // Find and read the config file
	if err != nil { // Handle errors reading the config file
		panic(fmt.Errorf("Fatal error config file: %s \n", err))
	}
}


func TestMain(m *testing.M) {
	log.Println("Do stuff BEFORE the tests!")
	LoadConfig()
	gcsBackend, _ = getGcsBackend()
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


func TestGCSBackend_CreateBackend(t *testing.T){
	config, _ := getGcsConfig(viper.GetString("goofys.gcs.bucketWithPermission"))
	gcsBackend, err := NewGCS(config)

	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(gcsBackend)
}

func TestGCSBackend_HeadBlob(t *testing.T) {
	testCases := []struct {
		input   string
		isError bool
	}{
		{
			"tmpfile866376544", // direct object
			false,
		},
		{
			gcsBackend.config.Prefix + (RandStringBytesMaskImprSrc(32)), // random likely to not object
			true,
		},
	}
	for _, tc := range testCases {
		blobOut, err := gcsBackend.HeadBlob(&HeadBlobInput{Key: tc.input})
		if tc.isError {
			assert.Error(t, err)
			fmt.Println(err)
		} else {
			assert.Nil(t, err)
			fmt.Println(blobOut.BlobItemOutput, blobOut.LastModified, blobOut.Size)
		}
	}
}

func TestGCSBackend_GetBlob(t *testing.T){
	objName := "tmpfile866376544"
	blobOutput, err := gcsBackend.GetBlob(&GetBlobInput{
		Key:     objName,
	})
	assert.Nil(t, err)
	data, err := ioutil.ReadAll(blobOutput.Body)
	assert.Nil(t, err)
	fmt.Println(string(data))
}