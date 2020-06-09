package internal

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseBucketSpec(t *testing.T){
	assert := assert.New(t)

	testCases := []struct{
		input string
		expected BucketSpec
	}{
		{
			"s3://bucketName/hello/everyone", BucketSpec{
			Scheme: "s3",
			Bucket: "bucketName",
			Prefix: "hello/everyone/",
		},
		},
		{
			"gs://bucketName/hello/everyone", BucketSpec{
				Scheme: "gs",
				Bucket: "bucketName",
				Prefix: "hello/everyone/",
			},
		},
		{
			"bucketName/hello/everyone", BucketSpec{
			Scheme: "s3",
			Bucket: "bucketName/hello/everyone",
			Prefix: "",
		},
		},
	}

	for _, tc := range testCases {
		spec, err := ParseBucketSpec(tc.input)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(fmt.Sprintf("Input: %s, Scheme: %s, Bucket: %s, Prefix: %s",
			tc.input, spec.Scheme, spec.Bucket, spec.Prefix))
		assert.Equal(tc.expected.Scheme, spec.Scheme)
		assert.Equal(tc.expected.Bucket, spec.Bucket)
		assert.Equal(tc.expected.Prefix, spec.Prefix)

	}
}
