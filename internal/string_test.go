package internal

import (
	. "gopkg.in/check.v1"
)

type StringTest struct{}

var _ = Suite(&StringTest{})

func (s *StringTest) TestParseBucketSpec(t *C) {
	testCases := []struct {
		input    string
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
		//fmt.Println(fmt.Sprintf("Input: %s, Scheme: %s, Bucket: %s, Prefix: %s",
		//	tc.input, spec.Scheme, spec.Bucket, spec.Prefix))
		t.Assert(tc.expected, DeepEquals, spec)
	}
}
