package internal

import (
	. "gopkg.in/check.v1"
)

type ParseBucketTest struct{}

var _ = Suite(&ParseBucketTest{})

func (s *ParseBucketTest) TestParseBucketSpec(t *C) {
	testCases := []struct {
		input    string
		expected BucketSpec
	}{
		{
			input: "s3://bucketName/hello/everyone",
			expected: BucketSpec{
				Scheme: "s3",
				Bucket: "bucketName",
				Prefix: "hello/everyone/",
			},
		},
		{
			input: "gs://bucketName/hello/everyone",
			expected: BucketSpec{
				Scheme: "gs",
				Bucket: "bucketName",
				Prefix: "hello/everyone/",
			},
		},
		{
			input: "bucketName/hello/everyone",
			expected: BucketSpec{
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
		t.Assert(tc.expected, DeepEquals, spec)
	}
}
