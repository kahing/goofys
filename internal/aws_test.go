// Copyright 2016 Ka-Hing Cheung
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	. "gopkg.in/check.v1"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/jacobsa/fuse"
)

type AwsTest struct {
	fs *Goofys
}

var _ = Suite(&AwsTest{})

func (s *AwsTest) SetUpSuite(t *C) {
	awsConfig := &aws.Config{
		Region:           aws.String("us-east-1"),
		S3ForcePathStyle: aws.Bool(true),
	}

	s.fs = &Goofys{
		awsConfig: awsConfig,
	}

	s.fs.s3 = NewS3(s.fs, "", awsConfig, &FlagStorage{})
}

func (s *AwsTest) TestRegionDetection(t *C) {
	if s3, ok := s.fs.s3.(*S3Backend); ok {
		s3.bucket = "goofys-eu-west-1.kahing.xyz"

		err, isAws := s3.detectBucketLocationByHEAD()
		t.Assert(err, IsNil)
		t.Assert(*s3.awsConfig.Region, Equals, "eu-west-1")
		t.Assert(isAws, Equals, true)
	} else {
		t.Skip("only for S3")
	}
}

func (s *AwsTest) TestBucket404(t *C) {
	if s3, ok := s.fs.s3.(*S3Backend); ok {
		s3.bucket = RandStringBytesMaskImprSrc(64)

		err, isAws := s3.detectBucketLocationByHEAD()
		t.Assert(err, Equals, fuse.ENOENT)
		t.Assert(isAws, Equals, true)
	} else {
		t.Skip("only for S3")
	}
}
