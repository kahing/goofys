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
	. "github.com/StatCan/goofys/api/common"
	. "gopkg.in/check.v1"

	"context"

	"github.com/aws/aws-sdk-go/service/s3"
)

type MinioTest struct {
	fs    *Goofys
	flags FlagStorage
	s3    *S3Backend
}

var _ = Suite(&MinioTest{})

func (s *MinioTest) SetUpSuite(t *C) {
	s.fs = &Goofys{}

	conf := (&S3Config{
		AccessKey: "Q3AM3UQ867SPQQA43P2F",
		SecretKey: "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG",
	}).Init()
	s.flags = FlagStorage{
		Endpoint: "https://play.minio.io:9000",
		Backend:  conf,
	}

	var err error
	s.s3, err = NewS3("", &s.flags, conf)
	t.Assert(err, IsNil)

	_, err = s.s3.ListBuckets(nil)
	t.Assert(err, IsNil)
}

func (s *MinioTest) SetUpTest(t *C) {
	bucket := RandStringBytesMaskImprSrc(32)

	_, err := s.s3.CreateBucket(&s3.CreateBucketInput{
		Bucket: &bucket,
	})
	t.Assert(err, IsNil)

	s.fs = NewGoofys(context.Background(), bucket, &s.flags)
	t.Assert(s.fs, NotNil)
}

func (s *MinioTest) TestMinioNoop(t *C) {
}
