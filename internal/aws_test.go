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

	"fmt"
	"sync"
	"syscall"
	"time"
)

type AwsTest struct {
	s3 *S3Backend
}

var _ = Suite(&AwsTest{})

func (s *AwsTest) SetUpSuite(t *C) {
	var err error
	s.s3, err = NewS3("", &FlagStorage{}, &S3Config{
		Region: "us-east-1",
	})
	t.Assert(err, IsNil)
}

func (s *AwsTest) TestRegionDetection(t *C) {
	s.s3.bucket = "goofys-eu-west-1.StatCan.xyz"

	err, isAws := s.s3.detectBucketLocationByHEAD()
	t.Assert(err, IsNil)
	t.Assert(*s.s3.awsConfig.Region, Equals, "eu-west-1")
	t.Assert(isAws, Equals, true)
}

func (s *AwsTest) TestBucket404(t *C) {
	s.s3.bucket = RandStringBytesMaskImprSrc(64)

	err, isAws := s.s3.detectBucketLocationByHEAD()
	t.Assert(err, Equals, syscall.ENXIO)
	t.Assert(isAws, Equals, true)
}

type S3BucketEventualConsistency struct {
	*S3Backend
	// a list of blobs ever put by this backend, we speculatively
	// retry on these blobs to workaround eventual consistency
	mu    sync.RWMutex
	blobs map[string]bool
}

func NewS3BucketEventualConsistency(s *S3Backend) *S3BucketEventualConsistency {
	return &S3BucketEventualConsistency{
		s,
		sync.RWMutex{},
		make(map[string]bool),
	}
}

func (s *S3BucketEventualConsistency) Init(key string) (err error) {
	// TODO: make Init return errno instead
	NoSuchBucket := fmt.Sprintf("bucket %v does not exist", s.Bucket())

	for i := 0; i < 10; i++ {
		err = s.S3Backend.Init(key)
		if err != nil && err.Error() == NoSuchBucket {
			s3Log.Infof("waiting for bucket")
			time.Sleep((time.Duration(i) + 1) * 2 * time.Second)
		} else {
			return
		}
	}

	return
}

func (s *S3BucketEventualConsistency) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	var err error
	var res *HeadBlobOutput

	for i := 0; i < 10; i++ {
		res, err = s.S3Backend.HeadBlob(param)
		switch err {
		case syscall.ENXIO:
			s3Log.Infof("waiting for bucket")
			time.Sleep((time.Duration(i) + 1) * 2 * time.Second)
		case syscall.ENOENT:
			s.mu.RLock()
			_, ok := s.blobs[param.Key]
			s.mu.RUnlock()

			if ok {
				s3Log.Infof("waiting for blob: %v", param.Key)
				time.Sleep((time.Duration(i) + 1) * 20 * time.Millisecond)
			}
		default:
			return res, err
		}
	}

	return res, err
}

func (s *S3BucketEventualConsistency) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	for i := 0; i < 10; i++ {
		res, err := s.S3Backend.ListBlobs(param)
		switch err {
		case syscall.ENXIO:
			s3Log.Infof("waiting for bucket")
			time.Sleep((time.Duration(i) + 1) * 2 * time.Second)
		default:
			return res, err
		}
	}

	return nil, syscall.ENXIO
}

func (s *S3BucketEventualConsistency) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	for i := 0; i < 10; i++ {
		res, err := s.S3Backend.DeleteBlob(param)
		switch err {
		case syscall.ENXIO:
			s3Log.Infof("waiting for bucket")
			time.Sleep((time.Duration(i) + 1) * 2 * time.Second)
		default:
			return res, err
		}
	}

	return nil, syscall.ENXIO
}

func (s *S3BucketEventualConsistency) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	for i := 0; i < 10; i++ {
		res, err := s.S3Backend.DeleteBlobs(param)
		switch err {
		case syscall.ENXIO:
			s3Log.Infof("waiting for bucket")
			time.Sleep((time.Duration(i) + 1) * 2 * time.Second)
		default:
			return res, err
		}
	}

	return nil, syscall.ENXIO
}

func (s *S3BucketEventualConsistency) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	for i := 0; i < 10; i++ {
		res, err := s.S3Backend.CopyBlob(param)
		switch err {
		case syscall.ENXIO:
			s3Log.Infof("waiting for bucket")
			time.Sleep((time.Duration(i) + 1) * 2 * time.Second)
		default:
			return res, err
		}
	}

	return nil, syscall.ENXIO
}

func (s *S3BucketEventualConsistency) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	s.mu.Lock()
	s.blobs[param.Key] = true
	s.mu.Unlock()

	for i := 0; i < 10; i++ {
		res, err := s.S3Backend.PutBlob(param)
		switch err {
		case syscall.ENXIO:
			param.Body.Seek(0, 0)
			s3Log.Infof("waiting for bucket")
			time.Sleep((time.Duration(i) + 1) * 2 * time.Second)
		default:
			return res, err
		}
	}

	return nil, syscall.ENXIO
}

func (s *S3BucketEventualConsistency) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error) {
	for i := 0; i < 10; i++ {
		res, err := s.S3Backend.RemoveBucket(param)
		switch err {
		case syscall.ENXIO:
			s3Log.Infof("waiting for bucket")
			time.Sleep((time.Duration(i) + 1) * 2 * time.Second)
		default:
			return res, err
		}
	}

	return nil, syscall.ENXIO
}
