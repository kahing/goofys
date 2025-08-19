// Copyright 2019 Ka-Hing Cheung
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

	"fmt"
	"io"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/jacobsa/fuse"
)

// GCS variant of S3
type GCS3 struct {
	*S3Backend
}

type GCS3MultipartBlobCommitInput struct {
	Size uint64
	ETag *string
	Prev *MultipartBlobAddInput
}

func NewGCS3(bucket string, flags *FlagStorage, config *S3Config) (*GCS3, error) {
	s3Backend, err := NewS3(bucket, flags, config)
	if err != nil {
		return nil, err
	}
	s3Backend.Capabilities().Name = "gcs3"
	s := &GCS3{S3Backend: s3Backend}
	s.S3Backend.gcs = true
	s.S3Backend.cap.NoParallelMultipart = true
	return s, nil
}

func (s *GCS3) Delegate() interface{} {
	return s
}

func (s *GCS3) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	// GCS does not have multi-delete
	var wg sync.WaitGroup
	var overallErr error

	for _, key := range param.Items {
		wg.Add(1)
		go func(key string) {
			_, err := s.DeleteBlob(&DeleteBlobInput{
				Key: key,
			})
			if err != nil && err != fuse.ENOENT {
				overallErr = err
			}
			wg.Done()
		}(key)
	}
	wg.Wait()
	if overallErr != nil {
		return nil, mapAwsError(overallErr)
	}

	return &DeleteBlobsOutput{}, nil
}

func (s *GCS3) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	mpu := s3.CreateMultipartUploadInput{
		Bucket:       &s.bucket,
		Key:          &param.Key,
		StorageClass: &s.config.StorageClass,
		ContentType:  param.ContentType,
	}

	if s.config.UseSSE {
		mpu.ServerSideEncryption = &s.sseType
		if s.config.UseKMS && s.config.KMSKeyID != "" {
			mpu.SSEKMSKeyId = &s.config.KMSKeyID
		}
	}

	if s.config.ACL != "" {
		mpu.ACL = &s.config.ACL
	}

	req, _ := s.CreateMultipartUploadRequest(&mpu)
	// v4 signing of this fails
	s.setV2Signer(&req.Handlers)
	// get rid of ?uploads=
	req.HTTPRequest.URL.RawQuery = ""
	req.HTTPRequest.Header.Set("x-goog-resumable", "start")

	err := req.Send()
	if err != nil {
		s3Log.Errorf("CreateMultipartUpload %v = %v", param.Key, err)
		return nil, mapAwsError(err)
	}

	location := req.HTTPResponse.Header.Get("Location")
	_, err = url.Parse(location)
	if err != nil {
		s3Log.Errorf("CreateMultipartUpload %v %v = %v", param.Key, location, err)
		return nil, mapAwsError(err)
	}

	return &MultipartBlobCommitInput{
		Key:         &param.Key,
		Metadata:    param.Metadata,
		UploadId:    &location,
		Parts:       make([]*string, 10000), // at most 10K parts
		backendData: &GCS3MultipartBlobCommitInput{},
	}, nil
}

func (s *GCS3) uploadPart(param *MultipartBlobAddInput, totalSize uint64, last bool) (etag *string, err error) {
	atomic.AddUint32(&param.Commit.NumParts, 1)

	if closer, ok := param.Body.(io.Closer); ok {
		defer closer.Close()
	}

	// the mpuId serves as authentication token so
	// technically we don't need to sign this anymore and
	// can just use a plain HTTP request, but going
	// through aws-sdk-go anyway to get retry handling
	params := &s3.PutObjectInput{
		Bucket: &s.bucket,
		Key:    param.Commit.Key,
		Body:   param.Body,
	}

	s3Log.Debug(params)

	req, resp := s.PutObjectRequest(params)
	req.Handlers.Sign.Clear()
	req.HTTPRequest.URL, _ = url.Parse(*param.Commit.UploadId)

	start := totalSize - param.Size
	end := totalSize - 1
	var size string
	if last {
		size = strconv.FormatUint(totalSize, 10)
	} else {
		size = "*"
	}

	contentRange := fmt.Sprintf("bytes %v-%v/%v", start, end, size)

	req.HTTPRequest.Header.Set("Content-Length", strconv.FormatUint(param.Size, 10))
	req.HTTPRequest.Header.Set("Content-Range", contentRange)

	err = req.Send()
	if err != nil {
		// status indicating that we need more parts to finish this
		if req.HTTPResponse.StatusCode == 308 {
			err = nil
		} else {
			err = mapAwsError(err)
			return
		}
	}

	etag = resp.ETag

	return
}

func (s *GCS3) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	var commitData *GCS3MultipartBlobCommitInput
	var ok bool
	if commitData, ok = param.Commit.backendData.(*GCS3MultipartBlobCommitInput); !ok {
		panic("Incorrect commit data type")
	}

	if commitData.Prev != nil {
		if commitData.Prev.Size == 0 || commitData.Prev.Size%(256*1024) != 0 {
			s3Log.Errorf("size of each block must be multiple of 256KB: %v", param.Size)
			return nil, fuse.EINVAL
		}

		_, err := s.uploadPart(commitData.Prev, commitData.Size, false)
		if err != nil {
			return nil, err
		}
	}
	commitData.Size += param.Size

	copy := *param
	commitData.Prev = &copy
	param.Body = nil

	return &MultipartBlobAddOutput{}, nil
}

func (s *GCS3) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	var commitData *GCS3MultipartBlobCommitInput
	var ok bool
	if commitData, ok = param.backendData.(*GCS3MultipartBlobCommitInput); !ok {
		panic("Incorrect commit data type")
	}

	if commitData.Prev == nil {
		panic("commit should include last part")
	}

	etag, err := s.uploadPart(commitData.Prev, commitData.Size, true)
	if err != nil {
		return nil, err
	}

	return &MultipartBlobCommitOutput{
		ETag: etag,
	}, nil
}
