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
	"syscall"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

type S3Backend struct {
	*s3.S3
	fs *Goofys

	aws bool
}

func (s *S3Backend) ListObjectsV2(params *s3.ListObjectsV2Input) (*s3.ListObjectsV2Output, error) {
	if s.aws {
		return s.S3.ListObjectsV2(params)
	} else {
		v1 := s3.ListObjectsInput{
			Bucket:       params.Bucket,
			Delimiter:    params.Delimiter,
			EncodingType: params.EncodingType,
			MaxKeys:      params.MaxKeys,
			Prefix:       params.Prefix,
			RequestPayer: params.RequestPayer,
		}
		if params.StartAfter != nil {
			v1.Marker = params.StartAfter
		} else {
			v1.Marker = params.ContinuationToken
		}

		objs, err := s.S3.ListObjects(&v1)
		if err != nil {
			return nil, err
		}

		count := int64(len(objs.Contents))
		v2Objs := s3.ListObjectsV2Output{
			CommonPrefixes:        objs.CommonPrefixes,
			Contents:              objs.Contents,
			ContinuationToken:     objs.Marker,
			Delimiter:             objs.Delimiter,
			EncodingType:          objs.EncodingType,
			IsTruncated:           objs.IsTruncated,
			KeyCount:              &count,
			MaxKeys:               objs.MaxKeys,
			Name:                  objs.Name,
			NextContinuationToken: objs.NextMarker,
			Prefix:                objs.Prefix,
			StartAfter:            objs.Marker,
		}

		return &v2Objs, nil
	}
}

func (s *S3Backend) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	resp, err := s.S3.HeadObject(&s3.HeadObjectInput{
		Bucket: &s.fs.bucket,
		Key:    &param.Key,
	})
	if err != nil {
		return nil, mapAwsError(err)
	}
	return &HeadBlobOutput{
		BlobItemOutput: BlobItemOutput{
			Key:          &param.Key,
			ETag:         resp.ETag,
			LastModified: resp.LastModified,
			Size:         uint64(*resp.ContentLength),
			StorageClass: resp.StorageClass,
		},
		ContentType: resp.ContentType,
		Metadata:    resp.Metadata,
	}, nil
}

func (s *S3Backend) ListBlobs(param *ListBlobInput) (*ListBlobOutput, error) {
	resp, err := s.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket:            &s.fs.bucket,
		Prefix:            &param.Prefix,
		Delimiter:         &param.Delimiter,
		MaxKeys:           aws.Int64(int64(param.MaxKeys)),
		ContinuationToken: param.ContinuationToken,
	})
	if err != nil {
		return nil, mapAwsError(err)
	}

	prefixes := make([]BlobPrefixOutput, 0)
	items := make([]BlobItemOutput, 0)

	for _, p := range resp.CommonPrefixes {
		prefixes = append(prefixes, BlobPrefixOutput{Prefix: p.Prefix})
	}
	for _, i := range resp.Contents {
		items = append(items, BlobItemOutput{
			Key:          i.Key,
			ETag:         i.ETag,
			LastModified: i.LastModified,
			Size:         uint64(*i.Size),
			StorageClass: i.StorageClass,
		})
	}

	return &ListBlobOutput{
		ContinuationToken:     param.ContinuationToken,
		MaxKeys:               param.MaxKeys,
		Prefixes:              prefixes,
		Items:                 items,
		NextContinuationToken: resp.NextContinuationToken,
		IsTruncated:           *resp.IsTruncated,
	}, nil
}

func (s *S3Backend) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

/*
func (s *S3Backend) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {

}
*/
// PutBlob(param *PutBlobInput) (*PutBlobOutput, error)
// MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error)
// MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error)
// MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error)
// MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error)
