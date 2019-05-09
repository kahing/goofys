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
	"io"
	"time"
)

type HeadBlobInput struct {
	Key string
}

type BlobItemOutput struct {
	Key          *string
	ETag         *string
	LastModified *time.Time
	Size         uint64
	StorageClass *string
}

type HeadBlobOutput struct {
	BlobItemOutput

	ContentType *string
	Metadata    map[string]*string
}

type ListBlobsInput struct {
	Prefix            *string
	Delimiter         *string
	MaxKeys           *uint32
	StartAfter        *string // XXX: not supported by Azure
	ContinuationToken *string
}

type BlobPrefixOutput struct {
	Prefix *string
}

type ListBlobsOutput struct {
	ContinuationToken *string

	Prefixes              []BlobPrefixOutput
	Items                 []BlobItemOutput
	NextContinuationToken *string
	IsTruncated           bool
}

type RenameBlobInput struct {
	Source      string
	Destination string
}

type RenameBlobOutput struct {
}

type GetBlobInput struct {
	Key     string
	Start   uint64
	Count   uint64
	IfMatch *string
}

type GetBlobOutput struct {
	HeadBlobOutput

	Body io.ReadCloser
}

type PutBlobInput struct {
	Key      string
	Metadata map[string]*string

	Body io.ReadSeeker
}

type PutBlobOutput struct {
	ETag *string
}

type MultipartBlobBeginInput struct {
	Key      string
	Metadata map[string]*string
}

type MultipartBlobCommitInput struct {
	Key *string

	Metadata map[string]*string
	UploadId *string

	Parts []*string
}

type MultipartBlobAddInput struct {
	Commit     *MultipartBlobCommitInput
	PartNumber uint64

	Body io.ReadSeeker
}

type MultipartBlobAddOutput struct {
}

type MultipartBlobCommitOutput struct {
	ETag *string
}

type MultipartExpireInput struct {
}

type MultipartExpireOutput struct {
}

type StorageBackend interface {
	HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error)
	ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error)
	RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error)
	GetBlob(param *GetBlobInput) (*GetBlobOutput, error)
	PutBlob(param *PutBlobInput) (*PutBlobOutput, error)
	MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error)
	MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error)
	MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error)
	MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error)
}
