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

type Capabilities struct {
	NoParallelMultipart bool
	MaxMultipartSize    uint64
	// indicates that the blob store has native support for directories
	DirBlob bool
}

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
	IsDirBlob   bool
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

type DeleteBlobInput struct {
	Key string
}

type DeleteBlobOutput struct {
}

type DeleteBlobsInput struct {
	Items []string
}

type DeleteBlobsOutput struct {
}

type RenameBlobInput struct {
	Source      string
	Destination string
}

type RenameBlobOutput struct {
}

type CopyBlobInput struct {
	Source      string
	Destination string

	Size     *uint64
	ETag     *string
	Metadata map[string]*string
}

type CopyBlobOutput struct {
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
	Key         string
	Metadata    map[string]*string
	ContentType *string
	DirBlob     bool

	Body io.ReadSeeker
}

type PutBlobOutput struct {
	ETag *string
}

type MultipartBlobBeginInput struct {
	Key         string
	Metadata    map[string]*string
	ContentType *string
}

type MultipartBlobCommitInput struct {
	Key *string

	Metadata map[string]*string
	UploadId *string
	Parts    []*string
	NumParts uint32

	// for GCS
	backendData interface{}
}

type MultipartBlobAddInput struct {
	Commit     *MultipartBlobCommitInput
	PartNumber uint32

	Body io.ReadSeeker

	Size uint64 // GCS wants to know part size
	Last bool   // GCS needs to know if this part is the last one
}

type MultipartBlobAddOutput struct {
}

type MultipartBlobCommitOutput struct {
	ETag *string
}

type MultipartBlobAbortOutput struct {
}

type MultipartExpireInput struct {
}

type MultipartExpireOutput struct {
}

type RemoveBucketInput struct {
}

type RemoveBucketOutput struct {
}

type MakeBucketInput struct {
}

type MakeBucketOutput struct {
}

type StorageBackend interface {
	Init() error
	Capabilities() *Capabilities
	HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error)
	ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error)
	DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error)
	DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error)
	RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error)
	CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error)
	GetBlob(param *GetBlobInput) (*GetBlobOutput, error)
	PutBlob(param *PutBlobInput) (*PutBlobOutput, error)
	MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error)
	MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error)
	MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error)
	MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error)
	MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error)
	RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error)
	MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error)
}

type sortBlobPrefixOutput []BlobPrefixOutput

func (p sortBlobPrefixOutput) Len() int {
	return len(p)
}

func (p sortBlobPrefixOutput) Less(i, j int) bool {
	return *p[i].Prefix < *p[j].Prefix
}

func (p sortBlobPrefixOutput) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

type sortBlobItemOutput []BlobItemOutput

func (p sortBlobItemOutput) Len() int {
	return len(p)
}

func (p sortBlobItemOutput) Less(i, j int) bool {
	return *p[i].Key < *p[j].Key
}

func (p sortBlobItemOutput) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
