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
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	"github.com/jacobsa/fuse"
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

/// Implementations of all the functions here are expected to be
/// concurrency-safe, except for
///
/// Init() is called exactly once before any other functions are
/// called.
///
/// Capabilities() is expected to be const
type StorageBackend interface {
	Init(key string) error
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

func (b BlobItemOutput) String() string {
	return fmt.Sprintf("%v: %v", *b.Key, b.Size)
}

func (b BlobPrefixOutput) String() string {
	return fmt.Sprintf("%v", *b.Prefix)
}

type ReadSeekerCloser struct {
	io.ReadSeeker
}

func (r *ReadSeekerCloser) Close() error {
	if closer, ok := r.ReadSeeker.(io.Closer); ok {
		return closer.Close()
	} else {
		return nil
	}
}

type StorageBackendInitWrapper struct {
	StorageBackend
	init    sync.Once
	initKey string
	initErr error
}

const INIT_ERR_BLOB = "mount.err"

func (s *StorageBackendInitWrapper) Init(key string) error {
	var err error
	s.init.Do(func() {
		err = s.StorageBackend.Init(s.initKey)
		if err != nil {
			s3Log.Errorf("Init: %v", err)
			s.StorageBackend = StorageBackendInitError{err}
		}
	})
	return err
}

func (s *StorageBackendInitWrapper) Capabilities() *Capabilities {
	return s.StorageBackend.Capabilities()
}

func (s *StorageBackendInitWrapper) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	s.Init("")
	return s.StorageBackend.HeadBlob(param)
}

func (s *StorageBackendInitWrapper) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	s.Init("")
	return s.StorageBackend.ListBlobs(param)
}

func (s *StorageBackendInitWrapper) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	s.Init("")
	return s.StorageBackend.DeleteBlob(param)
}

func (s *StorageBackendInitWrapper) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	s.Init("")
	return s.StorageBackend.DeleteBlobs(param)
}

func (s *StorageBackendInitWrapper) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	s.Init("")
	return s.StorageBackend.RenameBlob(param)
}

func (s *StorageBackendInitWrapper) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	s.Init("")
	return s.StorageBackend.CopyBlob(param)
}

func (s *StorageBackendInitWrapper) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	s.Init("")
	return s.StorageBackend.GetBlob(param)
}

func (s *StorageBackendInitWrapper) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	s.Init("")
	return s.StorageBackend.PutBlob(param)
}

func (s *StorageBackendInitWrapper) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	s.Init("")
	return s.StorageBackend.MultipartBlobBegin(param)
}

func (s *StorageBackendInitWrapper) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	s.Init("")
	return s.StorageBackend.MultipartBlobAdd(param)
}

func (s *StorageBackendInitWrapper) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	s.Init("")
	return s.StorageBackend.MultipartBlobAbort(param)
}

func (s *StorageBackendInitWrapper) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	s.Init("")
	return s.StorageBackend.MultipartBlobCommit(param)
}

func (s *StorageBackendInitWrapper) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	s.Init("")
	return s.StorageBackend.MultipartExpire(param)
}

func (s *StorageBackendInitWrapper) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error) {
	s.Init("")
	return s.StorageBackend.RemoveBucket(param)
}

func (s *StorageBackendInitWrapper) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error) {
	s.Init("")
	return s.StorageBackend.MakeBucket(param)
}

type StorageBackendInitError struct {
	error
}

func (e StorageBackendInitError) Init(key string) error {
	return e
}

func (e StorageBackendInitError) Capabilities() *Capabilities {
	return &Capabilities{}
}

func (e StorageBackendInitError) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	if param.Key == INIT_ERR_BLOB {
		return &HeadBlobOutput{
			BlobItemOutput: BlobItemOutput{
				Key:          &param.Key,
				Size:         uint64(len(e.Error())),
				LastModified: PTime(time.Now()),
			},
			ContentType: PString("text/plain"),
		}, nil
	} else {
		return nil, fuse.ENOENT
	}
}

func (e StorageBackendInitError) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	// return a fake blob
	if param.Prefix == nil || *param.Prefix == "" {
		return &ListBlobsOutput{
			Items: []BlobItemOutput{
				BlobItemOutput{
					Key:          PString(INIT_ERR_BLOB),
					Size:         uint64(len(e.Error())),
					LastModified: PTime(time.Now()),
				},
			},
		}, nil
	} else {
		return &ListBlobsOutput{}, nil
	}
}

func (e StorageBackendInitError) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	return nil, e
}

func (e StorageBackendInitError) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	return nil, e
}

func (e StorageBackendInitError) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	return nil, e
}

func (e StorageBackendInitError) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	return nil, e
}

func (e StorageBackendInitError) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	if param.Key == INIT_ERR_BLOB {
		errStr := e.Error()
		return &GetBlobOutput{
			HeadBlobOutput: HeadBlobOutput{
				BlobItemOutput: BlobItemOutput{
					Key:          &param.Key,
					Size:         uint64(len(errStr)),
					LastModified: PTime(time.Now()),
				},
				ContentType: PString("text/plain"),
			},
			Body: ioutil.NopCloser(strings.NewReader(errStr)),
		}, nil
	} else {
		return nil, fuse.ENOENT
	}
}

func (e StorageBackendInitError) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	return nil, e
}

func (e StorageBackendInitError) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	return nil, e
}

func (e StorageBackendInitError) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	return nil, e
}

func (e StorageBackendInitError) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	return nil, e
}

func (e StorageBackendInitError) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	return nil, e
}

func (e StorageBackendInitError) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	return nil, e
}

func (e StorageBackendInitError) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error) {
	return nil, e
}

func (e StorageBackendInitError) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error) {
	return nil, e
}
