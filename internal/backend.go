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
	// set this to true to disable parallel upload
	NoParallelMultipart bool
	MaxMultipartSize    uint64
	// indicates that the blob store has native support for directories
	DirBlob bool
	Name    string
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

	RequestId string
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
	Prefixes              []BlobPrefixOutput
	Items                 []BlobItemOutput
	NextContinuationToken *string
	IsTruncated           bool

	RequestId string
}

type DeleteBlobInput struct {
	Key string
}

type DeleteBlobOutput struct {
	RequestId string
}

type DeleteBlobsInput struct {
	Items []string
}

type DeleteBlobsOutput struct {
	RequestId string
}

type RenameBlobInput struct {
	Source      string
	Destination string
}

type RenameBlobOutput struct {
	RequestId string
}

type CopyBlobInput struct {
	Source      string
	Destination string

	Size         *uint64
	ETag         *string            // if non-nil, do conditional copy
	Metadata     map[string]*string // if nil, copy from Source
	StorageClass *string            // if nil, copy from Source
}

type CopyBlobOutput struct {
	RequestId string
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

	RequestId string
}

type PutBlobInput struct {
	Key         string
	Metadata    map[string]*string
	ContentType *string
	DirBlob     bool

	Body io.ReadSeeker
	Size *uint64
}

type PutBlobOutput struct {
	ETag         *string
	LastModified *time.Time
	StorageClass *string

	RequestId string
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

	// additional backend specific data
	backendData interface{}
}

type MultipartBlobAddInput struct {
	Commit     *MultipartBlobCommitInput
	PartNumber uint32

	Body io.ReadSeeker

	Size   uint64 // GCS wants to know part size
	Last   bool   // GCS needs to know if this part is the last one
	Offset uint64 // ADLv2 needs to know offset
}

type MultipartBlobAddOutput struct {
	RequestId string
}

type MultipartBlobCommitOutput struct {
	ETag         *string
	LastModified *time.Time
	StorageClass *string

	RequestId string
}

type MultipartBlobAbortOutput struct {
	RequestId string
}

type MultipartExpireInput struct {
}

type MultipartExpireOutput struct {
	RequestId string
}

type RemoveBucketInput struct {
}

type RemoveBucketOutput struct {
	RequestId string
}

type MakeBucketInput struct {
}

type MakeBucketOutput struct {
	RequestId string
}

/// Implementations of all the functions here are expected to be
/// concurrency-safe, except for
///
/// Init() is called exactly once before any other functions are
/// called.
///
/// Capabilities()/Bucket() are expected to be const
type StorageBackend interface {
	Init(key string) error
	Capabilities() *Capabilities
	// typically this would return bucket/prefix
	Bucket() string
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
	Delegate() interface{}
}

type Delegator interface {
	Delegate() interface{}
}

var SmallActionsGate = Ticket{Total: 100}.Init()

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

func (l ListBlobsInput) String() string {
	return fmt.Sprintf("ListBlobsInput{Prefix: %v, Delim: %v, StartAfter: %v, MaxKeys: %v, ContToken: %v}",
		NilStr(l.Prefix),
		NilStr(l.Delimiter),
		NilStr(l.StartAfter),
		NilUint32(l.MaxKeys),
		NilStr(l.ContinuationToken))
}

func (b GetBlobInput) String() string {
	return fmt.Sprintf("GetBlobInput{Key: %s, Start: %d, Count: %d}", b.Key, b.Start, b.Count)
}

func (b BlobItemOutput) String() string {
	return fmt.Sprintf("%v: %v", *b.Key, b.Size)
}

func (b PutBlobInput) String() string {
	return fmt.Sprintf("PutBlobInput{Key: %s, Size: %d, ContentType: %s}", b.Key, NilUint64(b.Size),
		NilStr(b.ContentType))
}

func (b CopyBlobInput) String() string {
	return fmt.Sprintf("CopyBlobInput{Src: %s, Dest: %s, Size: %d}", b.Source, b.Destination, b.Size)
}

func (b MultipartBlobBeginInput) String() string {
	return fmt.Sprintf("MultipartBlobBeginInput{Key: %s, ContentType: %s}", b.Key, NilStr(b.ContentType))
}

func (b MultipartBlobAddInput) String() string {
	return fmt.Sprintf("MultipartBlobAddInput{Key: %s, Offset: %d, Size: %d, Last: %v}",
		NilStr(b.Commit.Key), b.Offset, b.Size, b.Last)
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
	s.init.Do(func() {
		s.initErr = s.StorageBackend.Init(s.initKey)
		if s.initErr != nil {
			log.Errorf("%T Init: %v", s.StorageBackend, s.initErr)
			s.StorageBackend = StorageBackendInitError{
				s.initErr,
				*s.StorageBackend.Capabilities(),
			}
		}
	})
	return s.initErr
}

func (s *StorageBackendInitWrapper) Capabilities() *Capabilities {
	return s.StorageBackend.Capabilities()
}

func (s *StorageBackendInitWrapper) Bucket() string {
	return s.StorageBackend.Bucket()
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
	cap Capabilities
}

func (e StorageBackendInitError) Init(key string) error {
	return e
}

func (e StorageBackendInitError) Delegate() interface{} {
	return e
}

func (e StorageBackendInitError) Capabilities() *Capabilities {
	return &e.cap
}

func (s StorageBackendInitError) Bucket() string {
	return ""
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
