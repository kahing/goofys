package internal

import (
	"cloud.google.com/go/storage"
	"context"

	. "github.com/kahing/goofys/api/common"
	"google.golang.org/api/option"
	"syscall"
)

// GCSBackend
type GCSBackend struct{
	client *storage.Client
	config *GCSConfig
	cap *Capabilities
}


// NewGCS returns GCSBackend
func NewGCS(config *GCSConfig) (*GCSBackend, error){
	var client *storage.Client
	var err error

	if config.Credentials != nil {
		ctx := context.Background()
		client, err = storage.NewClient(ctx)
	} else {
		ctx := context.Background()
		client, err = storage.NewClient(ctx, option.WithoutAuthentication())
	}

	if err != nil {
		return nil, err
	}

	return &GCSBackend{
		client: client,
		config: config,
	}, nil
}

var gcsLogger = GetLogger("GCS")

func (g *GCSBackend) Init(key string) error {
	return nil
}

func (g *GCSBackend) testBucket(key string) (err error) {
	_, err = g.HeadBlob(&HeadBlobInput{Key: key})
	//if err != nil {
	//	err = mapAZBError(err)
	//	if err == fuse.ENOENT {
	//		err = nil
	//	}
	//}

	return
}

func (g *GCSBackend) Capabilities() *Capabilities {
	return g.cap
}

// typically this would return bucket/prefix
func (g *GCSBackend) Bucket() string {
	return g.config.Bucket
}

func (g *GCSBackend) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	//g.client.Bucket()
	return nil, syscall.EPERM
}

func (g *GCSBackend) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	return nil, syscall.EPERM
}

func (g *GCSBackend) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	return nil, syscall.EPERM
}

func (g *GCSBackend) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	return nil, syscall.EPERM
}

func (g *GCSBackend) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	return nil, syscall.EPERM
}

func (g *GCSBackend) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	return nil, syscall.EPERM
}

func (g *GCSBackend) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	return nil, syscall.EPERM
}

func (g *GCSBackend) PutBlob(param *PutBlobInput) (*PutBlobOutput, error){
	return nil, syscall.EPERM
}

func (g *GCSBackend) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	return nil, syscall.EPERM
}

func (g *GCSBackend) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error){
	return nil, syscall.EPERM
}

func (g *GCSBackend) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error){
	return nil, syscall.EPERM
}

func (g *GCSBackend) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error){
	return nil, syscall.EPERM
}
func (g *GCSBackend) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error){
	return nil, syscall.EPERM
}
func (g *GCSBackend) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error){
	return nil, syscall.EPERM
}
func (g *GCSBackend) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error){
	return nil, syscall.EPERM
}

func (g *GCSBackend) Delegate() interface{} {
	return g
}