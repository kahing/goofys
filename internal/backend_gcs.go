package internal

import (
	"cloud.google.com/go/storage"
	"context"
	"strings"

	. "github.com/kahing/goofys/api/common"
	"google.golang.org/api/option"
	"syscall"
)

// GCSBackend
type GCSBackend struct{
	client *storage.Client
	config *GCSConfig
	cap *Capabilities
	bucket *storage.BucketHandle
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
		bucket: client.Bucket(config.Bucket),
	}, nil
}

var gcsLogger = GetLogger("GCS")

func (g *GCSBackend) Init(key string) error {
	return nil
}

func (g *GCSBackend) testBucket(key string) (err error) {
	ctx := context.Background()

	// v01: Require users to have read access to the bucket (bucket.list and bucket.get)
	_, err = g.bucket.Attrs(ctx)
	if err != nil {
		return err
	}

	_, err = g.HeadBlob(&HeadBlobInput{Key: key})
	if err == storage.ErrObjectNotExist {
		err = nil
	}

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
	objAttrs, err := g.bucket.Object(param.Key).Attrs(context.Background())
	if err != nil {
		return nil, err
	}
	
	return &HeadBlobOutput{
		BlobItemOutput: BlobItemOutput{
			Key: &objAttrs.Name,
			ETag: &objAttrs.Etag,
			LastModified: &objAttrs.Updated,
			Size: uint64(objAttrs.Size),
		},
		ContentType: &objAttrs.ContentType,
		IsDirBlob: strings.HasSuffix(param.Key, "/"),
		Metadata: mapGCSMetadataToBackendMetadata(objAttrs.Metadata),
	}, nil
}

func mapGCSMetadataToBackendMetadata(m map[string]string) map[string]*string {
	newMap := make(map[string]*string)

	if m != nil {
		for k, v := range m {
			lower := strings.ToLower(k)
			newMap[lower] = &v
		}
	}

	return newMap
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