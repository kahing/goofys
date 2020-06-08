package internal

import (
	//"golang.org/x/oauth2/google"
	"syscall"
)
//
type GCSBackend struct{
	//creds *google.Credentials
	//bucket string
}
//
//func NewGCS(key string) *GCSBackend{
//	ctx := context.Background()
//
//	// v0.1: only allows authenticated user
//	credentials, err := google.FindDefaultCredentials(ctx, storage.ScopeReadOnly)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	return &GCSBackend{
//		creds: credentials,
//		bucket: key,
//	}
//}

func (g *GCSBackend) Init(key string) error {
	return nil
}

func (g *GCSBackend) Capabilities() *Capabilities {
	return nil
}

// typically this would return bucket/prefix
func (g *GCSBackend) Bucket() string {
	return ""
}
func (g *GCSBackend) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
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