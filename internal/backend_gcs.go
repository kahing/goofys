package internal

type GCSBackend struct{
	bucket string
}

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
	return nil, nil
}

func (g *GCSBackend) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	return nil, nil
}

func (g *GCSBackend) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	return nil, nil
}

func (g *GCSBackend) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	return nil, nil
}

func (g *GCSBackend) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	return nil, nil
}

func (g *GCSBackend) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	return nil, nil
}

func (g *GCSBackend) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	return nil, nil
}

func (g *GCSBackend) PutBlob(param *PutBlobInput) (*PutBlobOutput, error){
	return nil, nil
}

func (g *GCSBackend) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	return nil, nil
}

func (g *GCSBackend) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error){
	return nil, nil
}

func (g *GCSBackend) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error){
	return nil, nil
}

func (g *GCSBackend) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error){
	return nil, nil
}
func (g *GCSBackend) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error){
	return nil, nil
}
func (g *GCSBackend) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error){
	return nil, nil
}
func (g *GCSBackend) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error){
	return nil, nil
}

func (g *GCSBackend) Delegate() interface{} {
	return g
}