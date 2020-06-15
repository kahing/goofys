package internal

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	. "github.com/kahing/goofys/api/common"
	"google.golang.org/api/iterator"
	"strings"
	"syscall"
)

type GCSBackend struct {
	client *storage.Client       // Client is used to interact with GCS and safe for concurrent use
	config *GCSConfig            // GCS Config stores user's and bucket configuration
	cap    *Capabilities         // Capabilities is owned by the storage bucket
	bucket *storage.BucketHandle // provides set of methods to operate on a bucket
	logger *LogHandle            // logger for GCS backend
}

// NewGCS initializes GCS client and returns GCSBackend and error
func NewGCS(config *GCSConfig) (*GCSBackend, error) {
	var client *storage.Client
	var err error

	if config.Credentials != nil {
		ctx := context.Background()
		client, err = storage.NewClient(ctx)
	}

	if err != nil {
		return nil, err
	}

	return &GCSBackend{
		client: client,
		config: config,
		bucket: client.Bucket(config.Bucket),
		cap: &Capabilities{
			MaxMultipartSize: 5 * 1024 * 1024 * 1024,
			Name:             "gcs",
			// TODO: no parallel multipart in GCS, but they have resumable uploads
			NoParallelMultipart: false,
		},
		logger: GetLogger("gcs"),
	}, nil
}

func (g *GCSBackend) Init(key string) (err error) {
	g.logger.Debug("Initializes GCS")
	err = g.testBucket(key)

	return
}

func (g *GCSBackend) testBucket(key string) (err error) {
	ctx := context.Background()

	// require users to have read access to the bucket bucket.get)
	g.logger.Debug("Getting bucket info..")
	_, err = g.bucket.Attrs(ctx)
	if err != nil {
		return
	}

	g.logger.Debug("Getting object Info..")
	_, err = g.HeadBlob(&HeadBlobInput{Key: key})
	if err == storage.ErrObjectNotExist {
		err = nil
	}

	return
}

func (g *GCSBackend) Capabilities() *Capabilities {
	return g.cap
}

// Bucket() typically returns the bucket/prefix
func (g *GCSBackend) Bucket() string {
	return g.config.Bucket
}

func getResponseStatus(err error) string {
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}
	return "SUCCESS"
}

func (g *GCSBackend) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	attrs, err := g.bucket.Object(param.Key).Attrs(context.Background())
	g.logger.Debugf("HEAD %v = %v", param.Key, getResponseStatus(err))

	if err != nil {
		return nil, err
	}

	var metadata map[string]*string
	if attrs.Metadata != nil {
		metadata = mapGCSMetadataToBackendMetadata(attrs.Metadata)
	}

	return &HeadBlobOutput{
		BlobItemOutput: BlobItemOutput{
			Key:          &attrs.Name,
			ETag:         &attrs.Etag,
			LastModified: &attrs.Updated,
			Size:         uint64(attrs.Size),
			StorageClass: &attrs.StorageClass,
		},
		ContentType: &attrs.ContentType,
		IsDirBlob:   strings.HasSuffix(param.Key, "/"),
		Metadata:    metadata,
	}, nil
}

func mapGCSMetadataToBackendMetadata(m map[string]string) map[string]*string {
	newMap := make(map[string]*string)

	for k, v := range m {
		newMap[k] = &v
	}

	return newMap
}

func (g *GCSBackend) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	// TODO: Ignore object versions from listing
	query := storage.Query{}

	if param.Prefix != nil {
		query.Prefix = nilStr(param.Prefix)
	}
	if param.Delimiter != nil {
		query.Delimiter = nilStr(param.Delimiter)
	}

	prefixes := make([]BlobPrefixOutput, 0)
	items := make([]BlobItemOutput, 0)

	it := g.bucket.Objects(context.Background(), &query)
	g.logger.Debugf("LIST Prefix = %s Delim = %s", nilStr(param.Prefix), nilStr(param.Delimiter))
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}
		if attrs.Prefix != "" {
			prefixes = append(prefixes, BlobPrefixOutput{&attrs.Prefix})
		}
		if attrs.Name != "" {
			items = append(items, BlobItemOutput{
				Key:          &attrs.Name,
				ETag:         &attrs.Etag,
				LastModified: &attrs.Updated,
				Size:         uint64(attrs.Size),
				StorageClass: &attrs.StorageClass,
			})
		}
	}

	return &ListBlobsOutput{
		Prefixes:              prefixes,
		Items:                 items,
		NextContinuationToken: nil,
		IsTruncated:           false,
	}, nil
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
	obj := g.bucket.Object(param.Key)
	ctx := context.Background()

	var err error

	// Get Object metadata
	attrs, err := g.bucket.Object(param.Key).Attrs(context.Background())
	if err != nil {
		return nil, err
	}
	g.logger.Debugf("GET ATTRS %v = %s ", param.Key, getResponseStatus(err))

	// Get Object reader
	var rc *storage.Reader
	if param.Start != 0 || param.Count != 0 {
		rc, err = obj.NewRangeReader(ctx, int64(param.Start), int64(param.Count))
	} else {
		rc, err = obj.NewReader(ctx)
	}

	if err != nil {
		return nil, err
	}
	g.logger.Debugf("GET OBJECT %v = %s ", param.Key, getResponseStatus(err))
	//metadata := make(map[string]*string)
	//metadata["content-encoding"] = &rc.Attrs.ContentEncoding
	//metadata["cache-control"] = &rc.Attrs.CacheControl

	return &GetBlobOutput{
		HeadBlobOutput: HeadBlobOutput{
			BlobItemOutput: BlobItemOutput{
				Key:          &attrs.Name,
				ETag:         &attrs.Etag,
				LastModified: &rc.Attrs.LastModified,
				Size:         uint64(rc.Attrs.Size),
				StorageClass: &attrs.StorageClass,
			},
			ContentType: &rc.Attrs.ContentType,
			//Metadata: metadata,
		},
		Body: rc,
	}, nil
}

func (g *GCSBackend) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	return nil, syscall.EPERM
}

func (g *GCSBackend) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	return nil, syscall.EPERM
}

func (g *GCSBackend) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	return nil, syscall.EPERM
}

func (g *GCSBackend) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	return nil, syscall.EPERM
}

func (g *GCSBackend) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	return nil, syscall.EPERM
}
func (g *GCSBackend) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	return nil, syscall.EPERM
}
func (g *GCSBackend) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error) {
	return nil, syscall.EPERM
}
func (g *GCSBackend) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error) {
	return nil, syscall.EPERM
}

func (g *GCSBackend) Delegate() interface{} {
	return g
}

func (g *GCSBackend) Logger() *LogHandle {
	return g.logger
}
