package internal

import (
	"bytes"
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/jacobsa/fuse"
	"github.com/kahing/goofys/api/common"
	"golang.org/x/sync/errgroup"
	syncsem "golang.org/x/sync/semaphore"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"io/ioutil"
	"net"
	"strings"
	"syscall"
)

type GCSBackend struct {
	client *storage.Client       // Client is used to interact with GCS and safe for concurrent use
	config *common.GCSConfig            // GCS Config stores user's and bucket configuration
	cap    *Capabilities         // Capabilities is owned by the storage bucket
	bucket *storage.BucketHandle // provides set of methods to operate on a bucket
	logger *common.LogHandle            // logger for GCS backend
}

// NewGCS initializes GCS client and returns GCSBackend and error
func NewGCS(config *common.GCSConfig) (*GCSBackend, error) {
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
		logger: common.GetLogger("gcs"),
	}, nil
}

// Init initializes GCS mount by checking user's access to bucket and random object checks.
func (g *GCSBackend) Init(key string) (err error) {
	g.logger.Debug("Initializes GCS")
	err = g.testBucket(key)

	return
}

func (g *GCSBackend) testBucket(key string) (err error) {
	ctx := context.Background()

	// Require users to have read access to the bucket
	g.logger.Debugf("Getting bucket Info for gs://%s", g.Bucket())
	ctx, cancel := g.getContextWithTimeout(nil)
	defer cancel()

	_, err = g.bucket.Attrs(ctx)
	if err != nil {
		return
	}

	g.logger.Debugf("Getting object Info for %s", key)
	_, err = g.bucket.Object(key).Attrs(ctx)
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
	ctx, cancel := g.getContextWithTimeout(nil)
	defer cancel()

	attrs, err := g.bucket.Object(param.Key).Attrs(ctx)
	g.logger.Debugf("HEAD %v = %v", param.Key, getResponseStatus(err))

	if err != nil {
		return nil, mapGcsError(err)
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

func (g *GCSBackend) getContextWithTimeout(ctx context.Context) (context.Context, func()) {
	if ctx == nil{
		ctx = context.Background()
	}
	cancel := func() {}
	if g.config.HTTPTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, g.config.HTTPTimeout)
	}
	return ctx, cancel
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
	// TODO: Pagination Implementation
	query := storage.Query{}

	if param.Prefix != nil {
		query.Prefix = nilStr(param.Prefix)
	}
	if param.Delimiter != nil {
		query.Delimiter = nilStr(param.Delimiter)
	}

	prefixes := make([]BlobPrefixOutput, 0)
	items := make([]BlobItemOutput, 0)

	ctx, cancel := g.getContextWithTimeout(nil)
	defer cancel()

	it := g.bucket.Objects(ctx, &query)
	g.logger.Debugf("LIST Prefix = %s Delim = %s", nilStr(param.Prefix), nilStr(param.Delimiter))
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, mapGcsError(err)
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
	obj := g.bucket.Object(param.Key)

	ctx, cancel := g.getContextWithTimeout(nil)
	defer cancel()

	err := obj.Delete(ctx)

	g.logger.Debugf("DELETE OBJECT %v = %s ", param.Key, getResponseStatus(err))
	if err != nil {
		return nil, mapGcsError(err)
	}

	return &DeleteBlobOutput{}, nil
}

func (g *GCSBackend) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	// TODO: Add concurrency mechanisms for DeleteBlobs (ErrGroup + Semaphores)
	eg, rootCtx := errgroup.WithContext(context.Background())
	sem := syncsem.NewWeighted(100)

	for _, item := range param.Items {
		if err := sem.Acquire(rootCtx, 1); err != nil {
			return nil, err
		}
		curItem := item
		eg.Go(func() error {
			defer sem.Release(1)

			obj := g.bucket.Object(curItem)
			ctx, cancel := g.getContextWithTimeout(rootCtx)
			defer cancel()

			err := obj.Delete(ctx)
			return err
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, mapGcsError(err)
	}

	return &DeleteBlobsOutput{}, nil
}

func (g *GCSBackend) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	return nil, syscall.EPERM
}

func (g *GCSBackend) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	return nil, syscall.EPERM
}

func (g *GCSBackend) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	obj := g.bucket.Object(param.Key)

	parentCtx := context.Background()

	var err error

	// Get Object metadata
	ctx1, cancel := g.getContextWithTimeout(parentCtx)
	defer cancel()

	attrs, err := g.bucket.Object(param.Key).Attrs(ctx1)
	g.logger.Debugf("GET ATTRS %v = %s ", param.Key, getResponseStatus(err))
	if err != nil {
		return nil, mapGcsError(err)
	}

	// Get Object reader
	ctx2, cancel := g.getContextWithTimeout(parentCtx)
	defer cancel()

	var rc *storage.Reader
	if param.Start != 0 || param.Count != 0 {
		rc, err = obj.NewRangeReader(ctx2, int64(param.Start), int64(param.Count))
	} else {
		rc, err = obj.NewReader(ctx2)
	}
	if err != nil {
		return nil, mapGcsError(err)
	}
	data, err := ioutil.ReadAll(rc)
	defer rc.Close()

	g.logger.Debugf("GET OBJECT %v = %s ", param.Key, getResponseStatus(err))
	if err != nil {
		return nil, mapGcsError(err)
	}

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
		},
		Body: ioutil.NopCloser(bytes.NewReader(data)),
	}, nil
}

func (g *GCSBackend) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	data, err := ioutil.ReadAll(param.Body)
	if err != nil {
		return nil, mapGcsError(err)
	}

	ctx, cancel := g.getContextWithTimeout(nil)
	defer cancel()

	wc := g.bucket.Object(param.Key).NewWriter(ctx)
	if param.ContentType != nil && *param.ContentType != "" {
		wc.ContentType = nilStr(param.ContentType)
	}

	g.logger.Debugf("PUT OBJECT %v = %s ", param.Key, getResponseStatus(err))

	if _, err := wc.Write(data); err != nil {
		return nil, mapGcsError(err)
	}

	if err := wc.Close(); err != nil {
		return nil, mapGcsError(err)
	}
	attrs := wc.Attrs()

	return &PutBlobOutput{
		ETag: &attrs.Etag,
		LastModified: &attrs.Updated,
		StorageClass: &attrs.StorageClass,
	}, nil
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
	ctx, cancel := g.getContextWithTimeout(nil)
	defer cancel()
	err := g.bucket.Delete(ctx)
	if err != nil {
		return nil, mapGcsError(err)
	}

	return &RemoveBucketOutput{}, nil
  }
func (g *GCSBackend) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error) {
	ctx, cancel := g.getContextWithTimeout(nil)
	defer cancel()

	err := g.bucket.Create(ctx, g.config.ProjectId, nil)
	if err != nil {
		return nil, mapGcsError(err)
	}

	return &MakeBucketOutput{}, nil
}

func (g *GCSBackend) Delegate() interface{} {
	return g
}

func (g *GCSBackend) Logger() *common.LogHandle {
	return g.logger
}

func mapGcsError(err error) error {
	if err == nil {
		return nil
	}

	if err == storage.ErrObjectNotExist {
		return fuse.ENOENT
	}

	if e, ok := err.(*googleapi.Error); ok {
		switch e.Code {
		case 409:
			if strings.Contains(e.Message, "already") && strings.Contains(e.Message, "bucket") {
				return fuse.EEXIST
			}
		case 404:
			if e.Message == "Not Found" {
				return syscall.ENXIO
			}
		}
	}

	if e, ok := err.(net.Error); ok {
		if e.Timeout() {
			return syscall.ETIMEDOUT
		}
	}

	return err
}
