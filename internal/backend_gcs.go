package internal

import (
	"cloud.google.com/go/storage"
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/jacobsa/fuse"
	"github.com/kahing/goofys/api/common"
	"golang.org/x/sync/errgroup"
	syncsem "golang.org/x/sync/semaphore"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"io/ioutil"
	"net"
	"strings"
	"syscall"
)

type GCSBackend struct {
	client *storage.Client       // Client is used to interact with GCS and safe for concurrent use
	config *common.GCSConfig     // GCS Config stores user's and bucket configuration
	cap    *Capabilities         // Capabilities is owned by the storage bucket
	bucket *storage.BucketHandle // provides set of methods to operate on a bucket
	logger *common.LogHandle     // logger for GCS backend
}

// NewGCS initializes GCS Backend.
// It initializes a new storage.Client, BucketHandle, Logger, and GCS storage Capabilities.
// It returns GCSBackend or error.
func NewGCS(config *common.GCSConfig) (*GCSBackend, error) {
	var client *storage.Client
	var err error

	ctx := context.Background()
	if config.Credentials != nil {
		client, err = storage.NewClient(ctx)
	} else {
		client, err = storage.NewClient(ctx, option.WithoutAuthentication())
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
			NoParallelMultipart: true,
		},
		logger: common.GetLogger("gcs"),
	}, nil
}

// Init initializes GCS by checking user's access to bucket.
// It returns an error if the user has no access to the Bucket.
func (g *GCSBackend) Init(key string) (err error) {
	g.logger.Debug("Initializes GCS")
	err = g.testBucket(key)

	return
}

// testBucket check if user has access to the bucket
// It returns an error if the user has no access to the bucket.
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

// Capabilities returns GCSBackend capabilities.
func (g *GCSBackend) Capabilities() *Capabilities {
	return g.cap
}

// Bucket returns the GCSBackend bucket.
func (g *GCSBackend) Bucket() string {
	return g.config.Bucket
}

func getResponseStatus(err error) string {
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}
	return "SUCCESS"
}

// HeadBlob gets the file object metadata on GCS.
// It returns the HeadBlobOutput if request is successful or error if encountered.
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

// getContextWithTimeout generates a context with timeout if the HTTPTimeout is not 0.
// It will create a new context if the parent context is nil or a child context if it is not nil.
// It returns the new context and a cancel function.
func (g *GCSBackend) getContextWithTimeout(ctx context.Context) (context.Context, func()) {
	if ctx == nil {
		ctx = context.Background()
	}
	cancel := func() {}
	if g.config.HTTPTimeout != 0 {
		ctx, cancel = context.WithTimeout(ctx, g.config.HTTPTimeout)
	}
	return ctx, cancel
}

// mapGCSMetadataToBackendMetadata converts the original map[string]string to map[string]*string.
func mapGCSMetadataToBackendMetadata(m map[string]string) map[string]*string {
	newMap := make(map[string]*string)

	for k, v := range m {
		newMap[k] = &v
	}

	return newMap
}

// ListBlobs generates list of blobs in GCS given the prefix and delimiter queries.
// It is capable to perform pagination or non-pagination requests depending on user
// the availability of MaxKeys or ContinuationToken param.
// It returns ListBlobsOutput if the request is successful and error if encountered.
func (g *GCSBackend) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	// TODO: Ignore object versions from listing
	requestId := getUUID()
	query := storage.Query{}

	if param.Prefix != nil {
		query.Prefix = nilStr(param.Prefix)
	}
	if param.Delimiter != nil {
		query.Delimiter = nilStr(param.Delimiter)
	}

	ctx, cancel := g.getContextWithTimeout(nil)
	defer cancel()

	it := g.bucket.Objects(ctx, &query)

	var prefixes []BlobPrefixOutput
	var items []BlobItemOutput
	var nextContToken *string

	// With Pagination
	if param.MaxKeys != nil || param.ContinuationToken != nil {
		pager := iterator.NewPager(it, int(*param.MaxKeys), nilStr(param.ContinuationToken))
		var entries []*storage.ObjectAttrs
		nextToken, err := pager.NextPage(&entries)

		if nextToken != "" {
			nextContToken = &nextToken
		}

		g.logger.Debugf("LIST Prefix = %s Delim = %s: %s", nilStr(param.Prefix),
			nilStr(param.Delimiter), getResponseStatus(err))

		if err != nil {
			return nil, mapGcsError(err)
		}

		for _, entry := range entries {
			if entry.Prefix != "" {
				prefixes = append(prefixes, BlobPrefixOutput{&entry.Prefix})
			}
			if entry.Name != "" {
				items = append(items, BlobItemOutput{
					Key:          &entry.Name,
					ETag:         &entry.Etag,
					LastModified: &entry.Updated,
					Size:         uint64(entry.Size),
					StorageClass: &entry.StorageClass,
				})
			}
		}
	} else {
		// No Pagination
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
	}

	return &ListBlobsOutput{
		Prefixes:              prefixes,
		Items:                 items,
		NextContinuationToken: nextContToken,
		IsTruncated:           nextContToken != nil,
		RequestId:             requestId,
	}, nil
}

// DeleteBlob deletes a GCS blob.
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

// DeleteBlobs deletes multiple GCS blobs.
// It uses concurrent mechanism to deletes object using semaphores and errorgroup.
func (g *GCSBackend) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
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

// RenameBlob is not supported for GCS backend.
func (g *GCSBackend) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

// CopyBlob copies a source object to another destination object under the same bucket.
func (g *GCSBackend) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	requestId := getUUID()
	src := g.bucket.Object(param.Source)
	dest := g.bucket.Object(param.Destination)

	ctx, cancel := g.getContextWithTimeout(nil)
	defer cancel()

	// Copy content and modify metadata.
	copier := dest.CopierFrom(src)
	_, err := copier.Run(ctx)

	if err != nil {
		return nil, mapGcsError(err)
	}

	return &CopyBlobOutput{
		RequestId: requestId,
	}, nil
}

// GetBlob returns a file reader for a GCS object.
func (g *GCSBackend) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	requestId := getUUID()
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

	// TODO: Handle Timeout
	//ctx2 := context.Background()

	// Get Object reader
	ctx2, cancel := g.getContextWithTimeout(parentCtx)
	defer cancel()

	var rc *storage.Reader
	if param.Start != 0 || param.Count != 0 {
		rc, err = obj.NewRangeReader(ctx2, int64(param.Start), int64(param.Count))
	} else {
		rc, err = obj.NewReader(ctx2)
	}
	g.logger.Debugf("GET OBJECT %v = %s ", param.Key, getResponseStatus(err))
	if err != nil {
		return nil, mapGcsError(err)
	}
	//data, err := ioutil.ReadAll(rc)
	//defer rc.Close()
	//
	//g.logger.Debugf("GET OBJECT %v = %s ", param.Key, getResponseStatus(err))
	//if err != nil {
	//	return nil, mapGcsError(err)
	//}

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
		//Body:      ioutil.NopCloser(bytes.NewReader(data)),
		Body:      rc,
		RequestId: requestId,
	}, nil
}

// PutBlob writes a file to GCS.
func (g *GCSBackend) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	requestId := getUUID()
	data, err := ioutil.ReadAll(param.Body)
	g.logger.Debugf("PUT BLOB READ %v = %s ", param.Key, getResponseStatus(err))
	if err != nil {
		return nil, mapGcsError(err)
	}

	// TODO: PUT request handling
	//ctx, cancel := g.getContextWithTimeout(nil)
	//defer cancel()

	ctx := context.Background()
	wc := g.bucket.Object(param.Key).NewWriter(ctx)

	if param.ContentType != nil && *param.ContentType != "" {
		wc.ContentType = nilStr(param.ContentType)
	}

	g.logger.Debugf("PUT BLOB %v = %s ", param.Key, getResponseStatus(err))

	if _, err := wc.Write(data); err != nil {
		return nil, mapGcsError(err)
	}

	g.logger.Debugf("PUT BLOB FLUSH %v = %s ", param.Key, getResponseStatus(err))

	if err := wc.Close(); err != nil {
		return nil, mapGcsError(err)
	}
	attrs := wc.Attrs()

	return &PutBlobOutput{
		ETag:         &attrs.Etag,
		LastModified: &attrs.Updated,
		StorageClass: &attrs.StorageClass,
		RequestId:    requestId,
	}, nil
}

func getUUID() string {
	requestId := uuid.New().String()
	return requestId
}

func (g *GCSBackend) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	return nil, syscall.ENOTSUP
}

func (g *GCSBackend) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	return nil, syscall.ENOTSUP
}

func (g *GCSBackend) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	return nil, syscall.ENOTSUP
}

func (g *GCSBackend) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	return nil, syscall.ENOTSUP
}
func (g *GCSBackend) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	return nil, syscall.ENOTSUP
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
