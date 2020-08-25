package internal

import (
	"github.com/kahing/goofys/api/common"

	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"path"
	"strings"
	"syscall"

	"cloud.google.com/go/storage"
	"github.com/jacobsa/fuse"

	"golang.org/x/sync/errgroup"
	syncsem "golang.org/x/sync/semaphore"

	"google.golang.org/api/googleapi"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

type GCSBackend struct {
	bucketName string
	config     *common.GCSConfig // stores user and bucket configuration
	cap        Capabilities
	bucket     *storage.BucketHandle // provides set of methods to operate on a bucket
	logger     *common.LogHandle     // logger for GCS backend
}

const (
	maxListKeys int = 1000 // the max limit for number of elements during listObjects
)

type GCSMultipartBlobCommitInput struct {
	cancel context.CancelFunc // useful to abort a multipart upload in GCS
	writer *storage.Writer    // used to emulate mpu under GCS, which currently used a single gcsWriter
}

// NewGCS initializes a GCS Backend.
// It creates an authenticated client or unauthenticated client based on existing credentials in the environment.
func NewGCS(bucket string, config *common.GCSConfig) (*GCSBackend, error) {
	var client *storage.Client
	var err error

	// TODO: storage.NewClient has automated mechanisms to set up credentials together with HTTP settings.
	// Currently, we are using config.Credentials only to differentiate between creating an authenticated or
	// unauthenticated client not using it to initialize a client.

	// If config.Credentials are configured, we'll get an authenticated client.
	if config.Credentials != nil {
		client, err = storage.NewClient(context.Background())
	} else {
		// otherwise we will get an unauthenticated client. option.WithoutAuthentication() is necessary
		// because the API will generate an error if it could not find credentials and this option is unset.
		client, err = storage.NewClient(context.Background(), option.WithoutAuthentication())
	}

	if err != nil {
		return nil, err
	}

	return &GCSBackend{
		config:     config,
		bucketName: bucket,
		bucket:     client.Bucket(bucket),
		cap: Capabilities{
			MaxMultipartSize: 5 * 1024 * 1024 * 1024,
			Name:             "gcs",
			// parallel multipart upload is not supported in GCS
			NoParallelMultipart: true,
		},
		logger: common.GetLogger("gcs"),
	}, nil
}

// Init checks user's access to bucket.
func (g *GCSBackend) Init(key string) error {
	// We will do a successful mount if the user can list on the bucket.
	// This is different other backends because GCS does not differentiate between object not found and
	// bucket not found.
	prefix, _ := path.Split(key)
	_, err := g.ListBlobs(&ListBlobsInput{
		MaxKeys: PUInt32(1),
		Prefix:  PString(prefix),
	})
	g.logger.Debugf("INIT GCS: ListStatus = %s", getDebugResponseStatus(err))
	if err == syscall.ENXIO {
		return fmt.Errorf("bucket %v does not exist", g.bucketName)
	}
	// Errors can be returned directly since ListBlobs converts them to syscall errors.
	return err
}

func (g *GCSBackend) Capabilities() *Capabilities {
	return &g.cap
}

// Bucket returns the GCSBackend's bucket name.
func (g *GCSBackend) Bucket() string {
	return g.bucketName
}

func getDebugResponseStatus(err error) string {
	if err != nil {
		return fmt.Sprintf("ERROR: %v", err)
	}
	return "SUCCESS"
}

// HeadBlob gets the file object metadata.
func (g *GCSBackend) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	attrs, err := g.bucket.Object(param.Key).Attrs(context.Background())
	g.logger.Debugf("HEAD %v = %v", param.Key, getDebugResponseStatus(err))
	if err != nil {
		return nil, mapGCSError(err)
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
		Metadata:    PMetadata(attrs.Metadata),
	}, nil
}

func (g *GCSBackend) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	query := storage.Query{
		Prefix:      NilStr(param.Prefix),
		Delimiter:   NilStr(param.Delimiter),
		StartOffset: NilStr(param.StartAfter),
	}
	objectIterator := g.bucket.Objects(context.Background(), &query)

	// Set max keys, a number > 0 is required by the SDK.
	maxKeys := int(NilUint32(param.MaxKeys))
	if maxKeys == 0 {
		maxKeys = maxListKeys // follow the default JSON API mechanism to return 1000 items if maxKeys is not set.
	}

	pager := iterator.NewPager(objectIterator, maxKeys, NilStr(param.ContinuationToken))

	var entries []*storage.ObjectAttrs
	nextToken, err := pager.NextPage(&entries)
	g.logger.Debugf("LIST %s : %s", param, getDebugResponseStatus(err))
	if err != nil {
		return nil, mapGCSError(err)
	}

	var nextContToken *string
	if nextToken != "" {
		nextContToken = &nextToken
	}

	var prefixes []BlobPrefixOutput
	var items []BlobItemOutput
	for _, entry := range entries {
		// if blob is a prefix, then Prefix field will be set
		if entry.Prefix != "" {
			prefixes = append(prefixes, BlobPrefixOutput{&entry.Prefix})
		} else if entry.Name != "" { // otherwise for actual blob, Name field will set
			items = append(items, BlobItemOutput{
				Key:          &entry.Name,
				ETag:         &entry.Etag,
				LastModified: &entry.Updated,
				Size:         uint64(entry.Size),
				StorageClass: &entry.StorageClass,
			})
		} else {
			log.Errorf("LIST Unknown object: %v", entry)
		}
	}

	return &ListBlobsOutput{
		Prefixes:              prefixes,
		Items:                 items,
		NextContinuationToken: nextContToken,
		IsTruncated:           nextContToken != nil,
	}, nil
}

func (g *GCSBackend) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	err := g.bucket.Object(param.Key).Delete(context.Background())

	g.logger.Debugf("DELETE Object %v = %s ", param.Key, getDebugResponseStatus(err))
	if err != nil {
		return nil, mapGCSError(err)
	}

	return &DeleteBlobOutput{}, nil
}

// DeleteBlobs deletes multiple GCS blobs.
func (g *GCSBackend) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	// The go sdk does not support batch requests: https://issuetracker.google.com/issues/142641783
	// So we're using goroutines and errorgroup to delete multiple objects
	eg, rootCtx := errgroup.WithContext(context.Background())
	sem := syncsem.NewWeighted(100)

	for _, item := range param.Items {
		if err := sem.Acquire(rootCtx, 1); err != nil {
			return nil, err
		}
		curItem := item
		eg.Go(func() error {
			defer sem.Release(1)
			return g.bucket.Object(curItem).Delete(rootCtx)
		})
	}

	if err := eg.Wait(); err != nil {
		return nil, mapGCSError(err)
	}

	return &DeleteBlobsOutput{}, nil
}

// RenameBlob is not supported for GCS backend. So Goofys will do a CopyBlob followed by DeleteBlob for renames.
func (g *GCSBackend) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

// CopyBlob copies a source object to another destination object under the same bucket.
func (g *GCSBackend) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	src := g.bucket.Object(param.Source)
	dest := g.bucket.Object(param.Destination)

	copier := dest.CopierFrom(src)
	copier.StorageClass = NilStr(param.StorageClass)
	copier.Etag = NilStr(param.ETag)
	copier.Metadata = NilMetadata(param.Metadata)

	_, err := copier.Run(context.Background())
	g.logger.Debugf("Copy object %s = %s ", param, getDebugResponseStatus(err))
	if err != nil {
		return nil, mapGCSError(err)
	}

	return &CopyBlobOutput{}, nil
}

// GetBlob returns a file reader for a GCS object.
func (g *GCSBackend) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	obj := g.bucket.Object(param.Key).ReadCompressed(true)

	var reader *storage.Reader
	var err error
	if param.Count != 0 {
		reader, err = obj.NewRangeReader(context.Background(), int64(param.Start), int64(param.Count))
	} else if param.Start != 0 {
		reader, err = obj.NewRangeReader(context.Background(), int64(param.Start), -1)
	} else {
		// If we don't limit the range, the full object will be read
		reader, err = obj.NewReader(context.Background())
	}

	g.logger.Debugf("GET Blob %s = %v", param, getDebugResponseStatus(err))
	if err != nil {
		return nil, mapGCSError(err)
	}

	// Caveats: the SDK's reader object doesn't provide ETag, StorageClass, and Metadata attributes within a single
	// API call, hence we're not returning these information in the output.
	// Relevant GitHub issue: https://github.com/googleapis/google-cloud-go/issues/2740
	return &GetBlobOutput{
		HeadBlobOutput: HeadBlobOutput{
			BlobItemOutput: BlobItemOutput{
				Key:          PString(param.Key),
				LastModified: &reader.Attrs.LastModified,
				Size:         uint64(reader.Attrs.Size),
			},
			ContentType: &reader.Attrs.ContentType,
		},
		Body: reader,
	}, nil
}

// PutBlob writes a file to GCS.
func (g *GCSBackend) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	// Handle nil pointer error when param.Body is nil
	body := param.Body
	if body == nil {
		body = bytes.NewReader([]byte(""))
	}

	writer := g.bucket.Object(param.Key).NewWriter(context.Background())
	writer.ContentType = NilStr(param.ContentType)
	writer.Metadata = NilMetadata(param.Metadata)
	// setting chunkSize to be equal to the file size will make this a single request upload
	writer.ChunkSize = int(NilUint64(param.Size))

	_, err := io.Copy(writer, body)
	g.logger.Debugf("PUT Blob (to writer) %s = %s ", param, getDebugResponseStatus(err))
	if err != nil {
		return nil, mapGCSError(err)
	}

	err = writer.Close()
	g.logger.Debugf("PUT Blob (Flush) %v = %s ", param.Key, getDebugResponseStatus(err))
	if err != nil {
		return nil, mapGCSError(err)
	}

	attrs := writer.Attrs()

	return &PutBlobOutput{
		ETag: &attrs.Etag,
		//LastModified: &attrs.Updated, // this field exist in the upstream open source goofys repo
		StorageClass: &attrs.StorageClass,
	}, nil
}

// MultipartBlobBegin begins a multi part blob request.
// Under GCS backend, we'll initialize the gcsWriter object and the context for the multipart blob request here.
func (g *GCSBackend) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	ctx, cancel := context.WithCancel(context.Background())
	writer := g.bucket.Object(param.Key).NewWriter(ctx)
	writer.ChunkSize = g.config.ChunkSize
	writer.ContentType = NilStr(param.ContentType)
	writer.Metadata = NilMetadata(param.Metadata)

	g.logger.Debugf("Multipart Blob BEGIN: %s", param)

	return &MultipartBlobCommitInput{
		Key:      &param.Key,
		Metadata: param.Metadata,
		backendData: &GCSMultipartBlobCommitInput{
			writer: writer,
			cancel: cancel,
		},
	}, nil
}

// MultipartBlobAdd adds part of blob to the upload request.
// Under GCS backend, we'll write that blob part into the gcsWriter.
// TODO(deka): This is a temporary implementation to allow most tests to run.
// We might change this implementation in the future.
func (g *GCSBackend) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	commitData, ok := param.Commit.backendData.(*GCSMultipartBlobCommitInput)
	if !ok {
		panic("Incorrect commit data type")
	}

	// Handle nil pointer error when param.Body is nil
	body := param.Body
	if body == nil {
		body = bytes.NewReader([]byte(""))
	}

	n, err := io.Copy(commitData.writer, body)
	g.logger.Debugf("Multipart Blob ADD %s bytesWritten: %v = %s", param, n, getDebugResponseStatus(err))
	if err != nil {
		commitData.cancel()
		return nil, err
	}

	return &MultipartBlobAddOutput{}, nil
}

func (g *GCSBackend) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	commitData, ok := param.backendData.(*GCSMultipartBlobCommitInput)
	if !ok {
		panic("Incorrect commit data type")
	}
	g.logger.Debugf("Multipart Blob ABORT %v", param.Key)
	commitData.cancel()

	return &MultipartBlobAbortOutput{}, nil
}

func (g *GCSBackend) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	commitData, ok := param.backendData.(*GCSMultipartBlobCommitInput)
	if !ok {
		panic("Incorrect commit data type")
	}

	// Flushing a writer will make GCS to fully upload the buffer
	err := commitData.writer.Close()
	g.logger.Debugf("Multipart Blob COMMIT %v = %s ", param.Key, getDebugResponseStatus(err))
	if err != nil {
		commitData.cancel()
		return nil, mapGCSError(err)
	}
	attrs := commitData.writer.Attrs()

	return &MultipartBlobCommitOutput{
		ETag: &attrs.Etag,
	}, nil
}

func (g *GCSBackend) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	// No-op: GCS expires a resumable session after 7 days automatically
	return &MultipartExpireOutput{}, nil
}

func (g *GCSBackend) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error) {
	err := g.bucket.Delete(context.Background())
	if err != nil {
		return nil, mapGCSError(err)
	}
	return &RemoveBucketOutput{}, nil
}

func (g *GCSBackend) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error) {
	// Requires an authenticated credentials
	err := g.bucket.Create(context.Background(), g.config.Credentials.ProjectID, nil)
	if err != nil {
		return nil, mapGCSError(err)
	}

	return &MakeBucketOutput{}, nil
}

func (g *GCSBackend) Delegate() interface{} {
	return g
}

// mapGCSError maps an error to syscall / fuse errors.
func mapGCSError(err error) error {
	if err == nil {
		return nil
	}

	if err == storage.ErrObjectNotExist {
		return fuse.ENOENT
	}

	// this error can be returned during list operation if the bucket does not exist
	if err == storage.ErrBucketNotExist {
		return syscall.ENXIO
	}

	if e, ok := err.(*googleapi.Error); ok {
		switch e.Code {
		case 409:
			return fuse.EEXIST
		case 404:
			return fuse.ENOENT
		// Retryable errors:
		// https://cloud.google.com/storage/docs/json_api/v1/status-codes#429_Too_Many_Requests
		// https://cloud.google.com/storage/docs/json_api/v1/status-codes#500_Internal_Server_Error
		case 429, 500, 502, 503, 504:
			return syscall.EAGAIN
		default:
			// return syscall error if it's not nil
			fuseErr := mapHttpError(e.Code)
			if fuseErr != nil {
				return fuseErr
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
