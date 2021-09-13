package internal

import (
	"fmt"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jacobsa/fuse"
	"github.com/kahing/goofys/api/common"
	gproto "github.com/kahing/goofys/proto"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
	"io/ioutil"
	"net/url"
	"path"
	"strings"
)

// MetadataCachedS3Backend is a backend that uses a local cached metadata file
// to respond to file/directory metadata operation, and uses s3 backend to perform
// the content reads. This backend only supports read-only mode as it doesn't
// do any reconciliation of metadata difference between cached and remote.
type MetadataCachedS3Backend struct {
	s3Backend *S3Backend

	// Root directory node of filesystem metadata cache
	rootCache *gproto.NodeMetadata
}

var _ StorageBackend = &MetadataCachedS3Backend{}

func NewMetadataCachedS3(bucket, path string, flags *common.FlagStorage, config *common.S3Config) (*MetadataCachedS3Backend, error) { //nolint:lll
	s3Backend, err := NewS3(bucket, path, flags, config)
	if err != nil {
		return nil, err
	}

	parsedUrl, err := url.Parse(flags.MetadataCacheFile)
	if err != nil {
		return nil, errors.Wrap(err, "parse cache file url")
	}

	cache := &gproto.NodeMetadata{}
	if parsedUrl.Scheme == "s3" {
		// Read the cache from s3 cache path
		output, err := s3Backend.S3.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(parsedUrl.Host),
			Key:    aws.String(parsedUrl.Path),
		})
		if err != nil {
			return nil, errors.Wrap(err, "read metadata s3 file")
		}

		body := make([]byte, *output.ContentLength)
		_, err = output.Body.Read(body)
		if err != nil {
			return nil, errors.Wrap(err, "read metadata body")
		}

		if err := proto.Unmarshal(body, cache); err != nil {
			return nil, errors.Wrap(err, "unmarshal metadata proto")
		}
	} else {
		if parsedUrl.Scheme != "" {
			return nil, fmt.Errorf("unsupported metadata url scheme: %s", parsedUrl.Scheme)
		}

		body, err := ioutil.ReadFile(flags.MetadataCacheFile)
		if err != nil {
			return nil, errors.Wrap(err, "read metadata local file")
		}

		if err := proto.Unmarshal(body, cache); err != nil {
			return nil, errors.Wrap(err, "unmarshal metadata proto")
		}
	}

	s3Log.Infof("Initialized cached s3 fs using cache data from %s", flags.MetadataCacheFile)

	return &MetadataCachedS3Backend{
		s3Backend: s3Backend,
		rootCache: cache,
	}, nil
}

func (m MetadataCachedS3Backend) Init(key string) error {
	return m.s3Backend.Init(key)
}

func (m MetadataCachedS3Backend) Capabilities() *Capabilities {
	return m.s3Backend.Capabilities()
}

func (m MetadataCachedS3Backend) Bucket() string {
	return m.s3Backend.Bucket()
}

// findKey recursively finds the metadata key in the cache based on the paths
func (m *MetadataCachedS3Backend) findKey(node *gproto.NodeMetadata, paths []string, level int) *gproto.NodeMetadata {
	// If listing the root path, return the root cache
	if level == 1 && len(paths) == 1 && paths[0] == "" {
		return node
	}

	if !node.GetDirectory() {
		return nil
	}

	currentPath := paths[level-1]

	child, ok := node.Children[currentPath]
	if !ok {
		return nil
	}

	if len(paths) == level {
		return child
	}

	return m.findKey(child, paths, level+1)
}

func (m MetadataCachedS3Backend) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	s3Log.Infof("Head blob with cached called with %s", param.Key)
	paths := strings.Split(param.Key, "/")
	cachedMetadata := m.findKey(m.rootCache, paths, 1)
	if cachedMetadata == nil {
		return nil, fuse.ENOENT
	}

	return &HeadBlobOutput{
		BlobItemOutput: BlobItemOutput{
			Key:          &param.Key,
			LastModified: aws.Time(cachedMetadata.GetLastModifiedAt().AsTime()),
			Size:         cachedMetadata.GetSize(),
		},
		IsDirBlob: cachedMetadata.GetDirectory(),
	}, nil
}

func (m MetadataCachedS3Backend) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	s3Log.Infof("List blobs with cached called with %+v", param)
	var paths []string
	rootPath := false
	expectDir := true
	if param.Prefix == nil {
		paths = []string{""}
		rootPath = true
	} else {
		expectDir = strings.HasSuffix(*param.Prefix, "/")
		paths = strings.Split(*param.Prefix, "/")
	}
	cachedMetadata := m.findKey(m.rootCache, paths, 1)
	if cachedMetadata == nil {
		return &ListBlobsOutput{}, nil
	}

	if expectDir && !cachedMetadata.GetDirectory() {
		// We are specifically looking for a directory, and if it wasn't we should return
		return &ListBlobsOutput{}, nil
	}

	var prefixes []BlobPrefixOutput
	var items []BlobItemOutput
	for name, child := range cachedMetadata.GetChildren() {
		if child.GetDirectory() {
			prefixes = append(prefixes, BlobPrefixOutput{
				Prefix: aws.String(path.Join(*param.Prefix, child.Name) + "/"),
			})
		} else {
			items = append(items, BlobItemOutput{
				Key:          aws.String(path.Join(*param.Prefix, name)),
				LastModified: aws.Time(child.GetLastModifiedAt().AsTime()),
				Size:         child.GetSize(),
			})
		}
	}

	return &ListBlobsOutput{
		Prefixes: prefixes,
		Items:    items,
	}, nil
}

func (m MetadataCachedS3Backend) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	return nil, errors.New("delete blob is not supported in cached backend")
}

func (m MetadataCachedS3Backend) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	return nil, errors.New("delete blobs is not supported in cached backend")
}

func (m MetadataCachedS3Backend) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	return nil, errors.New("rename blob is not supported in cached backend")
}

func (m MetadataCachedS3Backend) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	return nil, errors.New("copy blob is not supported in cached backend")
}

func (m MetadataCachedS3Backend) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	return m.s3Backend.GetBlob(param)
}

func (m MetadataCachedS3Backend) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	return nil, errors.New("put blob is not supported in cached backend")
}

func (m MetadataCachedS3Backend) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	return nil, errors.New("multi part blob begin is not supported in cached backend")
}

func (m MetadataCachedS3Backend) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	return nil, errors.New("multi part blob add is not supported in cached backend")
}

func (m MetadataCachedS3Backend) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) { //nolint:lll
	return nil, errors.New("multi part blob abort is not supported in cached backend")
}

func (m MetadataCachedS3Backend) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) { //nolint:lll
	return nil, errors.New("multi part blob commit is not supported in cached backend")
}

func (m MetadataCachedS3Backend) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	return nil, errors.New("multi part blob expire is not supported in cached backend")
}

func (m MetadataCachedS3Backend) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error) {
	return nil, errors.New("remove bucket is not supported in cached backend")
}

func (m MetadataCachedS3Backend) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error) {
	return nil, errors.New("make bucket is not supported in cached backend")
}

func (m MetadataCachedS3Backend) Delegate() interface{} {
	return m.s3Backend
}