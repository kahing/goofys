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
	"context"
	"encoding/base64"
	"fmt"
	"net/url"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/Azure/azure-pipeline-go/pipeline"
	"github.com/Azure/azure-storage-blob-go/azblob"

	"github.com/google/uuid"
	"github.com/jacobsa/fuse"
	"github.com/sirupsen/logrus"
)

type AZBlob struct {
	fs  *Goofys
	cap Capabilities

	mu sync.Mutex
	u  *azblob.ServiceURL
	c  *azblob.ContainerURL

	pipeline pipeline.Pipeline

	bucket           string
	bareURL          string
	sasTokenProvider SASTokenProvider
}

type SASTokenProvider func() (string, error)

const AZBlobEndpoint = "https://%v.blob.core.windows.net?%%v"
const AzuriteEndpoint = "http://127.0.0.1:8080/%v/?%%v"
const AzureDirBlobMetadataKey = "hdi_isfolder"

func NewAZBlob(fs *Goofys, container string, config *FlagStorage) *AZBlob {
	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{
		Log: pipeline.LogOptions{
			Log: func(level pipeline.LogLevel, msg string) {
				// naive casting kind of works because pipeline.INFO maps
				// to 5 which is logrus.DEBUG
				if level == pipeline.LogError {
					// somehow some http errors
					// are logged at Error, we
					// already log unhandled
					// errors so no need to do
					// that here
					level = pipeline.LogInfo
				}
				s3Log.Log(logrus.Level(uint32(level)), msg)
			},
			ShouldLog: func(level pipeline.LogLevel) bool {
				if level == pipeline.LogError {
					// somehow some http errors
					// are logged at Error, we
					// already log unhandled
					// errors so no need to do
					// that here
					level = pipeline.LogInfo
				}
				return s3Log.IsLevelEnabled(logrus.Level(uint32(level)))
			},
		},
		RequestLog: azblob.RequestLogOptions{
			LogWarningIfTryOverThreshold: time.Duration(-1),
		},
	})
	var bareURL string
	if config.AZAccountName != "" {
		bareURL = fmt.Sprintf(config.Endpoint, config.AZAccountName)
	} else {
		// endpoint already contains account name
		bareURL = config.Endpoint
	}

	var sasTokenProvider SASTokenProvider

	if strings.HasPrefix(config.AZAccountKey, "sig=") {
		// it's a SAS signature
		sasTokenProvider = func() (string, error) {
			return config.AZAccountKey, nil
		}
	} else {
		credential, err := azblob.NewSharedKeyCredential(config.AZAccountName, config.AZAccountKey)
		if err != nil {
			s3Log.Errorf("Unable to construct credential: %v", err)
			return nil
		}

		sasTokenProvider = func() (string, error) {
			sasQueryParams, err := azblob.AccountSASSignatureValues{
				Protocol:   azblob.SASProtocolHTTPSandHTTP,
				ExpiryTime: time.Now().UTC().Add(48 * time.Hour),
				Services:   azblob.AccountSASServices{Blob: true}.String(),
				ResourceTypes: azblob.AccountSASResourceTypes{
					Service:   true,
					Container: true,
					Object:    true,
				}.String(),
				Permissions: azblob.AccountSASPermissions{
					Read:   true,
					Write:  true,
					Delete: true,
					List:   true,
					Create: true,
				}.String(),
			}.NewSASQueryParameters(credential)
			if err != nil {
				return "", err
			}

			return sasQueryParams.Encode(), nil
		}
	}

	b := &AZBlob{
		fs: fs,
		cap: Capabilities{
			MaxMultipartSize: 100 * 1024 * 1024,
		},
		pipeline:         p,
		bucket:           container,
		bareURL:          bareURL,
		sasTokenProvider: sasTokenProvider,
	}

	return b
}

func (b *AZBlob) Capabilities() *Capabilities {
	return &b.cap
}

func (b *AZBlob) refreshToken() (*azblob.ContainerURL, error) {
	b.mu.Lock()

	if b.c == nil {
		b.mu.Unlock()
		return b.updateToken()
	} else {
		defer b.mu.Unlock()
		return b.c, nil
	}
}

func (b *AZBlob) updateToken() (*azblob.ContainerURL, error) {
	token, err := b.sasTokenProvider()
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(fmt.Sprintf(b.bareURL, token))
	if err != nil {
		return nil, err
	}

	serviceURL := azblob.NewServiceURL(*u, b.pipeline)
	containerURL := serviceURL.NewContainerURL(b.bucket)

	b.mu.Lock()
	defer b.mu.Unlock()

	b.u = &serviceURL
	b.c = &containerURL

	return b.c, nil
}

func (b *AZBlob) testBucket() (err error) {
	randomObjectName := b.fs.key(RandStringBytesMaskImprSrc(32))

	_, err = b.HeadBlob(&HeadBlobInput{Key: *randomObjectName})
	if err != nil {
		err = mapAZBError(err)
		if err == fuse.ENOENT {
			err = nil
		}
	}

	return
}

func (b *AZBlob) Init() error {
	_, err := b.updateToken()
	if err != nil {
		return err
	}

	err = b.testBucket()
	return err
}

func mapAZBError(err error) error {
	if err == nil {
		return nil
	}

	if stgErr, ok := err.(azblob.StorageError); ok {
		switch stgErr.ServiceCode() {
		case azblob.ServiceCodeBlobAlreadyExists:
			return syscall.EACCES
		case azblob.ServiceCodeBlobNotFound:
			return fuse.ENOENT
		case azblob.ServiceCodeContainerAlreadyExists:
			return syscall.EEXIST
		case azblob.ServiceCodeContainerBeingDeleted:
			return syscall.EAGAIN
		case azblob.ServiceCodeContainerDisabled:
			return syscall.EACCES
		case azblob.ServiceCodeContainerNotFound:
			return fuse.ENOENT
		case azblob.ServiceCodeCopyAcrossAccountsNotSupported:
			return fuse.EINVAL
		case azblob.ServiceCodeSourceConditionNotMet:
			return fuse.EINVAL
		case azblob.ServiceCodeSystemInUse:
			return syscall.EAGAIN
		case azblob.ServiceCodeTargetConditionNotMet:
			return fuse.EINVAL
		case azblob.ServiceCodeBlobBeingRehydrated:
			return syscall.EAGAIN
		case azblob.ServiceCodeBlobArchived:
			return fuse.EINVAL
		case azblob.ServiceCodeAccountBeingCreated:
			return syscall.EAGAIN
		case azblob.ServiceCodeAuthenticationFailed:
			return syscall.EACCES
		case azblob.ServiceCodeConditionNotMet:
			return syscall.EBUSY
		case azblob.ServiceCodeInternalError:
			return syscall.EAGAIN
		case azblob.ServiceCodeInvalidAuthenticationInfo:
			return syscall.EACCES
		case azblob.ServiceCodeOperationTimedOut:
			return syscall.EAGAIN
		case azblob.ServiceCodeResourceNotFound:
			return fuse.ENOENT
		case azblob.ServiceCodeServerBusy:
			return syscall.EAGAIN
		case "AuthorizationFailure": // from Azurite emulator
			return syscall.EACCES
		default:
			s3Log.Errorf("code=%v status=%v err=%v", stgErr.ServiceCode(), stgErr.Response().Status, stgErr)
			return stgErr
		}
	} else {
		return err
	}
}

func pMetadata(m map[string]string) map[string]*string {
	metadata := make(map[string]*string)
	for k, _ := range m {
		v := m[k]
		metadata[k] = &v
	}
	return metadata
}

func nilMetadata(m map[string]*string) map[string]string {
	metadata := make(map[string]string)
	for k, v := range m {
		metadata[k] = nilStr(v)
	}
	return metadata
}

func (b *AZBlob) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	c, err := b.refreshToken()
	if err != nil {
		return nil, err
	}

	blob := c.NewBlobURL(param.Key)
	resp, err := blob.GetProperties(context.TODO(), azblob.BlobAccessConditions{})
	if err != nil {
		return nil, mapAZBError(err)
	}

	metadata := resp.NewMetadata()
	isDir := strings.HasSuffix(param.Key, "/")
	if !isDir && metadata != nil {
		_, isDir = metadata[AzureDirBlobMetadataKey]
	}

	return &HeadBlobOutput{
		BlobItemOutput: BlobItemOutput{
			Key:          &param.Key,
			ETag:         PString(string(resp.ETag())),
			LastModified: PTime(resp.LastModified()),
			Size:         uint64(resp.ContentLength()),
			StorageClass: PString(resp.AccessTier()),
		},
		ContentType: PString(resp.ContentType()),
		Metadata:    pMetadata(metadata),
		IsDirBlob:   isDir,
	}, nil
}

func nilStr(v *string) string {
	if v == nil {
		return ""
	} else {
		return *v
	}
}
func nilUint32(v *uint32) uint32 {
	if v == nil {
		return 0
	} else {
		return *v
	}
}

func (b *AZBlob) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	c, err := b.refreshToken()
	if err != nil {
		return nil, err
	}

	prefixes := make([]BlobPrefixOutput, 0)
	items := make([]BlobItemOutput, 0)

	var blobItems []azblob.BlobItem
	var nextMarker *string

	options := azblob.ListBlobsSegmentOptions{
		Prefix:     nilStr(param.Prefix),
		MaxResults: int32(nilUint32(param.MaxKeys)),
		Details: azblob.BlobListingDetails{
			// blobfuse (following wasb) convention uses
			// an empty blob with "hdi_isfolder" metadata
			// set to represent a folder. So we include
			// metadaata in listing to discover that and
			// convert the result back to what we expect
			// (which is a "dir/" blob)
			// https://github.com/Azure/azure-storage-fuse/issues/222
			// https://blogs.msdn.microsoft.com/mostlytrue/2014/04/22/wasb-back-stories-masquerading-a-key-value-store/
			Metadata: true,
		},
	}

	if param.Delimiter != nil {
		resp, err := c.ListBlobsHierarchySegment(context.TODO(),
			azblob.Marker{
				param.ContinuationToken,
			},
			nilStr(param.Delimiter),
			options)
		if err != nil {
			return nil, mapAZBError(err)
		}

		for i, _ := range resp.Segment.BlobPrefixes {
			p := resp.Segment.BlobPrefixes[i]
			prefixes = append(prefixes, BlobPrefixOutput{Prefix: &p.Name})
		}

		if b.fs.flags.Endpoint == AzuriteEndpoint &&
			// XXX in Azurite this is not sorted
			!sort.IsSorted(sortBlobPrefixOutput(prefixes)) {
			sort.Sort(sortBlobPrefixOutput(prefixes))
		}

		blobItems = resp.Segment.BlobItems
		nextMarker = resp.NextMarker.Val
	} else {
		resp, err := c.ListBlobsFlatSegment(context.TODO(),
			azblob.Marker{
				param.ContinuationToken,
			},
			options)
		if err != nil {
			return nil, mapAZBError(err)
		}

		blobItems = resp.Segment.BlobItems
		nextMarker = resp.NextMarker.Val

		if b.fs.flags.Endpoint == AzuriteEndpoint &&
			!sort.IsSorted(sortBlobItemOutput(items)) {
			sort.Sort(sortBlobItemOutput(items))
		}
	}

	var sortItems bool

	for idx, _ := range blobItems {
		i := &blobItems[idx]
		p := &i.Properties

		if i.Metadata[AzureDirBlobMetadataKey] != "" {
			i.Name = i.Name + "/"

			if param.Delimiter != nil {
				// do we already have such a prefix?
				n := len(prefixes)
				if idx := sort.Search(n, func(idx int) bool {
					return *prefixes[idx].Prefix >= i.Name
				}); idx >= n || *prefixes[idx].Prefix != i.Name {
					if idx >= n {
						prefixes = append(prefixes, BlobPrefixOutput{
							Prefix: &i.Name,
						})
					} else {
						prefixes = append(prefixes, BlobPrefixOutput{})
						copy(prefixes[idx+1:], prefixes[idx:])
						prefixes[idx].Prefix = &i.Name
					}
				}
				continue
			} else {
				sortItems = true
			}
		}

		items = append(items, BlobItemOutput{
			Key:          &i.Name,
			ETag:         PString(string(p.Etag)),
			LastModified: PTime(p.LastModified),
			Size:         uint64(*p.ContentLength),
			StorageClass: PString(string(p.AccessTier)),
		})
	}
	// items are supposed to be alphabetical, but if there was a directory we would
	// have changed the ordering. XXX re-sort this for now but we can probably
	// insert smarter instead
	if sortItems {
		sort.Sort(sortBlobItemOutput(items))
	}

	if nextMarker != nil && *nextMarker == "" {
		nextMarker = nil
	}

	return &ListBlobsOutput{
		ContinuationToken:     param.ContinuationToken,
		Prefixes:              prefixes,
		Items:                 items,
		NextContinuationToken: nextMarker,
		IsTruncated:           nextMarker != nil,
	}, nil
}

func (b *AZBlob) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	c, err := b.refreshToken()
	if err != nil {
		return nil, err
	}

	blob := c.NewBlobURL(param.Key)
	_, err = blob.Delete(context.TODO(), azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	if err != nil {
		return nil, mapAZBError(err)
	}
	return &DeleteBlobOutput{}, nil
}

func (b *AZBlob) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	for _, i := range param.Items {
		_, err := b.DeleteBlob(&DeleteBlobInput{i})
		if err != nil {
			err = mapAZBError(err)
			if err != fuse.ENOENT {
				return nil, err
			}
		}
	}
	return &DeleteBlobsOutput{}, nil
}

func (b *AZBlob) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *AZBlob) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	c, err := b.refreshToken()
	if err != nil {
		return nil, err
	}

	src := c.NewBlobURL(param.Source)
	dest := c.NewBlobURL(param.Destination)
	resp, err := dest.StartCopyFromURL(context.TODO(), src.URL(), nil, azblob.ModifiedAccessConditions{}, azblob.BlobAccessConditions{})
	if err != nil {
		return nil, mapAZBError(err)
	}

	if resp.CopyStatus() == azblob.CopyStatusPending {
		var copy *azblob.BlobGetPropertiesResponse
		for copy, err = dest.GetProperties(context.TODO(), azblob.BlobAccessConditions{}); err == nil; copy, err = dest.GetProperties(context.TODO(), azblob.BlobAccessConditions{}) {
			// if there's a new copy, we can only assume the last one was done
			if copy.CopyStatus() != azblob.CopyStatusPending || copy.CopyID() != resp.CopyID() {
				break
			}
		}
		if err != nil {
			return nil, mapAZBError(err)
		}
	}

	return &CopyBlobOutput{}, nil
}

func (b *AZBlob) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	c, err := b.refreshToken()
	if err != nil {
		return nil, err
	}

	blob := c.NewBlobURL(param.Key)
	var ifMatch azblob.ETag
	if param.IfMatch != nil {
		ifMatch = azblob.ETag(*param.IfMatch)
	}

	resp, err := blob.Download(context.TODO(),
		int64(param.Start), int64(param.Count),
		azblob.BlobAccessConditions{
			ModifiedAccessConditions: azblob.ModifiedAccessConditions{
				IfMatch: ifMatch,
			},
		}, false)
	if err != nil {
		return nil, mapAZBError(err)
	}

	return &GetBlobOutput{
		HeadBlobOutput: HeadBlobOutput{
			BlobItemOutput: BlobItemOutput{
				Key:          &param.Key,
				ETag:         PString(string(resp.ETag())),
				LastModified: PTime(resp.LastModified()),
				Size:         uint64(resp.ContentLength()),
			},
			ContentType: PString(resp.ContentType()),
			Metadata:    pMetadata(resp.NewMetadata()),
		},
		Body: resp.Body(azblob.RetryReaderOptions{}),
	}, nil
}

func (b *AZBlob) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	c, err := b.refreshToken()
	if err != nil {
		return nil, err
	}

	blob := c.NewBlobURL(param.Key).ToBlockBlobURL()
	resp, err := blob.Upload(context.TODO(),
		param.Body,
		azblob.BlobHTTPHeaders{
			ContentType: nilStr(param.ContentType),
		},
		nilMetadata(param.Metadata), azblob.BlobAccessConditions{})
	if err != nil {
		return nil, mapAZBError(err)
	}

	return &PutBlobOutput{
		ETag: PString(string(resp.ETag())),
	}, nil
}

func (b *AZBlob) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	// we can have up to 50K parts, so %05d should be sufficient
	uploadId := uuid.New().String() + "::%05d"

	// this is implicitly done on the server side
	return &MultipartBlobCommitInput{
		Key:      &param.Key,
		Metadata: param.Metadata,
		UploadId: &uploadId,
		Parts:    make([]*string, 50000), // at most 50K parts
	}, nil
}

func (b *AZBlob) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	c, err := b.refreshToken()
	if err != nil {
		return nil, err
	}

	blob := c.NewBlockBlobURL(*param.Commit.Key)
	blockId := fmt.Sprintf(*param.Commit.UploadId, param.PartNumber)
	base64BlockId := base64.StdEncoding.EncodeToString([]byte(blockId))

	atomic.AddUint32(&param.Commit.NumParts, 1)

	_, err = blob.StageBlock(context.TODO(), base64BlockId, param.Body,
		azblob.LeaseAccessConditions{}, nil)
	if err != nil {
		return nil, mapAZBError(err)
	}

	param.Commit.Parts[param.PartNumber-1] = &base64BlockId

	return &MultipartBlobAddOutput{}, nil
}

func (b *AZBlob) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	// no-op, server will garbage collect them
	return &MultipartBlobAbortOutput{}, nil
}

func (b *AZBlob) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	c, err := b.refreshToken()
	if err != nil {
		return nil, err
	}

	blob := c.NewBlockBlobURL(*param.Key)
	parts := make([]string, param.NumParts)

	for i := uint32(0); i < param.NumParts; i++ {
		parts[i] = *param.Parts[i]
	}

	resp, err := blob.CommitBlockList(context.TODO(), parts,
		azblob.BlobHTTPHeaders{}, nilMetadata(param.Metadata),
		azblob.BlobAccessConditions{})
	if err != nil {
		return nil, mapAZBError(err)
	}

	return &MultipartBlobCommitOutput{
		ETag: PString(string(resp.ETag())),
	}, nil
}

func (b *AZBlob) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *AZBlob) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error) {
	c, err := b.refreshToken()
	if err != nil {
		return nil, err
	}

	_, err = c.Delete(context.TODO(), azblob.ContainerAccessConditions{})
	if err != nil {
		return nil, mapAZBError(err)
	}
	return &RemoveBucketOutput{}, nil
}

func (b *AZBlob) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error) {
	c, err := b.refreshToken()
	if err != nil {
		return nil, err
	}

	_, err = c.Create(context.TODO(), nil, azblob.PublicAccessNone)
	if err != nil {
		return nil, mapAZBError(err)
	}
	return &MakeBucketOutput{}, nil
}
