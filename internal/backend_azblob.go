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
	. "github.com/kahing/goofys/api/common"

	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"net/http"
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

const AzuriteEndpoint = "http://127.0.0.1:8080/devstoreaccount1/"
const AzureDirBlobMetadataKey = "hdi_isfolder"
const AzureBlobMetaDataHeaderPrefix = "x-ms-meta-"

// Azure Blob Store API does not not treat headers as case insensitive.
// This is particularly a problem with `AzureDirBlobMetadataKey` header.
// pipelineWrapper wraps around an implementation of `Pipeline` and
// changes the Do function to update the input request headers before invoking
// Do on the wrapping Pipeline onject.
type pipelineWrapper struct {
	p pipeline.Pipeline
}

type requestWrapper struct {
	pipeline.Request
}

var pipelineHTTPClient = newDefaultHTTPClient()

// Clone of https://github.com/Azure/azure-pipeline-go/blob/master/pipeline/core.go#L202
func newDefaultHTTPClient() *http.Client {
	return &http.Client{
		Transport: GetHTTPTransport(),
	}
}

// Creates a pipeline.Factory object that fixes headers related to azure blob store
// and sends HTTP requests to Go's default http.Client.
func newAzBlobHTTPClientFactory() pipeline.Factory {
	return pipeline.FactoryFunc(
		func(next pipeline.Policy, po *pipeline.PolicyOptions) pipeline.PolicyFunc {
			return func(ctx context.Context, request pipeline.Request) (pipeline.Response, error) {
				// Fix the Azure Blob store metadata headers.
				// Problem:
				// - Golang canonicalizes headers and converts them into camel case
				//   because HTTP headers are supposed to be case insensitive. E.g After
				//   canonicalization, 'foo-bar' becomes 'Foo-Bar'.
				// - Azure API treats HTTP headers in case sensitive manner.
				// Solution: Convert the problematic headers to lower case.
				for key, value := range request.Header {
					keyLower := strings.ToLower(key)
					// We are mofifying the map while iterating on it. So we check for
					// keyLower != key to avoid potential infinite loop.
					// See https://golang.org/ref/spec#RangeClause for more info.
					if keyLower != key && strings.Contains(keyLower, AzureBlobMetaDataHeaderPrefix) {
						request.Header.Del(key)
						request.Header[keyLower] = value
					}
				}
				// Send the HTTP request.
				r, err := pipelineHTTPClient.Do(request.WithContext(ctx))
				if err != nil {
					err = pipeline.NewError(err, "HTTP request failed")
				}
				return pipeline.NewHTTPResponse(r), err
			}
		})
}

type AZBlob struct {
	config *AZBlobConfig
	cap    Capabilities

	mu sync.Mutex
	u  *azblob.ServiceURL
	c  *azblob.ContainerURL

	pipeline pipeline.Pipeline

	bucket           string
	bareURL          string
	sasTokenProvider SASTokenProvider
	tokenExpire      time.Time
	tokenRenewBuffer time.Duration
	tokenRenewGate   *Ticket
}

var azbLog = GetLogger("azblob")

func NewAZBlob(container string, config *AZBlobConfig) (*AZBlob, error) {
	po := azblob.PipelineOptions{
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
				azbLog.Log(logrus.Level(uint32(level)), msg)
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
				return azbLog.IsLevelEnabled(logrus.Level(uint32(level)))
			},
		},
		RequestLog: azblob.RequestLogOptions{
			LogWarningIfTryOverThreshold: time.Duration(-1),
		},
		HTTPSender: newAzBlobHTTPClientFactory(),
	}

	p := azblob.NewPipeline(azblob.NewAnonymousCredential(), po)
	bareURL := config.Endpoint

	var bu *azblob.ServiceURL
	var bc *azblob.ContainerURL

	if config.SasToken == nil {
		credential, err := azblob.NewSharedKeyCredential(config.AccountName, config.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("Unable to construct credential: %v", err)
		}

		p = azblob.NewPipeline(credential, po)

		u, err := url.Parse(bareURL)
		if err != nil {
			return nil, err
		}

		serviceURL := azblob.NewServiceURL(*u, p)
		containerURL := serviceURL.NewContainerURL(container)

		bu = &serviceURL
		bc = &containerURL
	}

	b := &AZBlob{
		config: config,
		cap: Capabilities{
			MaxMultipartSize: 100 * 1024 * 1024,
			Name:             "wasb",
		},
		pipeline:         p,
		bucket:           container,
		bareURL:          bareURL,
		sasTokenProvider: config.SasToken,
		u:                bu,
		c:                bc,
		tokenRenewBuffer: config.TokenRenewBuffer,
		tokenRenewGate:   Ticket{Total: 1}.Init(),
	}

	return b, nil
}

func (b *AZBlob) Delegate() interface{} {
	return b
}

func (b *AZBlob) Capabilities() *Capabilities {
	return &b.cap
}

func (b *AZBlob) Bucket() string {
	return b.bucket
}

func (b *AZBlob) refreshToken() (*azblob.ContainerURL, error) {
	if b.sasTokenProvider == nil {
		return b.c, nil
	}

	b.mu.Lock()

	if b.c == nil {
		b.mu.Unlock()
		return b.updateToken()
	} else if b.tokenExpire.Before(time.Now().UTC()) {
		// our token totally expired, renew inline before using it
		b.mu.Unlock()
		b.tokenRenewGate.Take(1, true)
		defer b.tokenRenewGate.Return(1)

		b.mu.Lock()
		// check again, because in the mean time maybe it's renewed
		if b.tokenExpire.Before(time.Now().UTC()) {
			b.mu.Unlock()
			azbLog.Warnf("token expired: %v", b.tokenExpire)
			_, err := b.updateToken()
			if err != nil {
				azbLog.Errorf("Unable to refresh token: %v", err)
				return nil, syscall.EACCES
			}
		} else {
			// another concurrent goroutine renewed it for us
			b.mu.Unlock()
		}
	} else if b.tokenExpire.Add(b.tokenRenewBuffer).Before(time.Now().UTC()) {
		b.mu.Unlock()
		// only allow one token renew at a time
		if b.tokenRenewGate.Take(1, false) {

			go func() {
				defer b.tokenRenewGate.Return(1)
				_, err := b.updateToken()
				if err != nil {
					azbLog.Errorf("Unable to refresh token: %v", err)
				}
			}()

			// if we cannot renew token, treat it as a
			// transient failure because the token is
			// still valid for a while. When the grace
			// period is over we will get an error when we
			// actually access the blob store
		} else {
			// another goroutine is already renewing
			azbLog.Infof("token renewal already in progress")
		}
	} else {
		b.mu.Unlock()
	}
	return b.c, nil
}

func parseSasToken(token string) (expire time.Time) {
	expire = TIME_MAX

	parts, err := url.ParseQuery(token)
	if err != nil {
		return
	}

	se := parts.Get("se")
	if se == "" {
		azbLog.Error("token missing 'se' param")
		return
	}

	expire, err = time.Parse("2006-01-02T15:04:05Z", se)
	if err != nil {
		// sometimes they only have the date
		expire, err = time.Parse("2006-01-02", se)
		if err != nil {
			expire = TIME_MAX
		}
	}
	return
}

func (b *AZBlob) updateToken() (*azblob.ContainerURL, error) {
	token, err := b.sasTokenProvider()
	if err != nil {
		azbLog.Errorf("Unable to generate SAS token: %v", err)
		return nil, syscall.EACCES
	}

	expire := parseSasToken(token)
	azbLog.Infof("token for %v refreshed, next expire at %v", b.bucket, expire.String())

	sUrl := b.bareURL + "?" + token
	u, err := url.Parse(sUrl)
	if err != nil {
		azbLog.Errorf("Unable to construct service URL: %v", sUrl)
		return nil, fuse.EINVAL
	}

	serviceURL := azblob.NewServiceURL(*u, b.pipeline)
	containerURL := serviceURL.NewContainerURL(b.bucket)

	b.mu.Lock()
	defer b.mu.Unlock()

	b.u = &serviceURL
	b.c = &containerURL
	b.tokenExpire = expire

	return b.c, nil
}

func (b *AZBlob) testBucket(key string) (err error) {
	_, err = b.HeadBlob(&HeadBlobInput{Key: key})
	if err != nil {
		err = mapAZBError(err)
		if err == fuse.ENOENT {
			err = nil
		}
	}

	return
}

func (b *AZBlob) Init(key string) error {
	_, err := b.refreshToken()
	if err != nil {
		return err
	}

	err = b.testBucket(key)
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
			return syscall.ENODEV
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
			err = mapHttpError(stgErr.Response().StatusCode)
			if err != nil {
				return err
			} else {
				azbLog.Errorf("code=%v status=%v err=%v", stgErr.ServiceCode(), stgErr.Response().Status, stgErr)
				return stgErr
			}
		}
	} else {
		return err
	}
}

func pMetadata(m map[string]string) map[string]*string {
	metadata := make(map[string]*string)
	for k, _ := range m {
		k = strings.ToLower(k)
		v := m[k]
		metadata[k] = &v
	}
	return metadata
}

func nilMetadata(m map[string]*string) map[string]string {
	metadata := make(map[string]string)
	for k, v := range m {
		k = strings.ToLower(k)
		metadata[k] = NilStr(v)
	}
	return metadata
}

func (b *AZBlob) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	c, err := b.refreshToken()
	if err != nil {
		return nil, err
	}

	if strings.HasSuffix(param.Key, "/") {
		dirBlob, err := b.HeadBlob(&HeadBlobInput{Key: param.Key[:len(param.Key)-1]})
		if err == nil {
			if !dirBlob.IsDirBlob {
				// we requested for a dir suffix, but this isn't one
				err = fuse.ENOENT
			}
		}
		return dirBlob, err
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
	// don't expose this to user land
	delete(metadata, AzureDirBlobMetadataKey)

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

func nilUint32(v *uint32) uint32 {
	if v == nil {
		return 0
	} else {
		return *v
	}
}

func (b *AZBlob) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	// azure blob does not support startAfter
	if param.StartAfter != nil {
		return nil, syscall.ENOTSUP
	}

	c, err := b.refreshToken()
	if err != nil {
		return nil, err
	}

	prefixes := make([]BlobPrefixOutput, 0)
	items := make([]BlobItemOutput, 0)

	var blobItems []azblob.BlobItem
	var nextMarker *string

	options := azblob.ListBlobsSegmentOptions{
		Prefix:     NilStr(param.Prefix),
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
			NilStr(param.Delimiter),
			options)
		if err != nil {
			return nil, mapAZBError(err)
		}

		for i, _ := range resp.Segment.BlobPrefixes {
			p := resp.Segment.BlobPrefixes[i]
			prefixes = append(prefixes, BlobPrefixOutput{Prefix: &p.Name})
		}

		if b.config.Endpoint == AzuriteEndpoint &&
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

		if b.config.Endpoint == AzuriteEndpoint &&
			!sort.IsSorted(sortBlobItemOutput(items)) {
			sort.Sort(sortBlobItemOutput(items))
		}
	}

	if len(blobItems) == 1 && len(blobItems[0].Name) <= len(options.Prefix) && strings.HasSuffix(options.Prefix, "/") {
		// There is only 1 result and that one result does not have the desired prefix. This can
		// happen if we ask for ListBlobs under /some/path/ and the result is List(/some/path). This
		// means the prefix we are listing is a blob => So return empty response to indicate that
		// this prefix should not be treated a directory by goofys.
		// NOTE: This undesired behaviour happens only on azblob when hierarchial namespaces are
		// enabled.
		return &ListBlobsOutput{}, nil
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

	if strings.HasSuffix(options.Prefix, "/") {
		// because azure doesn't use dir/ blobs, dir/ would not show up
		// so we make another request to fill that in
		dirBlob, err := b.HeadBlob(&HeadBlobInput{options.Prefix})
		if err == nil {
			*dirBlob.Key += "/"
			items = append(items, dirBlob.BlobItemOutput)
			sortItems = true
		} else if err == fuse.ENOENT {
			err = nil
		} else {
			return nil, err
		}
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

	if strings.HasSuffix(param.Key, "/") {
		return b.DeleteBlob(&DeleteBlobInput{Key: param.Key[:len(param.Key)-1]})
	}

	blob := c.NewBlobURL(param.Key)
	_, err = blob.Delete(context.TODO(), azblob.DeleteSnapshotsOptionInclude, azblob.BlobAccessConditions{})
	if err != nil {
		return nil, mapAZBError(err)
	}
	return &DeleteBlobOutput{}, nil
}

func (b *AZBlob) DeleteBlobs(param *DeleteBlobsInput) (ret *DeleteBlobsOutput, deleteError error) {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		if deleteError != nil {
			ret = nil
		} else {
			ret = &DeleteBlobsOutput{}
		}
	}()

	for _, i := range param.Items {
		SmallActionsGate.Take(1, true)
		wg.Add(1)

		go func(key string) {
			defer func() {
				SmallActionsGate.Return(1)
				wg.Done()
			}()

			_, err := b.DeleteBlob(&DeleteBlobInput{key})
			if err != nil {
				err = mapAZBError(err)
				if err != fuse.ENOENT {
					deleteError = err
				}
			}
		}(i)

		if deleteError != nil {
			return
		}
	}

	return
}

func (b *AZBlob) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *AZBlob) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	if strings.HasSuffix(param.Source, "/") && strings.HasSuffix(param.Destination, "/") {
		param.Source = param.Source[:len(param.Source)-1]
		param.Destination = param.Destination[:len(param.Destination)-1]
		return b.CopyBlob(param)
	}

	c, err := b.refreshToken()
	if err != nil {
		return nil, err
	}

	src := c.NewBlobURL(param.Source)
	dest := c.NewBlobURL(param.Destination)
	resp, err := dest.StartCopyFromURL(context.TODO(), src.URL(), nilMetadata(param.Metadata),
		azblob.ModifiedAccessConditions{}, azblob.BlobAccessConditions{})
	if err != nil {
		return nil, mapAZBError(err)
	}

	if resp.CopyStatus() == azblob.CopyStatusPending {
		time.Sleep(50 * time.Millisecond)

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

	metadata := pMetadata(resp.NewMetadata())
	delete(metadata, AzureDirBlobMetadataKey)

	return &GetBlobOutput{
		HeadBlobOutput: HeadBlobOutput{
			BlobItemOutput: BlobItemOutput{
				Key:          &param.Key,
				ETag:         PString(string(resp.ETag())),
				LastModified: PTime(resp.LastModified()),
				Size:         uint64(resp.ContentLength()),
			},
			ContentType: PString(resp.ContentType()),
			Metadata:    metadata,
		},
		Body: resp.Body(azblob.RetryReaderOptions{}),
	}, nil
}

func (b *AZBlob) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	c, err := b.refreshToken()
	if err != nil {
		return nil, err
	}

	if param.DirBlob && strings.HasSuffix(param.Key, "/") {
		// turn this into an empty blob with "hdi_isfolder" metadata
		param.Key = param.Key[:len(param.Key)-1]
		if param.Metadata != nil {
			param.Metadata[AzureDirBlobMetadataKey] = PString("true")
		} else {
			param.Metadata = map[string]*string{
				AzureDirBlobMetadataKey: PString("true"),
			}
		}
		return b.PutBlob(param)
	}

	body := param.Body
	if body == nil {
		body = bytes.NewReader([]byte(""))
	}

	blob := c.NewBlobURL(param.Key).ToBlockBlobURL()
	resp, err := blob.Upload(context.TODO(),
		body,
		azblob.BlobHTTPHeaders{
			ContentType: NilStr(param.ContentType),
		},
		nilMetadata(param.Metadata), azblob.BlobAccessConditions{})
	if err != nil {
		return nil, mapAZBError(err)
	}

	return &PutBlobOutput{
		ETag:         PString(string(resp.ETag())),
		LastModified: PTime(resp.LastModified()),
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
		ETag:         PString(string(resp.ETag())),
		LastModified: PTime(resp.LastModified()),
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
