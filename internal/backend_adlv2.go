// Copyright 2019 Databricks
// Copyright (c) Microsoft and contributors for generated code from azure-sdk-for-go
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

	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/jacobsa/fuse"
	"github.com/sirupsen/logrus"

	adl2 "github.com/Azure/azure-sdk-for-go/services/storage/datalake/2018-11-09/storagedatalake"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
)

type ADLv2 struct {
	cap Capabilities

	flags  *FlagStorage
	config *ADLv2Config

	client adl2PathClient
	bucket string
}

const ADL2_CLIENT_REQUEST_ID = "X-Ms-Client-Request-Id"
const ADL2_REQUEST_ID = "X-Ms-Request-Id"

var adl2Log = GetLogger("adlv2")

type ADLv2MultipartBlobCommitInput struct {
	Size           uint64
	ContentType    string
	RenewLeaseStop chan bool
}

func IsADLv2Endpoint(endpoint string) bool {
	return strings.HasPrefix(endpoint, "abfs://")
}

func adl2LogResp(level logrus.Level, r *http.Response) {
	if r == nil {
		return
	}

	if adl2Log.IsLevelEnabled(level) {
		requestId := r.Request.Header.Get(ADL2_CLIENT_REQUEST_ID)
		respId := r.Header.Get(ADL2_REQUEST_ID)
		// don't log anything if this is being called twice,
		// which it is via ResponseInspector
		if respId != "" {
			adl2Log.Logf(level, "%v %v %v %v %v", r.Request.Method,
				r.Request.URL.String(),
				requestId, r.Status, respId)
			r.Header.Del(ADL2_REQUEST_ID)
		}
	}
}

func NewADLv2(bucket string, flags *FlagStorage, config *ADLv2Config) (*ADLv2, error) {
	u, err := url.Parse(config.Endpoint)
	if err != nil {
		return nil, err
	}

	parts := strings.SplitN(u.Hostname(), ".", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("Invalid endpoint: %v", config.Endpoint)
	}
	storageAccountName := parts[0]
	dnsSuffix := parts[1]

	LogRequest := func(p autorest.Preparer) autorest.Preparer {
		return autorest.PreparerFunc(func(r *http.Request) (*http.Request, error) {
			r.URL.Scheme = u.Scheme
			date := time.Now().Format(time.RFC1123)
			date = strings.Replace(date, "UTC", "GMT", 1)

			r.Header.Set("X-Ms-Date", date)
			r.Header.Set("X-Ms-Version", "2018-11-09")
			r.Header.Set(ADL2_CLIENT_REQUEST_ID, uuid.New().String())
			r.Header.Set("Accept-Charset", "utf-8")
			r.Header.Set("Content-Type", "")
			r.Header.Set("Accept", "application/json, application/octet-stream")
			// set transfer encoding and non-nil Body to
			// ensure Content-Length: 0 is sent, seems
			// like an idiotic golang behavior:
			// https://github.com/golang/go/issues/20257
			// azure server side rejects the request if
			// Content-Length is missing
			r.TransferEncoding = []string{"identity"}
			if r.Header.Get("Content-Length") == "0" {
				r.Body = http.NoBody
			} else if r.Body == nil {
				r.Body = http.NoBody
			}

			if adl2Log.IsLevelEnabled(logrus.DebugLevel) {
				requestId := r.Header.Get(ADL2_CLIENT_REQUEST_ID)
				op := r.Method
				switch op {
				case http.MethodPost:
					// this is a lease
					leaseAction := r.Header.Get("X-Ms-Lease-Action")
					leaseId := r.Header.Get("X-Ms-Lease-Id")
					proposeLeaseId := r.Header.Get("X-Ms-Proposed-Lease-Id")
					op += fmt.Sprintf(" %v (%v, %v)",
						leaseAction, leaseId, proposeLeaseId)
				case http.MethodPatch:
					action := r.URL.Query().Get("action")
					op += " " + action
					if action == "append" {
						op += fmt.Sprintf("(%v)", r.ContentLength)
					}
				}
				adl2Log.Debugf("%v %v %v", op,
					r.URL.String(), requestId)
			}

			r, err := p.Prepare(r)
			if err != nil {
				adl2Log.Error(err)
			}
			return r, err
		})
	}

	LogResponse := func(p autorest.Responder) autorest.Responder {
		return autorest.ResponderFunc(func(r *http.Response) error {
			adl2LogResp(logrus.DebugLevel, r)
			err := p.Respond(r)
			if err != nil {
				adl2Log.Error(err)
			}
			return err
		})
	}

	client := adl2.NewWithoutDefaults("", storageAccountName, dnsSuffix)
	client.Authorizer = config.Authorizer
	client.RequestInspector = LogRequest
	client.ResponseInspector = LogResponse
	client.Sender.(*http.Client).Transport = GetHTTPTransport()

	b := &ADLv2{
		flags:  flags,
		config: config,
		client: adl2PathClient{client},
		bucket: bucket,
		cap: Capabilities{
			DirBlob: true,
			Name:    "adl2",
			// tested on 2019-11-07, seems to have same
			// limit as azblob
			MaxMultipartSize: 100 * 1024 * 1024,
		},
	}

	return b, nil
}

func (b *ADLv2) Bucket() string {
	return b.bucket
}

func (b *ADLv2) Delegate() interface{} {
	return b
}

func (b *ADLv2) Init(key string) (err error) {
	_, err = b.HeadBlob(&HeadBlobInput{Key: key})
	if err == fuse.ENOENT {
		err = nil
	}
	return
}

func (b *ADLv2) Capabilities() *Capabilities {
	return &b.cap
}

type ADL2Error struct {
	adl2.DataLakeStorageError
}

func (e ADL2Error) Error() string {
	return fmt.Sprintf("%v: %v", *e.DataLakeStorageError.Error.Code,
		*e.DataLakeStorageError.Error.Message)
}

func decodeADLv2Error(body io.Reader) (adlErr adl2.DataLakeStorageError, err error) {
	decoder := json.NewDecoder(body)
	err = decoder.Decode(&adlErr)
	return
}

func adlv2ErrLogHeaders(errCode string, resp *http.Response) {
	switch errCode {
	case "MissingRequiredHeader", "UnsupportedHeader":
		var s strings.Builder
		for k, _ := range resp.Request.Header {
			s.WriteString(k)
			s.WriteString(" ")
		}
		adl2Log.Errorf("%v, sent: %v", errCode, s.String())
	case "InvalidHeaderValue":
		var s strings.Builder
		for k, v := range resp.Request.Header {
			if k != "Authorization" {
				s.WriteString(k)
				s.WriteString(":")
				s.WriteString(v[0])
				s.WriteString(" ")
			}
		}
		adl2Log.Errorf("%v, sent: %v", errCode, s.String())
	case "InvalidSourceUri":
		adl2Log.Errorf("SourceUri: %v",
			resp.Request.Header.Get("X-Ms-Rename-Source"))
	}
}

func mapADLv2Error(resp *http.Response, err error, rawError bool) error {

	if resp == nil {
		if err != nil {
			if detailedError, ok := err.(autorest.DetailedError); ok {
				if urlErr, ok := detailedError.Original.(*url.Error); ok {
					adl2Log.Errorf("url.Err: %T: %v %v %v %v %v", urlErr.Err, urlErr.Err, urlErr.Temporary(), urlErr.Timeout(), urlErr.Op, urlErr.URL)
				} else {
					adl2Log.Errorf("%T: %v", detailedError.Original, detailedError.Original)
				}
			} else {
				adl2Log.Errorf("unknown error: %v", err)
			}
			return syscall.EAGAIN
		} else {
			return err
		}
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		defer resp.Body.Close()
		if rawError {
			adlErr, err := decodeADLv2Error(resp.Body)
			if err == nil {
				return ADL2Error{adlErr}
			} else {
				adl2Log.Errorf("cannot parse error: %v", err)
				return syscall.EAGAIN
			}
		} else {
			switch resp.StatusCode {
			case http.StatusBadRequest:
				if !adl2Log.IsLevelEnabled(logrus.DebugLevel) {
					adl2LogResp(logrus.ErrorLevel, resp)
				}
				adlErr, err := decodeADLv2Error(resp.Body)
				if err == nil {
					adlv2ErrLogHeaders(*adlErr.Error.Code, resp)
				}
			case http.StatusPreconditionFailed:
				return syscall.EAGAIN
			}

			err = mapHttpError(resp.StatusCode)
			if err != nil {
				return err
			} else {
				if !adl2Log.IsLevelEnabled(logrus.DebugLevel) {
					adl2LogResp(logrus.ErrorLevel, resp)
				}
				adl2Log.Errorf("resp: %#v %v", resp, err)
				return syscall.EINVAL
			}
		}
	} else if resp.StatusCode == http.StatusOK && err != nil {
		// trying to capture this error:
		// autorest.DetailedError{Original:(*errors.errorString)(0xc0003eb3f0),
		// PackageType:"storagedatalake.adl2PathClient",
		// Method:"List", StatusCode:200, Message:"Failure
		// responding to request", ServiceError:[]uint8(nil),
		// Response:(*http.Response)(0xc0016517a0)}
		// ("storagedatalake.adl2PathClient#List: Failure
		// responding to request: StatusCode=200 -- Original
		// Error: Error occurred reading http.Response#Body -
		// Error = 'read tcp
		// 10.20.255.49:34194->52.239.155.98:443: read:
		// connection reset by peer'")
		if detailedErr, ok := err.(autorest.DetailedError); ok {
			if detailedErr.Method == "List" &&
				strings.Contains(detailedErr.Error(),
					"read: connection reset by peer") {
				return syscall.ECONNRESET
			}
		}
	}

	return err
}

func getHeader(resp *http.Response, key string) *string {
	if v, set := resp.Header[http.CanonicalHeaderKey(key)]; set {
		return &v[0]
	} else {
		return nil
	}
}

func parseADLv2Time(v string) *time.Time {
	t, err := time.Parse(time.RFC1123, v)
	if err == nil {
		return &t
	} else {
		return nil
	}
}

func adlv2ToBlobItem(resp *http.Response, key string) BlobItemOutput {
	return BlobItemOutput{
		Key:          &key,
		ETag:         getHeader(resp, "ETag"),
		Size:         uint64(resp.ContentLength),
		LastModified: parseADLv2Time(resp.Header.Get("Last-Modified")),
	}
}

func (b *ADLv2) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	key := param.Key
	if strings.HasSuffix(key, "/") {
		key = key[:len(key)-1]
	}

	// GetProperties(GetStatus) does not return user defined
	// properties, despite what the documentation says, use a 0
	// bytes range get instead
	res, err := b.GetBlob(&GetBlobInput{
		Key:   key,
		Start: 0,
		Count: 0,
	})
	if err != nil {
		return nil, err
	}
	res.Body.Close()

	return &res.HeadBlobOutput, nil
}

// autorest handles retry based on request errors but doesn't retry on
// reading body. List is idempotent anyway so we can retry it here
func (b *ADLv2) listBlobs(param *ListBlobsInput, maxResults *int32) (adl2PathList, error) {
	var err error
	var res adl2PathList

	// autorest's DefaultMaxRetry is 3 which seems wrong. Also
	// read errors are transient and should probably be retried more
	for attempt := 0; attempt < 30; attempt++ {
		res, err = b.client.List(context.TODO(), param.Delimiter == nil, b.bucket,
			NilStr(param.Prefix), NilStr(param.ContinuationToken), maxResults,
			nil, "", nil, "")
		err = mapADLv2Error(res.Response.Response, err, false)
		if err == nil {
			break
		} else if err != syscall.ECONNRESET {
			return res, err
		} else {
			// autorest's DefaultRetryDuration is 30s but
			// that's for failed requests. Read errors is
			// probably more transient and should be
			// retried faster
			if !autorest.DelayForBackoffWithCap(
				30*time.Millisecond,
				0,
				attempt,
				res.Response.Response.Request.Context().Done()) {
				return res, err
			}
		}

	}

	return res, err
}

func (b *ADLv2) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	if param.Delimiter != nil && *param.Delimiter != "/" {
		return nil, fuse.EINVAL
	}

	var maxResults *int32
	if param.MaxKeys != nil {
		maxResults = PInt32(int32(*param.MaxKeys))
	}

	res, err := b.listBlobs(param, maxResults)
	if err != nil {
		if err == fuse.ENOENT {
			return &ListBlobsOutput{
				RequestId: res.Response.Response.Header.Get(ADL2_REQUEST_ID),
			}, nil
		} else {
			return nil, err
		}
	}

	var prefixes []BlobPrefixOutput
	var items []BlobItemOutput

	if param.Delimiter != nil && param.Prefix != nil {
		// we didn't get 404 which means the path must
		// exists. If the path is actually a file, adlv2
		// returns the file itself as the result. That's
		// already handled by the loop below
		if len(*res.Paths) != 1 ||
			*(*res.Paths)[0].Name != strings.TrimRight(*param.Prefix, "/") {
			// the prefix we listed is a directory
			if strings.HasSuffix(*param.Prefix, "/") {
				// we listed for the dir object itself
				items = append(items, BlobItemOutput{
					Key: param.Prefix,
				})
			} else {
				prefixes = append(prefixes, BlobPrefixOutput{
					PString(*param.Prefix + "/"),
				})
			}
		} else {
			if strings.HasSuffix(*param.Prefix, "/") {
				// we asked for a dir and got a file
				return &ListBlobsOutput{
					RequestId: res.Response.Response.Header.Get(ADL2_REQUEST_ID),
				}, nil
			}
		}
	}

	for _, p := range *res.Paths {
		if param.Delimiter != nil {
			if p.isDirectory() {
				prefixes = append(prefixes, BlobPrefixOutput{
					PString(*p.Name + "/"),
				})
				continue
			}
		}

		key := *p.Name
		if p.isDirectory() {
			key += "/"
		}
		items = append(items, BlobItemOutput{
			Key:          &key,
			ETag:         p.ETag,
			LastModified: parseADLv2Time(NilStr(p.LastModified)),
			Size:         uint64(p.contentLength()),
		})
	}

	continuationToken := getHeader(res.Response.Response, "x-ms-continuation")

	return &ListBlobsOutput{
		Prefixes:              prefixes,
		Items:                 items,
		NextContinuationToken: continuationToken,
		IsTruncated:           continuationToken != nil,
		RequestId:             res.Response.Response.Header.Get(ADL2_REQUEST_ID),
	}, nil
}

func (b *ADLv2) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	if strings.HasSuffix(param.Key, "/") {
		return b.DeleteBlob(&DeleteBlobInput{param.Key[:len(param.Key)-1]})
	}

	res, err := b.client.Delete(context.TODO(), b.bucket, param.Key, nil, "", "",
		/*ifMatch=*/ "", "", "", "", "", nil, "")
	err = mapADLv2Error(res.Response, err, false)
	if err != nil {
		return nil, err
	}
	return &DeleteBlobOutput{}, nil
}

func (b *ADLv2) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *ADLv2) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	var continuation string

	renameDest := param.Destination
	if strings.HasSuffix(renameDest, "/") {
		renameDest = renameDest[:len(renameDest)-1]
	}
	renameSource := param.Source
	if strings.HasSuffix(renameSource, "/") {
		renameSource = renameSource[:len(renameSource)-1]
	}
	renameSource = "/" + b.bucket + "/" + url.PathEscape(renameSource)

	var requestId string
	for cont := true; cont; cont = continuation != "" {
		res, err := b.client.Create(context.TODO(), b.bucket, renameDest,
			"", continuation, "", "", "", "", "", "", "", "", "", "",
			renameSource, "", "", "", "", "", "", "", "", "", "", "",
			"", "", "", nil, "")
		if err != nil {
			return nil, mapADLv2Error(res.Response, err, false)
		}

		continuation = res.Header.Get("x-ms-continuation")
		requestId = res.Header.Get(ADL2_REQUEST_ID)
	}

	return &RenameBlobOutput{requestId}, nil
}

func (b *ADLv2) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	if param.Source != param.Destination || param.Metadata == nil {
		return nil, syscall.ENOTSUP
	}

	res, err := b.client.Update(context.TODO(), adl2.SetProperties, b.bucket, param.Source, nil,
		nil, nil, nil, "", "", "", "", "", "", "", "", b.toADLProperties(param.Metadata),
		"", "", "", "", "", "", "", "", nil, "", nil, "")
	if err != nil {
		return nil, mapADLv2Error(res.Response, err, false)
	}

	return &CopyBlobOutput{
		RequestId: res.Response.Header.Get(ADL2_REQUEST_ID),
	}, nil
}

func (b *ADLv2) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	var bytes string
	if param.Start != 0 || param.Count != 0 {
		if param.Count != 0 {
			bytes = fmt.Sprintf("bytes=%v-%v", param.Start, param.Start+param.Count-1)
		} else {
			bytes = fmt.Sprintf("bytes=%v-", param.Start)
		}
	}

	res, err := b.client.Read(context.TODO(), b.bucket, param.Key, bytes,
		"", nil, NilStr(param.IfMatch), "", "", "",
		"", nil, "")
	if err != nil {
		return nil, mapADLv2Error(res.Response.Response, err, false)
	}

	metadata := make(map[string]*string)
	for _, p := range res.Header["X-Ms-Properties"] {
		csv := strings.Split(p, ",")
		for _, kv := range csv {
			kv = strings.TrimSpace(kv)
			if len(kv) == 0 {
				continue
			}

			s := strings.SplitN(kv, "=", 2)
			if len(s) != 2 {
				adl2Log.Warnf("Dropping property: %v: %v", param.Key, kv)
				continue
			}
			key := strings.TrimSpace(s[0])
			value := strings.TrimSpace(s[1])
			buf, err := base64.StdEncoding.DecodeString(value)
			if err != nil {
				adl2Log.Warnf("Unable to decode property: %v: %v",
					param.Key, key)
				continue
			}
			metadata[key] = PString(string(buf))
		}
	}

	return &GetBlobOutput{
		HeadBlobOutput: HeadBlobOutput{
			BlobItemOutput: adlv2ToBlobItem(res.Response.Response, param.Key),
			ContentType:    getHeader(res.Response.Response, "Content-Type"),
			IsDirBlob:      res.Header.Get("X-Ms-Resource-Type") == string(adl2.Directory),
			Metadata:       metadata,
		},
		Body: *res.Value,
	}, nil
}

func (b *ADLv2) toADLProperties(metadata map[string]*string) string {
	var buf strings.Builder
	for k, v := range metadata {
		buf.WriteString(k)
		buf.WriteString("=")
		buf.WriteString(base64.StdEncoding.EncodeToString([]byte(*v)))
		buf.WriteString(",")
	}
	var s = buf.String()
	if len(s) != 0 {
		// remove trailing comma
		s = s[:len(s)-1]
	}
	return s
}

func (b *ADLv2) create(key string, pathType adl2.PathResourceType, contentType *string,
	metadata map[string]*string, leaseId string) (resp autorest.Response, err error) {
	resp, err = b.client.Create(context.TODO(), b.bucket, key,
		pathType, "", "", "", "", "", "", "", NilStr(contentType),
		"", "", "", "", leaseId, "", b.toADLProperties(metadata), "", "", "", "", "", "",
		"", "", "", "", "", nil, "")
	if err != nil {
		err = mapADLv2Error(resp.Response, err, false)
	}
	return
}

func (b *ADLv2) append(key string, offset int64, size int64, body io.ReadSeeker,
	leaseId string) (resp autorest.Response, err error) {
	resp, err = b.client.Update(context.TODO(), adl2.Append, b.bucket,
		key, &offset, nil, nil, &size, "", leaseId, "",
		"", "", "", "", "", "", "", "", "", "",
		"", "", "", "", &ReadSeekerCloser{body},
		"", nil, "")
	if err != nil {
		err = mapADLv2Error(resp.Response, err, false)
	}
	return
}

func (b *ADLv2) flush(key string, offset int64, contentType string, leaseId string) (res autorest.Response, err error) {
	res, err = b.client.Update(context.TODO(), adl2.Flush, b.bucket,
		key, &offset, PBool(false), PBool(true), PInt64(0), "", leaseId, "",
		contentType, "", "", "", "", "", "", "", "", "",
		"", "", "", "", nil, "", nil, "")
	if err != nil {
		err = mapADLv2Error(res.Response, err, false)
	}
	return
}

func (b *ADLv2) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	if param.DirBlob {
		res, err := b.create(param.Key, adl2.Directory, param.ContentType,
			param.Metadata, "")
		if err != nil {
			return nil, err
		}
		return &PutBlobOutput{
			ETag:         getHeader(res.Response, "ETag"),
			LastModified: parseADLv2Time(res.Response.Header.Get("Last-Modified")),
		}, nil
	} else {
		if param.Size == nil {
			panic("size cannot be nil")
		}

		create, err := b.create(param.Key, adl2.File, param.ContentType,
			param.Metadata, "")
		if err != nil {
			return nil, err
		}

		size := int64(*param.Size)
		if size == 0 {
			// nothing to write, we can return
			// here. appending a 0-size buffer
			// causes azure to fail with 400 bad
			// request
			return &PutBlobOutput{
				ETag: getHeader(create.Response, "ETag"),
			}, nil
		}

		// not doing a lease for these because append to 0
		// would guarantee that we don't have concurrent
		// appends, and flushing is safe to do
		_, err = b.append(param.Key, 0, size, param.Body, "")
		if err != nil {
			return nil, err
		}

		flush, err := b.flush(param.Key, size, NilStr(param.ContentType), "")
		if err != nil {
			return nil, err
		}

		return &PutBlobOutput{
			ETag:         getHeader(flush.Response, "ETag"),
			LastModified: parseADLv2Time(flush.Response.Header.Get("Last-Modified")),
		}, nil
	}
}

// adlv2 doesn't have atomic multipart upload, instead we will hold a
// lease, replace the object, then release the lease
func (b *ADLv2) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	leaseId := uuid.New().String()
	err := b.lease(adl2.Acquire, param.Key, leaseId, 60, "")
	if err == fuse.ENOENT {
		// the file didn't exist, we will create the file
		// first and then acquire the lease
		create, err := b.create(param.Key, adl2.File, param.ContentType, param.Metadata, "")
		if err != nil {
			return nil, err
		}

		err = b.lease(adl2.Acquire, param.Key, leaseId, 60,
			create.Response.Header.Get("ETag"))
		if err != nil {
			return nil, err
		}
	} else {
		if err != nil {
			return nil, err
		}

		defer func() {
			if err != nil {
				err2 := b.lease(adl2.Release, param.Key, leaseId, 0, "")
				if err2 != nil {
					adl2Log.Errorf("Unable to release lease for %v: %v",
						param.Key, err2)
				}
			}
		}()

		_, err = b.create(param.Key, adl2.File, param.ContentType, param.Metadata, leaseId)
		if err != nil {
			return nil, err
		}

	}

	commitData := &ADLv2MultipartBlobCommitInput{
		ContentType:    NilStr(param.ContentType),
		RenewLeaseStop: make(chan bool, 1),
	}

	go func() {
		for {
			select {
			case <-commitData.RenewLeaseStop:
				break
			case <-time.After(30 * time.Second):
				b.lease(adl2.Renew, param.Key, leaseId, 60, "")
			}
		}
	}()

	return &MultipartBlobCommitInput{
		Key:         &param.Key,
		Metadata:    param.Metadata,
		UploadId:    &leaseId,
		backendData: commitData,
	}, nil
}

func (b *ADLv2) lease(action adl2.PathLeaseAction, key string, leaseId string, durationSec int32,
	ifMatch string) error {
	var proposeLeaseId string
	var prevLeaseId string
	if action == adl2.Acquire {
		proposeLeaseId = leaseId
	} else {
		prevLeaseId = leaseId
	}

	var duration *int32
	if durationSec != 0 {
		duration = &durationSec
	}

	res, err := b.client.Lease(context.TODO(), action, b.bucket, key,
		duration, nil, prevLeaseId, proposeLeaseId, ifMatch, "", "", "", "", nil, "")
	if err != nil {
		err = mapADLv2Error(res.Response, err, false)
	}
	return err
}

func (b *ADLv2) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	var commitData *ADLv2MultipartBlobCommitInput
	var ok bool
	if commitData, ok = param.Commit.backendData.(*ADLv2MultipartBlobCommitInput); !ok {
		panic("Incorrect commit data type")
	}

	res, err := b.append(*param.Commit.Key, int64(param.Offset), int64(param.Size),
		param.Body, *param.Commit.UploadId)
	if err != nil {
		return nil, err
	}
	atomic.AddUint64(&commitData.Size, param.Size)

	return &MultipartBlobAddOutput{
		res.Response.Header.Get(ADL2_REQUEST_ID),
	}, nil
}

func (b *ADLv2) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	if param.UploadId != nil {
		err := b.lease(adl2.Release, *param.Key, *param.UploadId, 0, "")
		if err != nil {
			return nil, err
		}
	}
	return &MultipartBlobAbortOutput{}, nil
}

func (b *ADLv2) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	var commitData *ADLv2MultipartBlobCommitInput
	var ok bool
	if commitData, ok = param.backendData.(*ADLv2MultipartBlobCommitInput); !ok {
		panic("Incorrect commit data type")
	}

	defer func() {
		commitData.RenewLeaseStop <- true
		leaseId := *param.UploadId
		// if the commit failed, we don't need to release the
		// lease during abort
		param.UploadId = nil

		err2 := b.lease(adl2.Release, *param.Key, leaseId, 0, "")
		if err2 != nil {
			adl2Log.Errorf("Unable to release lease for %v: %v",
				*param.Key, err2)
		}
	}()

	flush, err := b.flush(*param.Key, int64(commitData.Size), commitData.ContentType, *param.UploadId)
	if err != nil {
		return nil, err
	}

	return &MultipartBlobCommitOutput{
		LastModified: parseADLv2Time(flush.Response.Header.Get("Last-Modified")),
		ETag:         getHeader(flush.Response, "ETag"),
		RequestId:    flush.Response.Header.Get(ADL2_REQUEST_ID),
	}, nil
}

func (b *ADLv2) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *ADLv2) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error) {
	fs := adl2.FilesystemClient{b.client.BaseClient}
	res, err := fs.Delete(context.TODO(), b.bucket, "", "", uuid.New().String(), nil, "")
	if err != nil {
		return nil, mapADLv2Error(res.Response, err, false)
	}
	return &RemoveBucketOutput{}, nil
}

func (b *ADLv2) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error) {
	fs := adl2.FilesystemClient{b.client.BaseClient}
	res, err := fs.Create(context.TODO(), b.bucket, "", uuid.New().String(), nil, "")
	if err != nil {
		return nil, mapADLv2Error(res.Response, err, false)
	}
	return &MakeBucketOutput{}, nil
}

// hacked from azure-sdk-for-go
// remove after these bugs are fixed:
// https://github.com/Azure/azure-sdk-for-go/issues/5502
// https://github.com/Azure/azure-sdk-for-go/issues/5550
// https://github.com/Azure/azure-sdk-for-go/issues/5549
type adl2PathClient struct {
	adl2.BaseClient
}

func (client adl2PathClient) Create(ctx context.Context, filesystem string, pathParameter string, resource adl2.PathResourceType, continuation string, mode adl2.PathRenameMode, cacheControl string, contentEncoding string, contentLanguage string, contentDisposition string, xMsCacheControl string, xMsContentType string, xMsContentEncoding string, xMsContentLanguage string, xMsContentDisposition string, xMsRenameSource string, xMsLeaseID string, xMsSourceLeaseID string, xMsProperties string, xMsPermissions string, xMsUmask string, ifMatch string, ifNoneMatch string, ifModifiedSince string, ifUnmodifiedSince string, xMsSourceIfMatch string, xMsSourceIfNoneMatch string, xMsSourceIfModifiedSince string, xMsSourceIfUnmodifiedSince string, xMsClientRequestID string, timeout *int32, xMsDate string) (result autorest.Response, err error) {
	req, err := client.CreatePreparer(ctx, filesystem, pathParameter, resource, continuation, mode, cacheControl, contentEncoding, contentLanguage, contentDisposition, xMsCacheControl, xMsContentType, xMsContentEncoding, xMsContentLanguage, xMsContentDisposition, xMsRenameSource, xMsLeaseID, xMsSourceLeaseID, xMsProperties, xMsPermissions, xMsUmask, ifMatch, ifNoneMatch, ifModifiedSince, ifUnmodifiedSince, xMsSourceIfMatch, xMsSourceIfNoneMatch, xMsSourceIfModifiedSince, xMsSourceIfUnmodifiedSince, xMsClientRequestID, timeout, xMsDate)
	if err != nil {
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "Create", nil, "Failure preparing request")
		return
	}

	resp, err := client.CreateSender(req)
	if err != nil {
		result.Response = resp
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "Create", resp, "Failure sending request")
		return
	}

	result, err = client.CreateResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "Create", resp, "Failure responding to request")
	}

	return
}

// CreatePreparer prepares the Create request.
func (client adl2PathClient) CreatePreparer(ctx context.Context, filesystem string, pathParameter string, resource adl2.PathResourceType, continuation string, mode adl2.PathRenameMode, cacheControl string, contentEncoding string, contentLanguage string, contentDisposition string, xMsCacheControl string, xMsContentType string, xMsContentEncoding string, xMsContentLanguage string, xMsContentDisposition string, xMsRenameSource string, xMsLeaseID string, xMsSourceLeaseID string, xMsProperties string, xMsPermissions string, xMsUmask string, ifMatch string, ifNoneMatch string, ifModifiedSince string, ifUnmodifiedSince string, xMsSourceIfMatch string, xMsSourceIfNoneMatch string, xMsSourceIfModifiedSince string, xMsSourceIfUnmodifiedSince string, xMsClientRequestID string, timeout *int32, xMsDate string) (*http.Request, error) {
	urlParameters := map[string]interface{}{
		"accountName": client.AccountName,
		"dnsSuffix":   client.DNSSuffix,
	}

	pathParameters := map[string]interface{}{
		"filesystem": autorest.Encode("path", filesystem),
		//"path":       url.PathEscape(pathParameter),
		"path": autorest.Encode("path", pathParameter),
	}

	queryParameters := map[string]interface{}{}
	if len(string(resource)) > 0 {
		queryParameters["resource"] = autorest.Encode("query", resource)
	}
	if len(continuation) > 0 {
		queryParameters["continuation"] = autorest.Encode("query", continuation)
	}
	if len(string(mode)) > 0 {
		queryParameters["mode"] = autorest.Encode("query", mode)
	}
	if timeout != nil {
		queryParameters["timeout"] = autorest.Encode("query", *timeout)
	}

	preparer := autorest.CreatePreparer(
		autorest.AsPut(),
		autorest.WithCustomBaseURL("http://{accountName}.{dnsSuffix}", urlParameters),
		autorest.WithPathParameters("/{filesystem}/{path}", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	if len(cacheControl) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("Cache-Control", autorest.String(cacheControl)))
	}
	if len(contentEncoding) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("Content-Encoding", autorest.String(contentEncoding)))
	}
	if len(contentLanguage) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("Content-Language", autorest.String(contentLanguage)))
	}
	if len(contentDisposition) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("Content-Disposition", autorest.String(contentDisposition)))
	}
	if len(xMsCacheControl) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-cache-control", autorest.String(xMsCacheControl)))
	}
	if len(xMsContentType) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-content-type", autorest.String(xMsContentType)))
	}
	if len(xMsContentEncoding) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-content-encoding", autorest.String(xMsContentEncoding)))
	}
	if len(xMsContentLanguage) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-content-language", autorest.String(xMsContentLanguage)))
	}
	if len(xMsContentDisposition) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-content-disposition", autorest.String(xMsContentDisposition)))
	}
	if len(xMsRenameSource) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-rename-source", autorest.String(xMsRenameSource)))
	}
	if len(xMsLeaseID) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-lease-id", autorest.String(xMsLeaseID)))
	}
	if len(xMsSourceLeaseID) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-source-lease-id", autorest.String(xMsSourceLeaseID)))
	}
	if len(xMsProperties) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-properties", autorest.String(xMsProperties)))
	}
	if len(xMsPermissions) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-permissions", autorest.String(xMsPermissions)))
	}
	if len(xMsUmask) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-umask", autorest.String(xMsUmask)))
	}
	if len(ifMatch) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Match", autorest.String(ifMatch)))
	}
	if len(ifNoneMatch) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-None-Match", autorest.String(ifNoneMatch)))
	}
	if len(ifModifiedSince) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Modified-Since", autorest.String(ifModifiedSince)))
	}
	if len(ifUnmodifiedSince) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Unmodified-Since", autorest.String(ifUnmodifiedSince)))
	}
	if len(xMsSourceIfMatch) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-source-if-match", autorest.String(xMsSourceIfMatch)))
	}
	if len(xMsSourceIfNoneMatch) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-source-if-none-match", autorest.String(xMsSourceIfNoneMatch)))
	}
	if len(xMsSourceIfModifiedSince) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-source-if-modified-since", autorest.String(xMsSourceIfModifiedSince)))
	}
	if len(xMsSourceIfUnmodifiedSince) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-source-if-unmodified-since", autorest.String(xMsSourceIfUnmodifiedSince)))
	}
	if len(xMsClientRequestID) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-client-request-id", autorest.String(xMsClientRequestID)))
	}
	if len(xMsDate) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-date", autorest.String(xMsDate)))
	}
	if len(client.XMsVersion) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-version", autorest.String(client.XMsVersion)))
	}
	return preparer.Prepare((client.defaultRequest()).WithContext(ctx))
}

// CreateSender sends the Create request. The method will close the
// http.Response Body if it receives an error.
func (client adl2PathClient) CreateSender(req *http.Request) (*http.Response, error) {
	sd := autorest.GetSendDecorators(req.Context(), autorest.DoRetryForStatusCodes(client.RetryAttempts, client.RetryDuration, autorest.StatusCodesForRetry...))
	return autorest.SendWithSender(client, req, sd...)
}

// CreateResponder handles the response to the Create request. The method always
// closes the http.Response Body.
func (client adl2PathClient) CreateResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusCreated),
		autorest.ByClosing())
	result.Response = resp
	return
}

func (client adl2PathClient) Delete(ctx context.Context, filesystem string, pathParameter string, recursive *bool, continuation string, xMsLeaseID string, ifMatch string, ifNoneMatch string, ifModifiedSince string, ifUnmodifiedSince string, xMsClientRequestID string, timeout *int32, xMsDate string) (result autorest.Response, err error) {
	req, err := client.DeletePreparer(ctx, filesystem, pathParameter, recursive, continuation, xMsLeaseID, ifMatch, ifNoneMatch, ifModifiedSince, ifUnmodifiedSince, xMsClientRequestID, timeout, xMsDate)
	if err != nil {
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "Delete", nil, "Failure preparing request")
		return
	}

	resp, err := client.DeleteSender(req)
	if err != nil {
		result.Response = resp
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "Delete", resp, "Failure sending request")
		return
	}

	result, err = client.DeleteResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "Delete", resp, "Failure responding to request")
	}

	return
}

// DeletePreparer prepares the Delete request.
func (client adl2PathClient) DeletePreparer(ctx context.Context, filesystem string, pathParameter string, recursive *bool, continuation string, xMsLeaseID string, ifMatch string, ifNoneMatch string, ifModifiedSince string, ifUnmodifiedSince string, xMsClientRequestID string, timeout *int32, xMsDate string) (*http.Request, error) {
	urlParameters := map[string]interface{}{
		"accountName": client.AccountName,
		"dnsSuffix":   client.DNSSuffix,
	}

	pathParameters := map[string]interface{}{
		"filesystem": autorest.Encode("path", filesystem),
		"path":       autorest.Encode("path", pathParameter),
	}

	queryParameters := map[string]interface{}{}
	if recursive != nil {
		queryParameters["recursive"] = autorest.Encode("query", *recursive)
	}
	if len(continuation) > 0 {
		queryParameters["continuation"] = autorest.Encode("query", continuation)
	}
	if timeout != nil {
		queryParameters["timeout"] = autorest.Encode("query", *timeout)
	}

	preparer := autorest.CreatePreparer(
		autorest.AsDelete(),
		autorest.WithCustomBaseURL("http://{accountName}.{dnsSuffix}", urlParameters),
		autorest.WithPathParameters("/{filesystem}/{path}", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	if len(xMsLeaseID) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-lease-id", autorest.String(xMsLeaseID)))
	}
	if len(ifMatch) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Match", autorest.String(ifMatch)))
	}
	if len(ifNoneMatch) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-None-Match", autorest.String(ifNoneMatch)))
	}
	if len(ifModifiedSince) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Modified-Since", autorest.String(ifModifiedSince)))
	}
	if len(ifUnmodifiedSince) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Unmodified-Since", autorest.String(ifUnmodifiedSince)))
	}
	if len(xMsClientRequestID) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-client-request-id", autorest.String(xMsClientRequestID)))
	}
	if len(xMsDate) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-date", autorest.String(xMsDate)))
	}
	if len(client.XMsVersion) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-version", autorest.String(client.XMsVersion)))
	}
	return preparer.Prepare((client.defaultRequest()).WithContext(ctx))
}

// DeleteSender sends the Delete request. The method will close the
// http.Response Body if it receives an error.
func (client adl2PathClient) DeleteSender(req *http.Request) (*http.Response, error) {
	sd := autorest.GetSendDecorators(req.Context(), autorest.DoRetryForStatusCodes(client.RetryAttempts, client.RetryDuration, autorest.StatusCodesForRetry...))
	return autorest.SendWithSender(client, req, sd...)
}

// DeleteResponder handles the response to the Delete request. The method always
// closes the http.Response Body.
func (client adl2PathClient) DeleteResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByClosing())
	result.Response = resp
	return
}

func (client adl2PathClient) GetProperties(ctx context.Context, filesystem string, pathParameter string, action adl2.PathGetPropertiesAction, upn *bool, xMsLeaseID string, ifMatch string, ifNoneMatch string, ifModifiedSince string, ifUnmodifiedSince string, xMsClientRequestID string, timeout *int32, xMsDate string) (result autorest.Response, err error) {
	req, err := client.GetPropertiesPreparer(ctx, filesystem, pathParameter, action, upn, xMsLeaseID, ifMatch, ifNoneMatch, ifModifiedSince, ifUnmodifiedSince, xMsClientRequestID, timeout, xMsDate)
	if err != nil {
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "GetProperties", nil, "Failure preparing request")
		return
	}

	resp, err := client.GetPropertiesSender(req)
	if err != nil {
		result.Response = resp
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "GetProperties", resp, "Failure sending request")
		return
	}

	result, err = client.GetPropertiesResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "GetProperties", resp, "Failure responding to request")
	}

	return
}

// GetPropertiesPreparer prepares the GetProperties request.
func (client adl2PathClient) GetPropertiesPreparer(ctx context.Context, filesystem string, pathParameter string, action adl2.PathGetPropertiesAction, upn *bool, xMsLeaseID string, ifMatch string, ifNoneMatch string, ifModifiedSince string, ifUnmodifiedSince string, xMsClientRequestID string, timeout *int32, xMsDate string) (*http.Request, error) {
	urlParameters := map[string]interface{}{
		"accountName": client.AccountName,
		"dnsSuffix":   client.DNSSuffix,
	}

	pathParameters := map[string]interface{}{
		"filesystem": autorest.Encode("path", filesystem),
		"path":       autorest.Encode("path", pathParameter),
	}

	queryParameters := map[string]interface{}{}
	if len(string(action)) > 0 {
		queryParameters["action"] = autorest.Encode("query", action)
	}
	if upn != nil {
		queryParameters["upn"] = autorest.Encode("query", *upn)
	}
	if timeout != nil {
		queryParameters["timeout"] = autorest.Encode("query", *timeout)
	}

	preparer := autorest.CreatePreparer(
		autorest.AsHead(),
		autorest.WithCustomBaseURL("http://{accountName}.{dnsSuffix}", urlParameters),
		autorest.WithPathParameters("/{filesystem}/{path}", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	if len(xMsLeaseID) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-lease-id", autorest.String(xMsLeaseID)))
	}
	if len(ifMatch) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Match", autorest.String(ifMatch)))
	}
	if len(ifNoneMatch) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-None-Match", autorest.String(ifNoneMatch)))
	}
	if len(ifModifiedSince) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Modified-Since", autorest.String(ifModifiedSince)))
	}
	if len(ifUnmodifiedSince) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Unmodified-Since", autorest.String(ifUnmodifiedSince)))
	}
	if len(xMsClientRequestID) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-client-request-id", autorest.String(xMsClientRequestID)))
	}
	if len(xMsDate) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-date", autorest.String(xMsDate)))
	}
	if len(client.XMsVersion) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-version", autorest.String(client.XMsVersion)))
	}
	return preparer.Prepare((client.defaultRequest()).WithContext(ctx))
}

// GetPropertiesSender sends the GetProperties request. The method will close the
// http.Response Body if it receives an error.
func (client adl2PathClient) GetPropertiesSender(req *http.Request) (*http.Response, error) {
	sd := autorest.GetSendDecorators(req.Context(), autorest.DoRetryForStatusCodes(client.RetryAttempts, client.RetryDuration, autorest.StatusCodesForRetry...))
	return autorest.SendWithSender(client, req, sd...)
}

// GetPropertiesResponder handles the response to the GetProperties request. The method always
// closes the http.Response Body.
func (client adl2PathClient) GetPropertiesResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByClosing())
	result.Response = resp
	return
}

func (client adl2PathClient) Lease(ctx context.Context, xMsLeaseAction adl2.PathLeaseAction, filesystem string, pathParameter string, xMsLeaseDuration *int32, xMsLeaseBreakPeriod *int32, xMsLeaseID string, xMsProposedLeaseID string, ifMatch string, ifNoneMatch string, ifModifiedSince string, ifUnmodifiedSince string, xMsClientRequestID string, timeout *int32, xMsDate string) (result autorest.Response, err error) {
	req, err := client.LeasePreparer(ctx, xMsLeaseAction, filesystem, pathParameter, xMsLeaseDuration, xMsLeaseBreakPeriod, xMsLeaseID, xMsProposedLeaseID, ifMatch, ifNoneMatch, ifModifiedSince, ifUnmodifiedSince, xMsClientRequestID, timeout, xMsDate)
	if err != nil {
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "Lease", nil, "Failure preparing request")
		return
	}

	resp, err := client.LeaseSender(req)
	if err != nil {
		result.Response = resp
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "Lease", resp, "Failure sending request")
		return
	}

	result, err = client.LeaseResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "Lease", resp, "Failure responding to request")
	}

	return
}

// LeasePreparer prepares the Lease request.
func (client adl2PathClient) LeasePreparer(ctx context.Context, xMsLeaseAction adl2.PathLeaseAction, filesystem string, pathParameter string, xMsLeaseDuration *int32, xMsLeaseBreakPeriod *int32, xMsLeaseID string, xMsProposedLeaseID string, ifMatch string, ifNoneMatch string, ifModifiedSince string, ifUnmodifiedSince string, xMsClientRequestID string, timeout *int32, xMsDate string) (*http.Request, error) {
	urlParameters := map[string]interface{}{
		"accountName": client.AccountName,
		"dnsSuffix":   client.DNSSuffix,
	}

	pathParameters := map[string]interface{}{
		"filesystem": autorest.Encode("path", filesystem),
		"path":       autorest.Encode("path", pathParameter),
	}

	queryParameters := map[string]interface{}{}
	if timeout != nil {
		queryParameters["timeout"] = autorest.Encode("query", *timeout)
	}

	preparer := autorest.CreatePreparer(
		autorest.AsPost(),
		autorest.WithCustomBaseURL("http://{accountName}.{dnsSuffix}", urlParameters),
		autorest.WithPathParameters("/{filesystem}/{path}", pathParameters),
		autorest.WithQueryParameters(queryParameters),
		autorest.WithHeader("x-ms-lease-action", autorest.String(xMsLeaseAction)))
	if xMsLeaseDuration != nil {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-lease-duration", autorest.String(*xMsLeaseDuration)))
	}
	if xMsLeaseBreakPeriod != nil {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-lease-break-period", autorest.String(*xMsLeaseBreakPeriod)))
	}
	if len(xMsLeaseID) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-lease-id", autorest.String(xMsLeaseID)))
	}
	if len(xMsProposedLeaseID) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-proposed-lease-id", autorest.String(xMsProposedLeaseID)))
	}
	if len(ifMatch) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Match", autorest.String(ifMatch)))
	}
	if len(ifNoneMatch) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-None-Match", autorest.String(ifNoneMatch)))
	}
	if len(ifModifiedSince) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Modified-Since", autorest.String(ifModifiedSince)))
	}
	if len(ifUnmodifiedSince) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Unmodified-Since", autorest.String(ifUnmodifiedSince)))
	}
	if len(xMsClientRequestID) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-client-request-id", autorest.String(xMsClientRequestID)))
	}
	if len(xMsDate) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-date", autorest.String(xMsDate)))
	}
	if len(client.XMsVersion) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-version", autorest.String(client.XMsVersion)))
	}
	return preparer.Prepare((client.defaultRequest()).WithContext(ctx))
}

// LeaseSender sends the Lease request. The method will close the
// http.Response Body if it receives an error.
func (client adl2PathClient) LeaseSender(req *http.Request) (*http.Response, error) {
	sd := autorest.GetSendDecorators(req.Context(), autorest.DoRetryForStatusCodes(client.RetryAttempts, client.RetryDuration, autorest.StatusCodesForRetry...))
	return autorest.SendWithSender(client, req, sd...)
}

// LeaseResponder handles the response to the Lease request. The method always
// closes the http.Response Body.
func (client adl2PathClient) LeaseResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusCreated, http.StatusAccepted),
		autorest.ByClosing())
	result.Response = resp
	return
}

func (client adl2PathClient) List(ctx context.Context, recursive bool, filesystem string, directory string, continuation string, maxResults *int32, upn *bool, xMsClientRequestID string, timeout *int32, xMsDate string) (result adl2PathList, err error) {
	req, err := client.ListPreparer(ctx, recursive, filesystem, directory, continuation, maxResults, upn, xMsClientRequestID, timeout, xMsDate)
	if err != nil {
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "List", nil, "Failure preparing request")
		return
	}

	resp, err := client.ListSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "List", resp, "Failure sending request")
		return
	}

	result, err = client.ListResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "List", resp, "Failure responding to request")
	}

	return
}

// ListPreparer prepares the List request.
func (client adl2PathClient) ListPreparer(ctx context.Context, recursive bool, filesystem string, directory string, continuation string, maxResults *int32, upn *bool, xMsClientRequestID string, timeout *int32, xMsDate string) (*http.Request, error) {
	urlParameters := map[string]interface{}{
		"accountName": client.AccountName,
		"dnsSuffix":   client.DNSSuffix,
	}

	pathParameters := map[string]interface{}{
		"filesystem": autorest.Encode("path", filesystem),
	}

	queryParameters := map[string]interface{}{
		"recursive": autorest.Encode("query", recursive),
		"resource":  autorest.Encode("query", "filesystem"),
	}
	if len(directory) > 0 {
		queryParameters["directory"] = autorest.Encode("query", directory)
	}
	if len(continuation) > 0 {
		queryParameters["continuation"] = autorest.Encode("query", continuation)
	}
	if maxResults != nil {
		queryParameters["maxresults"] = autorest.Encode("query", *maxResults)
	}
	if upn != nil {
		queryParameters["upn"] = autorest.Encode("query", *upn)
	}
	if timeout != nil {
		queryParameters["timeout"] = autorest.Encode("query", *timeout)
	}

	preparer := autorest.CreatePreparer(
		autorest.AsGet(),
		autorest.WithCustomBaseURL("http://{accountName}.{dnsSuffix}", urlParameters),
		autorest.WithPathParameters("/{filesystem}", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	if len(xMsClientRequestID) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-client-request-id", autorest.String(xMsClientRequestID)))
	}
	if len(xMsDate) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-date", autorest.String(xMsDate)))
	}
	if len(client.XMsVersion) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-version", autorest.String(client.XMsVersion)))
	}
	return preparer.Prepare((client.defaultRequest()).WithContext(ctx))
}

// ListSender sends the List request. The method will close the
// http.Response Body if it receives an error.
func (client adl2PathClient) ListSender(req *http.Request) (*http.Response, error) {
	sd := autorest.GetSendDecorators(req.Context(), autorest.DoRetryForStatusCodes(client.RetryAttempts, client.RetryDuration, autorest.StatusCodesForRetry...))
	return autorest.SendWithSender(client, req, sd...)
}

// ListResponder handles the response to the List request. The method always
// closes the http.Response Body.
func (client adl2PathClient) ListResponder(resp *http.Response) (result adl2PathList, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK),
		autorest.ByUnmarshallingJSON(&result),
		autorest.ByClosing())
	result.Response = autorest.Response{Response: resp}
	return
}

func (client adl2PathClient) Read(ctx context.Context, filesystem string, pathParameter string, rangeParameter string, xMsLeaseID string, xMsRangeGetContentMd5 *bool, ifMatch string, ifNoneMatch string, ifModifiedSince string, ifUnmodifiedSince string, xMsClientRequestID string, timeout *int32, xMsDate string) (result adl2.ReadCloser, err error) {
	req, err := client.ReadPreparer(ctx, filesystem, pathParameter, rangeParameter, xMsLeaseID, xMsRangeGetContentMd5, ifMatch, ifNoneMatch, ifModifiedSince, ifUnmodifiedSince, xMsClientRequestID, timeout, xMsDate)
	if err != nil {
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "Read", nil, "Failure preparing request")
		return
	}

	resp, err := client.ReadSender(req)
	if err != nil {
		result.Response = autorest.Response{Response: resp}
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "Read", resp, "Failure sending request")
		return
	}

	result, err = client.ReadResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "Read", resp, "Failure responding to request")
	}

	return
}

// ReadPreparer prepares the Read request.
func (client adl2PathClient) ReadPreparer(ctx context.Context, filesystem string, pathParameter string, rangeParameter string, xMsLeaseID string, xMsRangeGetContentMd5 *bool, ifMatch string, ifNoneMatch string, ifModifiedSince string, ifUnmodifiedSince string, xMsClientRequestID string, timeout *int32, xMsDate string) (*http.Request, error) {
	urlParameters := map[string]interface{}{
		"accountName": client.AccountName,
		"dnsSuffix":   client.DNSSuffix,
	}

	pathParameters := map[string]interface{}{
		"filesystem": autorest.Encode("path", filesystem),
		"path":       autorest.Encode("path", pathParameter),
	}

	queryParameters := map[string]interface{}{}
	if timeout != nil {
		queryParameters["timeout"] = autorest.Encode("query", *timeout)
	}

	preparer := autorest.CreatePreparer(
		autorest.AsGet(),
		autorest.WithCustomBaseURL("http://{accountName}.{dnsSuffix}", urlParameters),
		autorest.WithPathParameters("/{filesystem}/{path}", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	if len(rangeParameter) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("Range", autorest.String(rangeParameter)))
	}
	if len(xMsLeaseID) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-lease-id", autorest.String(xMsLeaseID)))
	}
	if xMsRangeGetContentMd5 != nil {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-range-get-content-md5", autorest.String(*xMsRangeGetContentMd5)))
	}
	if len(ifMatch) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Match", autorest.String(ifMatch)))
	}
	if len(ifNoneMatch) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-None-Match", autorest.String(ifNoneMatch)))
	}
	if len(ifModifiedSince) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Modified-Since", autorest.String(ifModifiedSince)))
	}
	if len(ifUnmodifiedSince) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Unmodified-Since", autorest.String(ifUnmodifiedSince)))
	}
	if len(xMsClientRequestID) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-client-request-id", autorest.String(xMsClientRequestID)))
	}
	if len(xMsDate) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-date", autorest.String(xMsDate)))
	}
	if len(client.XMsVersion) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-version", autorest.String(client.XMsVersion)))
	}
	return preparer.Prepare((client.defaultRequest()).WithContext(ctx))
}

// ReadSender sends the Read request. The method will close the
// http.Response Body if it receives an error.
func (client adl2PathClient) ReadSender(req *http.Request) (*http.Response, error) {
	sd := autorest.GetSendDecorators(req.Context(), autorest.DoRetryForStatusCodes(client.RetryAttempts, client.RetryDuration, autorest.StatusCodesForRetry...))
	return autorest.SendWithSender(client, req, sd...)
}

// ReadResponder handles the response to the Read request. The method always
// closes the http.Response Body.
func (client adl2PathClient) ReadResponder(resp *http.Response) (result adl2.ReadCloser, err error) {
	result.Value = &resp.Body
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusPartialContent))
	result.Response = autorest.Response{Response: resp}
	return
}

func (client adl2PathClient) Update(ctx context.Context, action adl2.PathUpdateAction, filesystem string, pathParameter string, position *int64, retainUncommittedData *bool, closeParameter *bool, contentLength *int64, contentMD5 string, xMsLeaseID string, xMsCacheControl string, xMsContentType string, xMsContentDisposition string, xMsContentEncoding string, xMsContentLanguage string, xMsContentMd5 string, xMsProperties string, xMsOwner string, xMsGroup string, xMsPermissions string, xMsACL string, ifMatch string, ifNoneMatch string, ifModifiedSince string, ifUnmodifiedSince string, requestBody io.ReadCloser, xMsClientRequestID string, timeout *int32, xMsDate string) (result autorest.Response, err error) {
	req, err := client.UpdatePreparer(ctx, action, filesystem, pathParameter, position, retainUncommittedData, closeParameter, contentLength, contentMD5, xMsLeaseID, xMsCacheControl, xMsContentType, xMsContentDisposition, xMsContentEncoding, xMsContentLanguage, xMsContentMd5, xMsProperties, xMsOwner, xMsGroup, xMsPermissions, xMsACL, ifMatch, ifNoneMatch, ifModifiedSince, ifUnmodifiedSince, requestBody, xMsClientRequestID, timeout, xMsDate)
	if err != nil {
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "Update", nil, "Failure preparing request")
		return
	}

	resp, err := client.UpdateSender(req)
	if err != nil {
		result.Response = resp
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "Update", resp, "Failure sending request")
		return
	}

	result, err = client.UpdateResponder(resp)
	if err != nil {
		err = autorest.NewErrorWithError(err, "storagedatalake.adl2PathClient", "Update", resp, "Failure responding to request")
	}

	return
}

// UpdatePreparer prepares the Update request.
func (client adl2PathClient) UpdatePreparer(ctx context.Context, action adl2.PathUpdateAction, filesystem string, pathParameter string, position *int64, retainUncommittedData *bool, closeParameter *bool, contentLength *int64, contentMD5 string, xMsLeaseID string, xMsCacheControl string, xMsContentType string, xMsContentDisposition string, xMsContentEncoding string, xMsContentLanguage string, xMsContentMd5 string, xMsProperties string, xMsOwner string, xMsGroup string, xMsPermissions string, xMsACL string, ifMatch string, ifNoneMatch string, ifModifiedSince string, ifUnmodifiedSince string, requestBody io.ReadCloser, xMsClientRequestID string, timeout *int32, xMsDate string) (*http.Request, error) {
	urlParameters := map[string]interface{}{
		"accountName": client.AccountName,
		"dnsSuffix":   client.DNSSuffix,
	}

	pathParameters := map[string]interface{}{
		"filesystem": autorest.Encode("path", filesystem),
		"path":       autorest.Encode("path", pathParameter),
	}

	queryParameters := map[string]interface{}{
		"action": autorest.Encode("query", action),
	}
	if position != nil {
		queryParameters["position"] = autorest.Encode("query", *position)
	}
	if retainUncommittedData != nil {
		// query params need to be lower case otherwise azure-storage-blob-go's
		// SharedKeyCredential signing won't work:
		// https://github.com/Azure/azure-storage-blob-go/issues/146
		queryParameters["retainuncommitteddata"] = autorest.Encode("query", *retainUncommittedData)
	}
	if closeParameter != nil {
		queryParameters["close"] = autorest.Encode("query", *closeParameter)
	}
	if timeout != nil {
		queryParameters["timeout"] = autorest.Encode("query", *timeout)
	}

	preparer := autorest.CreatePreparer(
		autorest.AsContentType("application/octet-stream"),
		autorest.AsPatch(),
		autorest.WithCustomBaseURL("http://{accountName}.{dnsSuffix}", urlParameters),
		autorest.WithPathParameters("/{filesystem}/{path}", pathParameters),
		autorest.WithQueryParameters(queryParameters))
	if requestBody != nil {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithFile(requestBody))
	}
	if contentLength != nil {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("Content-Length", autorest.String(*contentLength)))
	}
	if len(contentMD5) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("Content-MD5", autorest.String(contentMD5)))
	}
	if len(xMsLeaseID) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-lease-id", autorest.String(xMsLeaseID)))
	}
	if len(xMsCacheControl) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-cache-control", autorest.String(xMsCacheControl)))
	}
	if len(xMsContentType) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-content-type", autorest.String(xMsContentType)))
	}
	if len(xMsContentDisposition) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-content-disposition", autorest.String(xMsContentDisposition)))
	}
	if len(xMsContentEncoding) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-content-encoding", autorest.String(xMsContentEncoding)))
	}
	if len(xMsContentLanguage) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-content-language", autorest.String(xMsContentLanguage)))
	}
	if len(xMsContentMd5) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-content-md5", autorest.String(xMsContentMd5)))
	}
	if len(xMsProperties) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-properties", autorest.String(xMsProperties)))
	}
	if len(xMsOwner) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-owner", autorest.String(xMsOwner)))
	}
	if len(xMsGroup) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-group", autorest.String(xMsGroup)))
	}
	if len(xMsPermissions) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-permissions", autorest.String(xMsPermissions)))
	}
	if len(xMsACL) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-acl", autorest.String(xMsACL)))
	}
	if len(ifMatch) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Match", autorest.String(ifMatch)))
	}
	if len(ifNoneMatch) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-None-Match", autorest.String(ifNoneMatch)))
	}
	if len(ifModifiedSince) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Modified-Since", autorest.String(ifModifiedSince)))
	}
	if len(ifUnmodifiedSince) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("If-Unmodified-Since", autorest.String(ifUnmodifiedSince)))
	}
	if len(xMsClientRequestID) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-client-request-id", autorest.String(xMsClientRequestID)))
	}
	if len(xMsDate) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-date", autorest.String(xMsDate)))
	}
	if len(client.XMsVersion) > 0 {
		preparer = autorest.DecoratePreparer(preparer,
			autorest.WithHeader("x-ms-version", autorest.String(client.XMsVersion)))
	}
	return preparer.Prepare((client.defaultRequest()).WithContext(ctx))
}

// UpdateSender sends the Update request. The method will close the
// http.Response Body if it receives an error.
func (client adl2PathClient) UpdateSender(req *http.Request) (*http.Response, error) {

	sd := autorest.GetSendDecorators(req.Context(), autorest.DoRetryForStatusCodes(client.RetryAttempts, client.RetryDuration, autorest.StatusCodesForRetry...))
	return autorest.SendWithSender(client, req, sd...)
}

// UpdateResponder handles the response to the Update request. The method always
// closes the http.Response Body.
func (client adl2PathClient) UpdateResponder(resp *http.Response) (result autorest.Response, err error) {
	err = autorest.Respond(
		resp,
		client.ByInspecting(),
		azure.WithErrorUnlessStatusCode(http.StatusOK, http.StatusAccepted),
		autorest.ByClosing())
	result.Response = resp
	return
}

func (client adl2PathClient) defaultRequest() *http.Request {
	r := &http.Request{}
	r.GetBody = func() (io.ReadCloser, error) {
		if r.Body == nil {
			return http.NoBody, nil
		} else if seeker, ok := r.Body.(io.ReadSeeker); ok {
			// internally goofys always uses seekable
			// readers, so we can rewind on
			// retry. autorest makes a copy of the buffer
			// and this avoids that waste
			_, err := seeker.Seek(0, 0)
			return &ReadSeekerCloser{seeker}, err
		} else {
			panic(fmt.Sprintf("Wrong type: %T", r.Body))
		}
	}
	return r
}

type adl2PathList struct {
	autorest.Response `json:"-"`
	Paths             *[]adl2Path `json:"paths,omitempty"`
}

type adl2Path struct {
	Name          *string    `json:"name,omitempty"`
	IsDirectory   adl2Bool   `json:"isDirectory,omitempty"`
	LastModified  *string    `json:"lastModified,omitempty"`
	ETag          *string    `json:"eTag,omitempty"`
	ContentLength *adl2Int64 `json:"contentLength,omitempty"`
	Owner         *string    `json:"owner,omitempty"`
	Group         *string    `json:"group,omitempty"`
	Permissions   *string    `json:"permissions,omitempty"`
}

func (p adl2Path) isDirectory() bool {
	return p.IsDirectory.boolValue()
}

func (p adl2Path) contentLength() int64 {
	if p.ContentLength == nil {
		return 0
	} else {
		return p.ContentLength.intValue()
	}
}

type adl2Bool struct {
	bool
}

func (b adl2Bool) boolValue() bool {
	return b.bool
}

func (b *adl2Bool) UnmarshalJSON(buf []byte) error {
	v := string(buf)
	b.bool = v == "true" || v == "\"true\"" || v == "'true'"
	return nil
}

type adl2Int64 struct {
	int64
}

func (b *adl2Int64) intValue() int64 {
	return b.int64
}

func (b *adl2Int64) UnmarshalJSON(buf []byte) error {
	if len(buf) == 0 {
		return fmt.Errorf("no input")
	}
	if buf[0] == '"' {
		var v string
		err := json.Unmarshal(buf, &v)
		if err != nil {
			return err
		}
		b.int64, err = strconv.ParseInt(v, 10, 64)
		return err
	} else {
		err := json.Unmarshal(buf, &b.int64)
		return err
	}
}
