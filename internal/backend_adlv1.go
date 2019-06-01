// Copyright 2019 Databricks
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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	retryablehttp "github.com/hashicorp/go-retryablehttp"
	"github.com/jacobsa/fuse"
	hdfs "github.com/vladimirvivien/gowfs"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type ADLv1 struct {
	cap   Capabilities
	flags *FlagStorage

	client   *retryablehttp.Client
	endpoint url.URL
	// ADLv1 doesn't actually have the concept of buckets (defined
	// by me as a top level container that can be created with
	// existing credentials). This bucket is more like a backend
	// level prefix mostly to ease testing
	bucket string
}

type ADLv1Op struct {
	method   string
	params   url.Values
	headers  http.Header
	rawError bool
}

func newADLv1Op(method, op string) ADLv1Op {
	return ADLv1Op{
		method:  method,
		params:  url.Values{"op": []string{op}},
		headers: http.Header{},
	}
}

func (op ADLv1Op) String() string {
	return op.params.Encode()
}

type ADLv1Err struct {
	RemoteException struct {
		Exception     string
		Message       string
		JavaClassName string
	}
	resp *http.Response
}

func (err ADLv1Err) Error() string {
	return fmt.Sprintf("%v %v", err.resp.Status, err.RemoteException)
}

func (op ADLv1Op) New() ADLv1Op {
	// stupid golang doesn't have an easy way to copy map
	res := ADLv1Op{
		method:   op.method,
		params:   url.Values{},
		headers:  http.Header{},
		rawError: op.rawError,
	}

	for k, v := range op.params {
		var values []string
		for _, s := range v {
			values = append(values, s)
		}
		res.params[k] = values
	}
	for k, v := range op.headers {
		var values []string
		for _, s := range v {
			values = append(values, s)
		}
		res.headers[k] = values
	}

	return res
}

func (op ADLv1Op) Param(k, v string) ADLv1Op {
	op.params.Add(k, v)
	return op
}

func (op ADLv1Op) Perm(mode os.FileMode) ADLv1Op {
	return op.Param("permission", fmt.Sprintf("0%o", mode))
}

func (op ADLv1Op) Header(k, v string) ADLv1Op {
	op.headers.Add(k, v)
	return op
}

func (op ADLv1Op) RawError() ADLv1Op {
	op.rawError = true
	return op
}

const ADL1_REQUEST_ID = "X-Ms-Request-Id"

// APPEND and CREATE additionally supports "append=true" and
// "write=true" to prevent initial 307 redirect because it just
// redirects to the same domain anyway
//
// there's no such thing as abort, but at least we should release the
// lease which technically is more like a commit than abort. In this
// case we are not sending data so there's no reason to do expect
// 100-continue
var ADL1_ABORT = newADLv1Op("POST", "APPEND").
	Param("append", "true").
	Param("syncFlag", "CLOSE")
var ADL1_APPEND = newADLv1Op("POST", "APPEND").
	Param("append", "true").
	Param("syncFlag", "DATA").
	Header("Expect", "100-continue").
	RawError()
var ADL1_CLOSE = newADLv1Op("POST", "APPEND").
	Param("append", "true").
	Param("syncFlag", "CLOSE").
	RawError()
var ADL1_CREATE = newADLv1Op("PUT", "CREATE").
	Param("write", "true").
	Param("overwrite", "true").
	Header("Expect", "100-continue")
var ADL1_DELETE = newADLv1Op("DELETE", "DELETE").Param("recursive", "false")
var ADL1_GETFILESTATUS = newADLv1Op("GET", "GETFILESTATUS")
var ADL1_LISTSTATUS = newADLv1Op("GET", "LISTSTATUS")
var ADL1_MKDIRS = newADLv1Op("PUT", "MKDIRS")
var ADL1_OPEN = newADLv1Op("GET", "OPEN").Param("read", "true")
var ADL1_RENAME = newADLv1Op("PUT", "RENAME").Param("renameoptions", "OVERWRITE")

type ListStatusResult struct {
	FileStatuses struct {
		FileStatus        []hdfs.FileStatus
		ContinuationToken string
	}
}
type BooleanResult struct {
	Boolean bool
}

type ADLv1MultipartBlobCommitInput struct {
	Size uint64
}

func retryableHttpClient(c *http.Client, oauth bool) *retryablehttp.Client {
	retryPolicy := func(ctx context.Context, resp *http.Response, err error) (bool, error) {
		// do not retry on context.Canceled or context.DeadlineExceeded
		if ctx.Err() != nil {
			return false, ctx.Err()
		}

		if oauth2Err := toOauth2Error(err); oauth2Err != nil {
			if oauth2Err.Response != nil {
				return retryablehttp.DefaultRetryPolicy(ctx, oauth2Err.Response, nil)
			} else {
				return true, oauth2Err
			}
		}

		return retryablehttp.DefaultRetryPolicy(ctx, resp, err)
	}

	return &retryablehttp.Client{
		HTTPClient:   c,
		Backoff:      retryablehttp.LinearJitterBackoff,
		Logger:       RetryHTTPLogger{s3Log},
		RetryWaitMax: 1 * time.Second,
		// XXX figure out a better number
		RetryMax:   20,
		CheckRetry: retryPolicy,
		RequestLogHook: func(_ retryablehttp.Logger, r *http.Request, nRetry int) {
			if nRetry != 0 {
				// generate a new request id
				oldRequestId := r.Header.Get(ADL1_REQUEST_ID)
				newRequestId := uuid.New().String()
				r.Header.Set(ADL1_REQUEST_ID, newRequestId)

				s3Log.Debugf("%v %v %v retry#%v as %v", r.Method, r.URL,
					oldRequestId, nRetry, newRequestId)
			} else {
				s3Log.Debugf("%v %v %v", r.Method, r.URL,
					r.Header.Get(ADL1_REQUEST_ID))
			}
		},
		ResponseLogHook: func(_ retryablehttp.Logger, r *http.Response) {
			s3Log.Debugf("%v %v %v %v %v", r.Request.Method, r.Request.URL,
				r.Request.Header.Get(ADL1_REQUEST_ID), r.Status, r.Header)
		},
	}
}

type RetryClient struct {
	client *retryablehttp.Client
}

func (c RetryClient) RoundTrip(r *http.Request) (*http.Response, error) {
	req, err := retryablehttp.NewRequest(r.Method, r.URL.String(), r.Body)
	if err != nil {
		return nil, err
	}
	req.Request = r
	return c.client.Do(req)
}

func IsADLv1Endpoint(endpoint string) bool {
	u, err := url.Parse(endpoint)
	if err != nil {
		return false
	}
	return strings.HasSuffix(u.Hostname(), ".azuredatalakestore.net")
}

func NewADLv1(bucket string, flags *FlagStorage) (*ADLv1, error) {
	tokenURL := flags.ADRefreshUrl
	if tokenURL == "" {
		tokenURL = fmt.Sprintf("https://login.microsoftonline.com/%v/oauth2/token", flags.ADTenantID)
	}

	conf := clientcredentials.Config{
		ClientID:       flags.ADClientID,
		ClientSecret:   flags.ADClientSecret,
		TokenURL:       tokenURL,
		EndpointParams: url.Values{"resource": {"https://management.core.windows.net/"}},
	}
	conf.AuthStyle = oauth2.AuthStyleInParams

	endpoint, err := url.Parse(flags.Endpoint)
	if err != nil {
		return nil, err
	}

	if endpoint.Host == "" {
		// if endpoint doesn't have scheme: foobar.com or foobar.com/a,
		// it will get parsed as the Path
		newEndpoint := "https://" + flags.Endpoint
		endpoint, err = url.Parse(newEndpoint)
		if err != nil {
			return nil, err
		}
	}
	if endpoint.Scheme == "" {
		endpoint.Scheme = "https"
	}
	endpoint.Path = "/webhdfs/v1/"

	ctx := context.WithValue(context.TODO(), oauth2.HTTPClient,
		&http.Client{
			Transport: RetryClient{retryableHttpClient(&http.Client{
				Transport: &http.Transport{
					Proxy: http.ProxyFromEnvironment,
					DialContext: (&net.Dialer{
						Timeout:   30 * time.Second,
						KeepAlive: 30 * time.Second,
					}).DialContext,
					MaxIdleConns:          100,
					IdleConnTimeout:       90 * time.Second,
					MaxIdleConnsPerHost:   100,
					TLSHandshakeTimeout:   3 * time.Second,
					ExpectContinueTimeout: 1 * time.Second,
				},
			}, true)},
		})
	tokenSource := oauth2.ReuseTokenSource(nil, conf.TokenSource(ctx))
	oauth2Client := oauth2.NewClient(context.TODO(), tokenSource)

	return &ADLv1{
		flags:    flags,
		client:   retryableHttpClient(oauth2Client, false),
		endpoint: *endpoint,
		bucket:   bucket,
		cap: Capabilities{
			NoParallelMultipart: true,
			DirBlob:             true,
		},
	}, nil
}

func toOauth2Error(err error) *oauth2.RetrieveError {
	if urlErr, ok := err.(*url.Error); ok {
		if oauth2Err, ok := urlErr.Err.(*oauth2.RetrieveError); ok {
			return oauth2Err
		}
	}
	return nil
}

func mapADLv1Error(op *ADLv1Op, resp *http.Response, err error) error {
	if err != nil {
		oauth2Err := toOauth2Error(err)
		if oauth2Err != nil {
			if op.rawError {
				return fmt.Errorf("%v", oauth2Err.Error())
			}
			err = mapHttpError(oauth2Err.Response.StatusCode)
			if err != nil {
				return err
			}
		}
		if op.rawError {
			return err
		}
		return syscall.EAGAIN
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusTemporaryRedirect {
			return nil
		}
		if resp.StatusCode >= 400 && op != nil && op.rawError {
			var adlErr ADLv1Err
			body, bodyErr := ioutil.ReadAll(resp.Body)
			if bodyErr == nil {
				decoder := json.NewDecoder(bytes.NewReader(body))
				jsonErr := decoder.Decode(&adlErr)

				if jsonErr == nil {
					s3Log.Errorf("%v %v %v", resp.Request.Method, resp.Request.URL.String(), adlErr)
					adlErr.resp = resp
					return adlErr
				} else {
					s3Log.Errorf("%v %v %v", resp.Request.Method, resp.Request.URL.String(), body)
				}
			} else {
				// we cannot read the error body,
				// nothing much we can do
			}

		}
		err = mapHttpError(resp.StatusCode)
		if err != nil {
			return err
		} else {
			op := resp.Request.URL.Query().Get("op")
			requestId := resp.Header.Get(ADL1_REQUEST_ID)
			s3Log.Errorf("%v %v %v %v", op, resp.Request.URL.String(), requestId, resp.Status)
			return syscall.EINVAL
		}
	}

	return nil
}

func (b *ADLv1) get(op ADLv1Op, path string, res interface{}) error {
	return b.call(op, path, nil, res)
}

func (b *ADLv1) call(op ADLv1Op, path string, arg interface{}, res interface{}) error {
	endpoint := b.endpoint
	path = strings.TrimLeft(path, "/")
	if b.bucket != "" {
		if path != "" {
			path = b.bucket + "/" + path
		} else {
			path = b.bucket
		}
	}

	endpoint.Path += path
	endpoint.RawQuery = op.params.Encode()

	var body io.ReadSeeker
	var err error

	if arg != nil {
		if r, ok := arg.(io.ReadSeeker); ok {
			body = r
			// retryablehttp doesn't close the request body
			if closer, ok := body.(io.Closer); ok {
				defer func() {
					err := closer.Close()
					if err != nil {
						s3Log.Errorf("%v close: %v", endpoint.String(), err)
					}
				}()
			}

		} else {
			buf, err := json.Marshal(arg)
			if err != nil {
				s3Log.Errorf("json error: %v", err)
				return fuse.EINVAL
			}
			body = bytes.NewReader(buf)
		}
	} else {
		body = bytes.NewReader([]byte(""))
	}

	req, err := retryablehttp.NewRequest(op.method, endpoint.String(), body)
	if err != nil {
		s3Log.Errorf("NewRequest error: %v", err)
		return fuse.EINVAL
	}

	req.Header = op.headers
	req.Header.Add(ADL1_REQUEST_ID, uuid.New().String())
	var resp *http.Response

	var i int
	for i = 0; i < 10; i++ {
		resp, err = b.client.Do(req)
		err = mapADLv1Error(&op, resp, err)
		if err != nil {
			return err
		}
		if resp.StatusCode == http.StatusTemporaryRedirect {
			location, err := resp.Location()
			if err != nil {
				s3Log.Errorf("redirect from %v but no Location header", endpoint)
				return syscall.EAGAIN
			}
			s3Log.Infof("redirect %v %v -> %v", req.Method, req.URL.String(), location)
			body.Seek(0, 0)
			req, err = retryablehttp.NewRequest(op.method, location.String(), body)
			if err != nil {
				s3Log.Errorf("NewRequest error: %v", err)
				return fuse.EINVAL
			}
		} else {
			break
		}
	}
	if i == 10 {
		s3Log.Errorf("too many redirects for %v %v", req.Method, req.URL.String())
	}

	if res != nil {
		if reader, ok := res.(**http.Response); ok {
			*reader = resp
		} else if reader, ok := res.(*io.Reader); ok {
			*reader = resp.Body
		} else {
			defer resp.Body.Close()

			decoder := json.NewDecoder(resp.Body)
			err = decoder.Decode(res)
			if err != nil {
				log.Errorf("adlv1 api %v decode error: %v", op, err)
			}
		}
	} else {
		// consume the stream so we can re-use the connection
		// for keep-alive
		defer resp.Body.Close()
		io.Copy(ioutil.Discard, resp.Body)
	}

	return err
}

func (b *ADLv1) Init(key string) error {
	return b.get(ADL1_GETFILESTATUS.New().RawError(), key, nil)
}

func (b *ADLv1) Capabilities() *Capabilities {
	return &b.cap
}

func adlv1LastModified(t int64) time.Time {
	return time.Unix(t/1000, t%1000000)
}

func adlv1FileStatus2BlobItem(f *hdfs.FileStatus, key *string) BlobItemOutput {
	return BlobItemOutput{
		Key:          key,
		LastModified: PTime(adlv1LastModified(f.ModificationTime)),
		Size:         uint64(f.Length),
		StorageClass: PString(strconv.FormatInt(f.Replication, 10)),
	}
}

func (b *ADLv1) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	type FileStatus struct {
		FileStatus hdfs.FileStatus
	}
	res := FileStatus{}
	err := b.get(ADL1_GETFILESTATUS.New(), param.Key, &res)
	if err != nil {
		return nil, err
	}

	f := res.FileStatus

	return &HeadBlobOutput{
		BlobItemOutput: adlv1FileStatus2BlobItem(&f, &param.Key),
		IsDirBlob:      f.Type == "DIRECTORY",
	}, nil
}

func (b *ADLv1) appendToListResults(listOp ADLv1Op, path string, recursive bool,
	prefixes []BlobPrefixOutput, items []BlobItemOutput) (*ListStatusResult, []BlobPrefixOutput, []BlobItemOutput, error) {
	res := ListStatusResult{}

	err := b.get(listOp, path, &res)
	if err != nil {
		return nil, nil, nil, err
	}

	path = strings.TrimRight(path, "/")

	for _, i := range res.FileStatuses.FileStatus {
		key := i.PathSuffix
		if path != "" {
			key = path + "/" + key
		}

		if i.Type == "DIRECTORY" {
			if recursive {
				// we shouldn't generate prefixes if
				// it's a recursive listing
				items = append(items, adlv1FileStatus2BlobItem(&i, &key))

				_, prefixes, items, err = b.appendToListResults(listOp,
					key, recursive, prefixes, items)
			} else {
				prefixes = append(prefixes, BlobPrefixOutput{
					Prefix: PString(key + "/"),
				})
			}
		} else {
			items = append(items, adlv1FileStatus2BlobItem(&i, &key))
		}
	}

	return &res, prefixes, items, nil
}

func (b *ADLv1) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	var recursive bool
	if param.Delimiter == nil {
		// used by tests to cleanup (and also slurping, but
		// that's only enabled on S3 right now)
		recursive = true
		// cannot emulate these
		if param.ContinuationToken != nil || param.StartAfter != nil {
			return nil, syscall.ENOTSUP
		}
	} else if *param.Delimiter != "/" {
		return nil, syscall.ENOTSUP
	}

	listOp := ADL1_LISTSTATUS.New()
	// it's not documented in
	// https://docs.microsoft.com/en-us/rest/api/datalakestore/webhdfs-filesystem-apis
	// but from their Java SDK looks like they support additional params:
	// https://github.com/Azure/azure-data-lake-store-java/blob/f5c270b8cb2ac68536b2cb123d355a874cade34c/src/main/java/com/microsoft/azure/datalake/store/Core.java#L861
	if param.StartAfter != nil {
		listOp.Param("listAfter", *param.StartAfter)
	}
	if param.MaxKeys != nil {
		listOp.Param("listSize", strconv.FormatUint(uint64(*param.MaxKeys), 10))
	}

	var continuationToken *string

	res, prefixes, items, err := b.appendToListResults(listOp, nilStr(param.Prefix),
		recursive, nil, nil)
	if err != fuse.ENOENT {
		if err != nil {
			return nil, err
		}

		if len(prefixes) == 0 && len(items) == 0 && param.Prefix != nil {
			// the only way we get nothing is if we are listing an empty dir,
			// because ADLv1 returns 404 if the prefix didn't exist
			if strings.HasSuffix(*param.Prefix, "/") {
				items = []BlobItemOutput{BlobItemOutput{
					Key: param.Prefix,
				},
				}
			} else {
				prefixes = []BlobPrefixOutput{BlobPrefixOutput{
					Prefix: PString(*param.Prefix + "/"),
				},
				}
			}
		} else if len(items) != 0 && param.Prefix != nil &&
			strings.HasSuffix(*items[0].Key, "/") {
			if *items[0].Key == *param.Prefix {
				// if we list "file/", somehow we will
				// get back pathSuffix="" which
				// appendToListResults would massage
				// back into "file1/", we don't want
				// that entry
				items = items[1:]
			} else if *items[0].Key == (*param.Prefix + "/") {
				items[0].Key = PString(*param.Prefix)
			}
		}

		if res.FileStatuses.ContinuationToken != "" {
			continuationToken = PString(res.FileStatuses.ContinuationToken)
		}
	} else {
		err = nil
	}

	return &ListBlobsOutput{
		Prefixes:          prefixes,
		Items:             items,
		ContinuationToken: continuationToken,
		IsTruncated:       continuationToken != nil,
	}, nil
}

func (b *ADLv1) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	var res BooleanResult

	err := b.call(ADL1_DELETE.New(), strings.TrimRight(param.Key, "/"), nil, &res)
	if err != nil {
		return nil, err
	}
	if !res.Boolean {
		return nil, fuse.ENOENT
	}
	return &DeleteBlobOutput{}, nil
}

func (b *ADLv1) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	progress := true
	toDelete := param.Items

	for progress {
		progress = false
		var dirs []string

		for _, i := range toDelete {
			_, err := b.DeleteBlob(&DeleteBlobInput{i})
			if err != nil {
				if err != fuse.ENOENT {
					// if we delete a directory that's not
					// empty, ADLv1 returns 403. That can
					// happen if we want to delete both
					// "dir1" and "dir1/file" but delete
					// them in the wrong order for example
					if err == syscall.EACCES {
						dirs = append(dirs, i)
					} else {
						return nil, err
					}
				} else {
					progress = true
				}
			} else {
				progress = true
			}
		}

		if len(dirs) == 0 {
			break
		}

		toDelete = dirs
	}

	return &DeleteBlobsOutput{}, nil
}

func (b *ADLv1) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	var res BooleanResult

	dest := param.Destination
	if b.bucket != "" {
		dest = b.bucket + "/" + dest
	}

	err := b.call(ADL1_RENAME.New().Param("destination", dest),
		param.Source, nil, &res)
	if err != nil {
		return nil, err
	}

	if !res.Boolean {
		// ADLv1 returns false if we try to rename a dir to a
		// file, or if the rename source doesn't exist. We
		// should have prevented renaming a dir to a file at
		// upper layer so this is probably the former

		// (the reverse, renaming a file to a directory works
		// in ADLv1 and is the same as moving the file into
		// the directory)
		return nil, fuse.ENOENT
	}

	return &RenameBlobOutput{}, nil
}

func (b *ADLv1) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *ADLv1) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	open := ADL1_OPEN.New()
	if param.Start != 0 {
		open.Param("offset", strconv.FormatUint(param.Start, 10))
	}
	if param.Count != 0 {
		open.Param("length", strconv.FormatUint(param.Count, 10))
	}
	if param.IfMatch != nil {
		open.Param("filesessionid", *param.IfMatch)
	}

	var resp *http.Response

	err := b.call(open, param.Key, nil, &resp)
	if err != nil {
		return nil, err
	}
	if resp != nil {
		defer func() {
			if resp.Body != nil {
				resp.Body.Close()
			}
		}()
	}
	// WebHDFS specifies that Content-Length is returned but ADLv1
	// doesn't return it. Thankfully we never actually use it in
	// the context of GetBlobOutput

	var contentType *string
	// not very useful since ADLv1 always return application/octet-stream
	if val, ok := resp.Header["Content-Type"]; ok && len(val) != 0 {
		contentType = &val[len(val)-1]
	}

	res := GetBlobOutput{
		HeadBlobOutput: HeadBlobOutput{
			BlobItemOutput: BlobItemOutput{
				Key: &param.Key,
			},
			ContentType: contentType,
			IsDirBlob:   false,
		},
		Body: resp.Body,
	}
	resp.Body = nil

	return &res, nil
}

func (b *ADLv1) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	if param.DirBlob {
		err := b.mkdir(param.Key)
		if err != nil {
			return nil, err
		}
	} else {
		create := ADL1_CREATE.New().Perm(b.flags.FileMode)
		if param.ContentType != nil {
			create.Header("Content-Type", *param.ContentType)
		}
		err := b.call(create, param.Key, param.Body, nil)
		if err != nil {
			return nil, err
		}
	}

	return &PutBlobOutput{}, nil
}

func (b *ADLv1) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	// ADLv1 doesn't have the concept of atomic replacement which
	// means that when we replace an object, readers may see
	// intermediate results. Here we implement MPU by first
	// sending a CREATE with 0 bytes and acquire a lease at the
	// same time.  much of these is not documented anywhere except
	// in the SDKs:
	// https://github.com/Azure/azure-data-lake-store-java/blob/f5c270b8cb2ac68536b2cb123d355a874cade34c/src/main/java/com/microsoft/azure/datalake/store/Core.java#L84
	leaseId := uuid.New().String()

	create := ADL1_CREATE.New().Perm(b.flags.FileMode)
	if param.ContentType != nil {
		create.Header("Content-Type", *param.ContentType)
	}
	create.Param("leaseid", leaseId)
	create.Param("syncFlag", "DATA")
	// https://docs.microsoft.com/en-us/dotnet/api/microsoft.azure.management.datalake.store.filesystemoperationsextensions.appendasync?view=azure-dotnet
	create.Param("filesessionid", leaseId)

	err := b.call(create, param.Key, nil, nil)
	if err != nil {
		return nil, err
	}
	return &MultipartBlobCommitInput{
		Key:         &param.Key,
		UploadId:    &leaseId,
		backendData: &ADLv1MultipartBlobCommitInput{},
	}, nil
}

func (b *ADLv1) uploadPart(param *MultipartBlobAddInput, offset uint64) error {
	append := ADL1_APPEND.New().
		Param("leaseid", *param.Commit.UploadId).
		Param("filesessionid", *param.Commit.UploadId).
		Param("offset", strconv.FormatUint(offset-param.Size, 10))

	err := b.call(append, *param.Commit.Key, param.Body, nil)
	if err != nil {
		if adlErr, ok := err.(ADLv1Err); ok {
			if adlErr.resp.StatusCode == 404 {
				// ADLv1 APPEND returns 404 if either:
				// the request payload is too large:
				// https://social.msdn.microsoft.com/Forums/azure/en-US/48e86ce8-79f8-4412-838f-8e2a60b5f387/notfound-error-on-call-to-data-lake-store-create?forum=AzureDataLake

				// or if a second concurrent stream is
				// created. The behavior is odd: seems
				// like the first stream will error
				// but the latter stream works fine
				err = fuse.EINVAL
				return err
			} else if adlErr.resp.StatusCode == 400 &&
				adlErr.RemoteException.Exception == "BadOffsetException" {
				appendErr := b.detectTransientError(param, offset)
				if appendErr == nil {
					return nil
				}
			}
			err = mapADLv1Error(nil, adlErr.resp, nil)
		}
	}
	return err
}

func (b *ADLv1) detectTransientError(param *MultipartBlobAddInput, offset uint64) error {
	append := ADL1_APPEND.New().
		Param("leaseid", *param.Commit.UploadId).
		Param("filesessionid", *param.Commit.UploadId).
		Param("offset", strconv.FormatUint(offset, 10))
	append.rawError = false
	return b.call(append, *param.Commit.Key, nil, nil)
}

func (b *ADLv1) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	// APPEND with the expected offsets (so we can detect
	// concurrent updates to the same file and fail, in case lease
	// is for some reason broken on the server side

	var commitData *ADLv1MultipartBlobCommitInput
	var ok bool
	if commitData, ok = param.Commit.backendData.(*ADLv1MultipartBlobCommitInput); !ok {
		panic("Incorrect commit data type")
	}

	commitData.Size += param.Size
	err := b.uploadPart(param, commitData.Size)
	if err != nil {
		return nil, err
	}

	return &MultipartBlobAddOutput{}, nil
}

func (b *ADLv1) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	// there's no such thing as abort, but at least we should release the lease
	// which technically is more like a commit than abort
	abort := ADL1_ABORT.New().
		Param("leaseid", *param.UploadId).
		Param("filesessionid", *param.UploadId)

	return &MultipartBlobAbortOutput{}, b.call(abort, *param.Key, nil, nil)
}

func (b *ADLv1) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	var commitData *ADLv1MultipartBlobCommitInput
	var ok bool
	if commitData, ok = param.backendData.(*ADLv1MultipartBlobCommitInput); !ok {
		panic("Incorrect commit data type")
	}

	close := ADL1_CLOSE.New().
		Param("leaseid", *param.UploadId).
		Param("filesessionid", *param.UploadId).
		Param("offset", strconv.FormatUint(commitData.Size, 10))

	err := b.call(close, *param.Key, nil, nil)
	if err == fuse.ENOENT {
		// either the blob was concurrently deleted or we got
		// another CREATE which broke our lease. Either way
		// technically we did finish uploading data so swallow
		// the error
		err = nil
	}
	if err != nil {
		return nil, err
	}

	return &MultipartBlobCommitOutput{}, nil
}

func (b *ADLv1) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *ADLv1) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error) {
	if b.bucket == "" {
		return nil, fuse.EINVAL
	}

	var res BooleanResult

	err := b.call(ADL1_DELETE.New(), "", nil, &res)
	if err != nil {
		return nil, err
	}

	// delete can return false because the directory is not there
	if !res.Boolean {
		return nil, fuse.ENOENT
	}

	return &RemoveBucketOutput{}, nil
}

func (b *ADLv1) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error) {
	if b.bucket == "" {
		return nil, fuse.EINVAL
	}

	err := b.mkdir("")
	if err != nil {
		return nil, err
	}

	return &MakeBucketOutput{}, nil
}

func (b *ADLv1) mkdir(dir string) error {
	var res BooleanResult

	err := b.call(ADL1_MKDIRS.New().Perm(b.flags.DirMode), dir, nil, &res)
	if err != nil {
		return err
	}
	if !res.Boolean {
		return fuse.EEXIST
	}
	return nil
}
