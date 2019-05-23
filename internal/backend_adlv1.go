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
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	retryablehttp "github.com/hashicorp/go-retryablehttp"
	"github.com/jacobsa/fuse"
	"github.com/sirupsen/logrus"
	hdfs "github.com/vladimirvivien/gowfs"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

type ADLv1 struct {
	fs    *Goofys
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
	params  url.Values
	headers http.Header
}

func newADLv1Op(op string) ADLv1Op {
	return ADLv1Op{
		params:  url.Values{"op": []string{op}},
		headers: http.Header{},
	}
}

func (op ADLv1Op) String() string {
	return op.params.Encode()
}

func (op ADLv1Op) New() ADLv1Op {
	// stupid golang doesn't have an easy way to copy map
	res := ADLv1Op{
		params:  url.Values{},
		headers: http.Header{},
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

var ADL1_GETFILESTATUS = newADLv1Op("GETFILESTATUS")
var ADL1_CREATE = newADLv1Op("CREATE").
	Param("overwrite", "true").
	Header("Expect", "100-continue")
var ADL1_MKDIRS = newADLv1Op("MKDIRS")

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
		},
		ResponseLogHook: func(_ retryablehttp.Logger, r *http.Response) {
			s3Log.Debugf("%v %v %v", r.Request.Method, r.Request.URL, r.Status)
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

func NewADLv1(fs *Goofys, bucket string, flags *FlagStorage) *ADLv1 {
	conf := clientcredentials.Config{
		ClientID:       flags.ADLv1ClientID,
		ClientSecret:   flags.ADLv1ClientCredential,
		TokenURL:       fmt.Sprintf("https://login.microsoftonline.com/%v/oauth2/token", flags.ADLv1TenantID),
		EndpointParams: url.Values{"resource": {"https://management.core.windows.net/"}},
	}
	conf.AuthStyle = oauth2.AuthStyleInParams

	endpoint, err := url.Parse(flags.Endpoint)
	if err != nil {
		return nil
	}

	endpoint.Path = "/webhdfs/v1/"

	_ = logrus.DebugLevel
	//s3Log.Level = logrus.DebugLevel

	ctx := context.WithValue(context.TODO(), oauth2.HTTPClient,
		&http.Client{
			Transport: RetryClient{retryableHttpClient(&http.Client{}, true)},
		})
	tokenSource := oauth2.ReuseTokenSource(nil, conf.TokenSource(ctx))
	oauth2Client := oauth2.NewClient(context.TODO(), tokenSource)

	return &ADLv1{
		fs:       fs,
		flags:    flags,
		client:   retryableHttpClient(oauth2Client, false),
		endpoint: *endpoint,
		bucket:   bucket,
		cap: Capabilities{
			NoParallelMultipart: true,
			DirBlob:             true,
		},
	}
}

func toOauth2Error(err error) *oauth2.RetrieveError {
	if urlErr, ok := err.(*url.Error); ok {
		if oauth2Err, ok := urlErr.Err.(*oauth2.RetrieveError); ok {
			return oauth2Err
		}
	}
	return nil
}

func mapADLv1Error(resp *http.Response, err error) error {
	if err != nil {
		oauth2Err := toOauth2Error(err)
		if oauth2Err != nil {
			err = mapHttpError(oauth2Err.Response.StatusCode)
			if err != nil {
				return err
			}
		}
		return syscall.EAGAIN
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if resp.StatusCode == http.StatusTemporaryRedirect {
			return nil
		}
		err = mapHttpError(resp.StatusCode)
		if err != nil {
			return err
		} else {
			op := resp.Request.URL.Query().Get("op")
			s3Log.Errorf("adlv1 %v %v", op, resp.Status)
			return syscall.EINVAL
		}
	}

	return nil
}

func (b *ADLv1) get(op ADLv1Op, path string, res interface{}) error {
	return b.call("GET", op, path, nil, res)
}

func (b *ADLv1) call(method string, op ADLv1Op, path string, arg interface{}, res interface{}) error {
	endpoint := b.endpoint
	path = strings.TrimLeft(path, "/")
	if b.bucket != "" {
		path = b.bucket + "/" + path
	}

	endpoint.Path += path
	endpoint.RawQuery = op.params.Encode()

	var body io.ReadSeeker
	var err error

	if arg != nil {
		if r, ok := arg.(io.ReadSeeker); ok {
			body = r
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

	req, err := retryablehttp.NewRequest(method, endpoint.String(), body)
	if err != nil {
		s3Log.Errorf("NewRequest error: %v", err)
		return fuse.EINVAL
	}

	req.Header = op.headers
	var resp *http.Response

	for true {
		resp, err = b.client.Do(req)
		err = mapADLv1Error(resp, err)
		if err != nil {
			if err == fuse.EINVAL && resp != nil {
				body, bodyErr := ioutil.ReadAll(resp.Body)
				if bodyErr != nil {
					return bodyErr
				}

				s3Log.Errorf("%v", string(body))
			}
			return err
		}
		if resp.StatusCode == http.StatusTemporaryRedirect {
			location, err := resp.Location()
			if err != nil {
				s3Log.Errorf("redirect from %v but no Location header", endpoint)
				return syscall.EAGAIN
			}
			s3Log.Debugf("redirect %v", location)
			req, err = retryablehttp.NewRequest(method, location.String(), body)
			if err != nil {
				s3Log.Errorf("NewRequest error: %v", err)
				return fuse.EINVAL
			}
		} else {
			break
		}
	}

	if res != nil {
		decoder := json.NewDecoder(resp.Body)
		err = decoder.Decode(res)
		if err != nil {
			log.Errorf("adlv1 api %v decode error: %v", op, err)
		}
	}

	return err
}

func (b *ADLv1) Init() error {
	randomObjectName := b.fs.key(RandStringBytesMaskImprSrc(32))

	_, err := b.HeadBlob(&HeadBlobInput{Key: *randomObjectName})
	if err != nil {
		if err == fuse.ENOENT {
			err = nil
		}
	}

	return err
}

func (b *ADLv1) Capabilities() *Capabilities {
	return &b.cap
}

func (b *ADLv1) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	type FileStatus struct {
		FileStatus hdfs.FileStatus
	}
	res := FileStatus{}
	err := b.get(ADL1_GETFILESTATUS, param.Key, &res)
	if err != nil {
		return nil, err
	}

	f := res.FileStatus

	return &HeadBlobOutput{
		BlobItemOutput: BlobItemOutput{
			Key:          &param.Key,
			LastModified: PTime(time.Unix(f.ModificationTime/1000, f.ModificationTime%1000000)),
			Size:         uint64(f.Length),
			StorageClass: PString(strconv.FormatInt(f.Replication, 10)),
		},
		IsDirBlob: f.Type == "DIRECTORY",
	}, nil
}

func (b *ADLv1) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *ADLv1) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *ADLv1) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *ADLv1) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *ADLv1) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *ADLv1) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *ADLv1) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	if param.DirBlob {
		// ADLv1 creates a "dir" blob if we create "dir/" and subsequently
		// disallow "dir/file" to be created
		err := b.mkdir(param.Key)
		if err != nil {
			return nil, err
		}
	} else {
		create := ADL1_CREATE.New().Perm(b.flags.FileMode)
		if param.ContentType != nil {
			create.Header("Content-Type", *param.ContentType)
		}
		err := b.call("PUT", create, param.Key, param.Body, nil)
		if err != nil {
			return nil, err
		}
	}

	return &PutBlobOutput{}, nil
}

func (b *ADLv1) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	return nil, syscall.ENOTSUP
}

func (b *ADLv1) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *ADLv1) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *ADLv1) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *ADLv1) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *ADLv1) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error) {
	return nil, syscall.ENOTSUP
}

func (b *ADLv1) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error) {
	if b.bucket == "" {
		return nil, fuse.EINVAL
	}

	err := b.mkdir(b.bucket)
	if err != nil {
		return nil, err
	}

	return &MakeBucketOutput{}, nil
}

func (b *ADLv1) mkdir(dir string) error {
	type Mkdirs struct {
		boolean bool
	}

	res := Mkdirs{}

	return b.call("PUT", ADL1_MKDIRS.New().Perm(b.flags.DirMode), dir, nil, &res)
}
