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

package common

import (
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
)

type S3Config struct {
	Region        string
	AccessKey     string
	SecretKey     string
	RequesterPays bool
	RegionSet     bool
	StorageClass  string
	Profile       string
	UseSSE        bool
	UseKMS        bool
	KMSKeyID      string
	ACL           string
	Subdomain     bool
}

var s3HTTPTransport = http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}).DialContext,
	MaxIdleConns:          1000,
	MaxIdleConnsPerHost:   1000,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 10 * time.Second,
}

func (c *S3Config) Init() *S3Config {
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	if c.StorageClass == "" {
		c.StorageClass = "STANDARD"
	}
	return c
}

func (c *S3Config) ToAwsConfig(flags *FlagStorage) *aws.Config {
	awsConfig := (&aws.Config{
		Region: &c.Region,
		Logger: GetLogger("s3"),
	}).WithHTTPClient(&http.Client{
		Transport: &s3HTTPTransport,
		Timeout:   flags.HTTPTimeout,
	})
	if flags.DebugS3 {
		awsConfig.LogLevel = aws.LogLevel(aws.LogDebug | aws.LogDebugWithRequestErrors)
	}

	if c.AccessKey != "" {
		awsConfig.Credentials = credentials.NewStaticCredentials(c.AccessKey, c.SecretKey, "")
	} else if c.Profile != "" {
		awsConfig.Credentials = credentials.NewSharedCredentials("", c.Profile)
	}

	if flags.Endpoint != "" {
		awsConfig.Endpoint = &flags.Endpoint
	}

	awsConfig.S3ForcePathStyle = aws.Bool(!c.Subdomain)

	return awsConfig
}
