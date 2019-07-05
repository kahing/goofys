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
	"crypto/md5"
	"encoding/base64"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/credentials/stscreds"
	"github.com/aws/aws-sdk-go/aws/session"
)

type S3Config struct {
	Profile         string
	AccessKey       string
	SecretKey       string
	RoleArn         string
	RoleExternalId  string
	RoleSessionName string
	StsEndpoint     string

	RequesterPays bool
	Region        string
	RegionSet     bool

	StorageClass string

	UseSSE     bool
	UseKMS     bool
	KMSKeyID   string
	SseC       string
	SseCDigest string
	ACL        string

	Subdomain bool

	Credentials *credentials.Credentials
	Session     *session.Session
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

var s3Session *session.Session

func (c *S3Config) Init() *S3Config {
	if c.Region == "" {
		c.Region = "us-east-1"
	}
	if c.StorageClass == "" {
		c.StorageClass = "STANDARD"
	}
	return c
}

func (c *S3Config) ToAwsConfig(flags *FlagStorage) (*aws.Config, error) {
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

	if c.Credentials == nil {
		if c.AccessKey != "" {
			c.Credentials = credentials.NewStaticCredentials(c.AccessKey, c.SecretKey, "")
		}
	}
	if flags.Endpoint != "" {
		awsConfig.Endpoint = &flags.Endpoint
	}

	awsConfig.S3ForcePathStyle = aws.Bool(!c.Subdomain)

	if c.Session == nil {
		if s3Session == nil {
			var err error
			s3Session, err = session.NewSessionWithOptions(session.Options{
				Profile:           c.Profile,
				SharedConfigState: session.SharedConfigEnable,
			})
			if err != nil {
				return nil, err
			}
		}
		c.Session = s3Session
	}

	if c.RoleArn != "" {
		c.Credentials = stscreds.NewCredentials(stsConfigProvider{c}, c.RoleArn,
			func(p *stscreds.AssumeRoleProvider) {
				if c.RoleExternalId != "" {
					p.ExternalID = &c.RoleExternalId
				}
				p.RoleSessionName = c.RoleSessionName
			})
	}

	if c.Credentials != nil {
		awsConfig.Credentials = c.Credentials
	}

	if c.SseC != "" {
		key, err := base64.StdEncoding.DecodeString(c.SseC)
		if err != nil {
			return nil, fmt.Errorf("sse-c is not base64-encoded: %v", err)
		}

		c.SseC = string(key)
		m := md5.Sum(key)
		c.SseCDigest = base64.StdEncoding.EncodeToString(m[:])
	}

	return awsConfig, nil
}

type stsConfigProvider struct {
	*S3Config
}

func (c stsConfigProvider) ClientConfig(serviceName string, cfgs ...*aws.Config) client.Config {
	config := c.Session.ClientConfig(serviceName, cfgs...)
	if c.Credentials != nil {
		config.Config.Credentials = c.Credentials
	}
	if c.StsEndpoint != "" {
		config.Endpoint = c.StsEndpoint
	}

	return config
}
