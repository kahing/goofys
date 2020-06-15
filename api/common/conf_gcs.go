package common

import (
	"context"
	"golang.org/x/oauth2/google"
)

type GCSConfig struct {
	Credentials *google.Credentials
	Bucket      string
	Prefix      string

	// TODO: Scope can be added to the config to limit user's access to the bucket
}

func NewGCSConfig(bucket string, prefix string) (*GCSConfig, error) {
	ctx := context.Background()

	// Currently, we only allow authenticated user to use goofys for GCS
	credentials, err := google.FindDefaultCredentials(ctx)

	if err != nil {
		return nil, err
	}

	return &GCSConfig{Credentials: credentials, Bucket: bucket, Prefix: prefix}, nil
}
