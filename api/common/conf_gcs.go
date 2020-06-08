package common

import (
	"cloud.google.com/go/storage"
	"context"
	"golang.org/x/oauth2/google"
)

type GCSConfig struct {
	Credentials *google.Credentials
	Bucket string
	Prefix string
}

func NewGCSConfig() (*GCSConfig, error){
	ctx := context.Background()

	// v0.1: only allows authenticated user
	credentials, err := google.FindDefaultCredentials(ctx, storage.ScopeReadOnly)
	if err != nil {
		return nil, err
	}

	return &GCSConfig{Credentials: credentials, Bucket: "", Prefix: ""}, nil
}

