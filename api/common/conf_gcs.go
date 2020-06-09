package common

import (
	"context"
	"golang.org/x/oauth2/google"
)

type GCSConfig struct {
	Credentials *google.Credentials
	Bucket string
	Prefix string
	//Scope
}

func NewGCSConfig() (*GCSConfig, error){
	ctx := context.Background()

	// v0.1: only allows authenticated user and read only acccess
	//credentials, err := google.FindDefaultCredentials(ctx)
	credentials, err := google.FindDefaultCredentials(ctx)

	if err != nil {
		return nil, err
	}

	return &GCSConfig{Credentials: credentials, Bucket: "", Prefix: ""}, nil
}

