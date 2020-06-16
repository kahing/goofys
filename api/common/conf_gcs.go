package common

import (
	"context"
	"golang.org/x/oauth2/google"
)

type GCSConfig struct {
	Credentials *google.Credentials
	ProjectId   string
	Bucket      string
	Prefix      string

	// TODO: Scope can be added to the config to limit user's access to the bucket
}

func NewGCSConfig(projectId string, bucket string, prefix string) (*GCSConfig, error) {
	ctx := context.Background()

	// Currently, we only allow authenticated user to use goofys for GCS
	credentials, err := google.FindDefaultCredentials(ctx)

	if err != nil {
		return nil, err
	}

	if projectId == "" {
		projectId = credentials.ProjectID
	}

	return &GCSConfig{Credentials: credentials, Bucket: bucket, Prefix: prefix, ProjectId: projectId}, nil
}
