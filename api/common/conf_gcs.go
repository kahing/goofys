package common

import (
	"context"
	"golang.org/x/oauth2/google"
	"time"
)

type GCSConfig struct {
	Credentials *google.Credentials
	ProjectId   string
	Bucket      string
	HTTPTimeout time.Duration

	// TODO: Scope can be added to the config to limit user's access to the bucket
}

func NewGCSConfig(projectId string, bucket string, flags *FlagStorage) (*GCSConfig, error) {
	ctx := context.Background()

	// Currently, we only allow authenticated user to use goofys for GCS
	credentials, err := google.FindDefaultCredentials(ctx)

	if err != nil {
		return nil, err
	}

	if projectId == "" {
		projectId = credentials.ProjectID
	}

	gcsConfig := &GCSConfig{Credentials: credentials, Bucket: bucket, ProjectId: projectId}
	if flags != nil {
		gcsConfig.HTTPTimeout = flags.HTTPTimeout
	}

	return gcsConfig, nil
}
