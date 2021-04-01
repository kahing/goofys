package common

import (
	"context"
	"golang.org/x/oauth2/google"
)

type GCSConfig struct {
	Credentials *google.Credentials
	ChunkSize   int // determine the size of chunks when uploading a file to GCS
}

const (
	defaultGCSChunkSize int = 64 * 1024 * 1024
)

// NewGCSConfig returns a GCS Config.
func NewGCSConfig() *GCSConfig {
	// Credentials will be checked when initializing GCS bucket to create an authenticated / unauthenticated client
	// We allow nil credentials for unauthenticated client.
	credentials, err := google.FindDefaultCredentials(context.Background())
	if err == nil {
		// authenticated config
		return &GCSConfig{
			ChunkSize:   defaultGCSChunkSize,
			Credentials: credentials,
		}
	} else {
		log.Debugf("Initializing an unauthenticated config: %v", err)
		// unauthenticated config
		return &GCSConfig{
			ChunkSize: defaultGCSChunkSize,
		}
	}
}
