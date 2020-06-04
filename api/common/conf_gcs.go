package common

import "golang.org/x/oauth2/google"

type GCSConfig struct{
	Credentials *google.Credentials
}