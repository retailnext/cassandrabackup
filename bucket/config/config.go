package config

import (
	"errors"
	"time"
)

const (
	PutJsonRetriesLimit       = 3
	GetJsonRetriesLimit       = 3
	GetBlobRetriesLimit       = 3
	ListManifestsRetriesLimit = 3
	RetrySleepPerAttempt      = time.Second
)

const (
	ProviderAWS    = "aws"
	ProviderGoogle = "google"
)

var UploadSkipped = errors.New("upload skipped")

type Config struct {
	Provider        string
	BucketName      string
	BucketRegion    string
	BucketKeyPrefix string
	S3StorageClass  string
	SharedCacheFile string
}

func (c *Config) IsAWS() bool {
	return c.Provider == ProviderAWS
}

func (c *Config) IsGCS() bool {
	return c.Provider == ProviderGoogle
}
