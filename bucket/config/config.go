package config

import (
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
	ProviderAWS                       = "aws"
	ProviderGoogle                    = "google"
	CloudProviderAWS    CloudProvider = 1
	CloudProviderGoogle CloudProvider = 2
)

type Config struct {
	Provider        CloudProvider
	BucketName      string
	BucketKeyPrefix string
	S3BucketRegion  string
	S3StorageClass  string
	SharedCacheFile string
}

type CloudProvider byte

func (c CloudProvider) Valid() bool {
	if c == 0 {
		panic("provider not specified")
	}
	if c != CloudProviderAWS && c != CloudProviderGoogle {
		return false
	}
	return true
}

func (c CloudProvider) IsAWS() bool {
	return c.Valid() && c == CloudProviderAWS
}

func (c CloudProvider) IsGCS() bool {
	return c.Valid() && c == CloudProviderGoogle
}

func Provider(provider string) CloudProvider {
	switch provider {
	case ProviderAWS:
		return CloudProviderAWS
	case ProviderGoogle:
		return CloudProviderGoogle
	default:
		panic("unknown provider:" + provider)
	}
}

func (c Config) IsAWS() bool {
	return c.Provider.IsAWS()
}

func (c Config) IsGCS() bool {
	return c.Provider.IsGCS()
}
