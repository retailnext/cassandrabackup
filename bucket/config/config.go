package config

import (
	"errors"
	"time"
)

const PutJsonRetriesLimit = 3
const GetJsonRetriesLimit = 3
const GetBlobRetriesLimit = 3
const ListManifestsRetriesLimit = 3
const RetrySleepPerAttempt = time.Second

type Config struct {
	Provider               string
	BucketName             string
	BucketRegion           string
	BucketKeyPrefix        string
	BucketBlobStorageClass string
	SharedCacheFile        string
}

var UploadSkipped = errors.New("upload skipped")
