// Copyright 2019 RetailNext, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package safeuploader

import (
	"cassandrabackup/digest"
	"cassandrabackup/paranoid"
	"context"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"go.uber.org/zap"
)

type SafeUploader struct {
	S3                   s3iface.S3API
	Bucket               string
	ServerSideEncryption *string
	StorageClass         *string
}

func (u *SafeUploader) UploadFile(ctx context.Context, key string, file paranoid.File, digests digest.ForUpload) error {
	upl := fileUploader{
		s3Svc: u.S3,

		bucket:               u.Bucket,
		key:                  key,
		serverSideEncryption: u.ServerSideEncryption,
		storageClass:         u.StorageClass,

		file:    file,
		digests: digests,

		errors: make(map[int64]error),
		etags:  make(map[int64]string),
	}
	return upl.Upload(ctx)
}

type fileUploader struct {
	s3Svc s3iface.S3API

	bucket               string
	key                  string
	serverSideEncryption *string
	storageClass         *string

	file    paranoid.File
	osFile  *os.File
	digests digest.ForUpload

	ctx       context.Context
	ctxCancel context.CancelFunc

	wg      sync.WaitGroup
	limiter chan struct{}

	lock     sync.Mutex
	errors   map[int64]error
	etags    map[int64]string
	uploadId string
}

func (u *fileUploader) Upload(ctx context.Context) error {
	if osFile, err := u.file.Open(); err != nil {
		return err
	} else {
		u.osFile = osFile
	}
	defer func() {
		if closeErr := u.osFile.Close(); closeErr != nil {
			panic(closeErr)
		}
	}()

	if u.digests.Parts() == 1 {
		return u.uploadSinglePart(ctx)
	}

	createMultipartUploadInput := s3.CreateMultipartUploadInput{
		Bucket:               &u.bucket,
		Key:                  &u.key,
		ServerSideEncryption: u.serverSideEncryption,
		StorageClass:         u.storageClass,
	}
	u.ctx, u.ctxCancel = context.WithCancel(ctx)

	var err error
	var createMultipartUploadOutput *s3.CreateMultipartUploadOutput
	createMultipartUploadOutput, err = u.s3Svc.CreateMultipartUploadWithContext(u.ctx, &createMultipartUploadInput)
	if err != nil {
		return err
	}
	u.uploadId = *createMultipartUploadOutput.UploadId
	defer func() {
		if err != nil {
			u.abort()
		}
	}()

	u.limiter = make(chan struct{}, 4)
	u.errors = make(map[int64]error)
	doneCh := u.ctx.Done()
	var partNumber int64
	for partNumber = 1; partNumber <= u.digests.Parts(); partNumber++ {
		select {
		case <-doneCh:
			break
		case u.limiter <- struct{}{}:
			u.wg.Add(1)
			go u.uploadPart(partNumber)
		}
	}
	u.wg.Wait()

	err = u.tryToComplete()
	return err
}

func (u *fileUploader) tryToComplete() error {
	u.lock.Lock()
	defer u.lock.Unlock()

	var parts []*s3.CompletedPart
	var partNumber int64
	for partNumber = 1; partNumber <= u.digests.Parts(); partNumber++ {
		etag, etagOk := u.etags[partNumber]
		if !etagOk {
			if len(u.errors) == 0 {
				if _, alreadyError := u.errors[partNumber]; !alreadyError {
					u.errors[partNumber] = fmt.Errorf("etag missing")
				}
			}
			continue
		}
		part := &s3.CompletedPart{
			PartNumber: aws.Int64(partNumber),
			ETag:       aws.String(etag),
		}
		parts = append(parts, part)
	}

	if len(u.errors) > 0 {
		return UploadPartFailures(u.errors)
	}

	completeMultipartUploadInput := &s3.CompleteMultipartUploadInput{
		Bucket:   &u.bucket,
		Key:      &u.key,
		UploadId: &u.uploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: parts,
		},
	}
	_, err := u.s3Svc.CompleteMultipartUpload(completeMultipartUploadInput)
	return err
}

func (u *fileUploader) abort() {
	lgr := zap.S()
	input := s3.AbortMultipartUploadInput{
		Bucket:   &u.bucket,
		Key:      &u.key,
		UploadId: &u.uploadId,
	}
	_, err := u.s3Svc.AbortMultipartUpload(&input)
	if err != nil {
		lgr.Errorw("abort_multipart_upload_error", "key", u.key, "err", err)
	} else {
		lgr.Infow("abort_multipart_upload_ok", "key", u.key)
	}
}

func (u *fileUploader) uploadPart(partNumber int64) {
	var err error

	defer func() {
		if err != nil {
			u.lock.Lock()
			u.errors[partNumber] = err
			u.ctxCancel()
			u.lock.Unlock()
		}
		<-u.limiter
		u.wg.Done()
	}()

	offset := u.digests.PartOffset(partNumber)
	length := u.digests.PartLength(partNumber)
	reader := io.NewSectionReader(u.osFile, offset, length)

	uploadPartInput := &s3.UploadPartInput{
		Bucket:        &u.bucket,
		Key:           &u.key,
		UploadId:      &u.uploadId,
		PartNumber:    &partNumber,
		ContentLength: &length,
		Body:          reader,
	}
	var uploadPartOutput *s3.UploadPartOutput
	uploadPartOutput, err = u.s3Svc.UploadPartWithContext(u.ctx, uploadPartInput, func(request *request.Request) {
		request.HTTPRequest.Header.Set(md5Header, u.digests.PartContentMD5(partNumber))
		request.HTTPRequest.Header.Set(sha256Header, u.digests.PartContentSHA256(partNumber))
	})
	if err != nil {
		return
	}

	u.lock.Lock()
	u.etags[partNumber] = *uploadPartOutput.ETag
	u.lock.Unlock()
}

func (u *fileUploader) uploadSinglePart(ctx context.Context) error {
	putObjectInput := s3.PutObjectInput{
		Bucket:               &u.bucket,
		Key:                  &u.key,
		ContentLength:        aws.Int64(u.digests.PartLength(1)),
		ServerSideEncryption: u.serverSideEncryption,
		StorageClass:         u.storageClass,
		Body:                 u.osFile,
	}
	_, err := u.s3Svc.PutObjectWithContext(ctx, &putObjectInput, func(i *request.Request) {
		i.HTTPRequest.Header.Set(md5Header, u.digests.PartContentMD5(1))
		i.HTTPRequest.Header.Set(sha256Header, u.digests.PartContentSHA256(1))
	})
	return err
}

const md5Header = "Content-Md5"
const sha256Header = "X-Amz-Content-Sha256"

type UploadPartFailures map[int64]error

func (e UploadPartFailures) Error() string {
	return fmt.Sprintf("%d parts failed to upload", len(e))
}
