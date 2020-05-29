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

package bucket

import (
	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/retailnext/cassandrabackup/manifests"
	"go.uber.org/zap"
)

func (c *Client) ListHostNames(ctx context.Context, cluster string) ([]manifests.NodeIdentity, error) {
	lgr := zap.S()
	prefix := c.absoluteKeyPrefixForClusterHosts(cluster)
	input := &s3.ListObjectsV2Input{
		Bucket:    &c.bucket,
		Delimiter: aws.String("/"),
		Prefix:    &prefix,
	}
	var result []manifests.NodeIdentity
	err := c.s3Svc.ListObjectsV2PagesWithContext(ctx, input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		nodes, bonus := c.decodeClusterHosts(page.CommonPrefixes)
		if len(bonus) > 0 {
			lgr.Warnw("unexpected_objects_in_bucket", "keys", bonus)
		}
		result = append(result, nodes...)
		if len(page.Contents) > 0 {
			unexpected := make([]string, 0, len(page.Contents))
			for _, o := range page.Contents {
				unexpected = append(unexpected, *o.Key)
			}
			lgr.Warnw("unexpected_objects_in_bucket", "keys", unexpected)
		}
		return true
	})
	return result, err
}

func (c *Client) ListClusters(ctx context.Context) ([]string, error) {
	lgr := zap.S()
	prefix := c.absoluteKeyPrefixForClusters()
	input := &s3.ListObjectsV2Input{
		Bucket:    &c.bucket,
		Delimiter: aws.String("/"),
		Prefix:    &prefix,
	}
	var result []string
	err := c.s3Svc.ListObjectsV2PagesWithContext(ctx, input, func(page *s3.ListObjectsV2Output, lastPage bool) bool {
		for _, obj := range page.CommonPrefixes {
			cluster, err := c.decodeCluster(*obj.Prefix)
			if err != nil {
				lgr.Errorw("decode_cluster_error", "err", err)
			} else {
				result = append(result, cluster)
			}
		}
		// Continue to the next page
		return page.NextContinuationToken != nil
	})
	return result, err
}
