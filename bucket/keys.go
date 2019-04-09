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
	"cassandrabackup/digest"
	"cassandrabackup/manifests"
	"cassandrabackup/unixtime"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3"
)

func (c *Client) keyWithPrefix(key string) string {
	if c.prefix == "" {
		return key
	}
	return fmt.Sprintf("%s/%s", c.prefix, key)
}

func (c *Client) absoluteKeyForBlob(digests digest.ForRestore) string {
	encoded := digests.URLSafe()
	return c.keyWithPrefix(fmt.Sprintf("files/blake2b/%s/%s/%s", encoded[0:1], encoded[1:2], encoded[2:]))
}

func (c *Client) absolteKeyPrefixForClusters() string {
	return c.keyWithPrefix("manifests/")
}

func (c *Client) absoluteKeyPrefixForClusterHosts(cluster string) string {
	if cluster == "" {
		panic("empty cluster")
	}
	urlCluster := base64.URLEncoding.EncodeToString([]byte(cluster))
	clustersPrefix := c.absolteKeyPrefixForClusters()
	return fmt.Sprintf("%s%s/", clustersPrefix, urlCluster)
}

func (c *Client) decodeClusterHosts(prefixes []*s3.CommonPrefix) ([]manifests.NodeIdentity, []string) {
	result := make([]manifests.NodeIdentity, 0, len(prefixes))
	var bonus []string
	skip := len(c.absolteKeyPrefixForClusters())
	for _, obj := range prefixes {
		raw := *obj.Prefix
		trimmed := raw[skip:]
		parts := strings.Split(trimmed, "/")
		if len(parts) != 3 {
			bonus = append(bonus, raw)
			continue
		}
		cluster, err := base64.URLEncoding.DecodeString(parts[0])
		if err != nil {
			bonus = append(bonus, raw)
			continue
		}
		hostname, err := base64.URLEncoding.DecodeString(parts[1])
		if err != nil {
			bonus = append(bonus, raw)
			continue
		}
		ni := manifests.NodeIdentity{
			Cluster:  string(cluster),
			Hostname: string(hostname),
		}
		result = append(result, ni)
	}
	return result, bonus
}

func (c *Client) absoluteKeyPrefixForManifests(identity manifests.NodeIdentity) string {
	if identity.Hostname == "" {
		panic("empty Hostname")
	}
	clusterPrefix := c.absoluteKeyPrefixForClusterHosts(identity.Cluster)
	urlHostname := base64.URLEncoding.EncodeToString([]byte(identity.Hostname))
	return fmt.Sprintf("%s%s/", clusterPrefix, urlHostname)
}

func (c *Client) absoluteKeyForManifestTimeRange(identity manifests.NodeIdentity, boundary unixtime.Seconds) string {
	return c.absoluteKeyPrefixForManifests(identity) + boundary.Decimal()
}

func (c *Client) absoluteKeyForManifest(identity manifests.NodeIdentity, manifestKey manifests.ManifestKey) string {
	return c.absoluteKeyPrefixForManifests(identity) + manifestKey.FileName()
}
