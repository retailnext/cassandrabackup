// Copyright 2020 RetailNext, Inc.
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
	"bytes"
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/manifests"
	"github.com/retailnext/cassandrabackup/unixtime"
)

type KeyStore struct {
	bucket string
	prefix string
}

func newKeyStore(bucket, prefix string) KeyStore {
	return KeyStore{bucket, prefix}
}

func (c *KeyStore) keyWithPrefix(key string) string {
	if c.prefix == "" {
		return key
	}
	var buffer bytes.Buffer
	buffer.WriteString(c.prefix)
	buffer.WriteString("/")
	buffer.WriteString(key)
	return buffer.String()
}

func (c *KeyStore) AbsoluteKeyForBlob(digests digest.ForRestore) string {
	var buffer bytes.Buffer
	encoded := digests.URLSafe()
	buffer.WriteString("files/blake2b/")
	buffer.WriteString(encoded[0:1])
	buffer.WriteString("/")
	buffer.WriteString(encoded[1:2])
	buffer.WriteString("/")
	buffer.WriteString(encoded[2:])
	return c.keyWithPrefix(buffer.String())
}

func (c *KeyStore) DecodeBlobKey(key string) (digest.ForRestore, error) {
	var digests digest.ForRestore
	var buffer bytes.Buffer
	blobPrefix := c.keyWithPrefix("files/blake2b/")
	encoded := strings.TrimPrefix(key, blobPrefix)
	buffer.WriteString(encoded[0:1])
	buffer.WriteString(encoded[2:3])
	buffer.WriteString(encoded[4:])
	binary, err := base64.URLEncoding.DecodeString(buffer.String())
	if err != nil {
		return digests, err
	}
	err = digests.UnmarshalBinary(binary)
	return digests, err
}

func (c *KeyStore) absoluteKeyPrefixForClusters() string {
	return c.keyWithPrefix("manifests/")
}

func (c *KeyStore) absoluteKeyPrefixForClusterHosts(cluster string) string {
	if cluster == "" {
		panic("empty cluster")
	}
	urlCluster := base64.URLEncoding.EncodeToString([]byte(cluster))
	clustersPrefix := c.absoluteKeyPrefixForClusters()
	return fmt.Sprintf("%s%s/", clustersPrefix, urlCluster)
}

func (c *KeyStore) decodeCluster(key string) (string, error) {
	clustersPrefix := c.absoluteKeyPrefixForClusters()
	urlCluster := strings.TrimSuffix(strings.TrimPrefix(key, clustersPrefix), "/")
	cluster, err := base64.URLEncoding.DecodeString(urlCluster)
	return string(cluster), err
}

func (c *KeyStore) decodeClusterHosts(prefixes []*s3.CommonPrefix) ([]manifests.NodeIdentity, []string) {
	result := make([]manifests.NodeIdentity, 0, len(prefixes))
	var bonus []string
	skip := len(c.absoluteKeyPrefixForClusters())
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

func (c *KeyStore) absoluteKeyPrefixForManifests(identity manifests.NodeIdentity) string {
	if identity.Hostname == "" {
		panic("empty Hostname")
	}
	clusterPrefix := c.absoluteKeyPrefixForClusterHosts(identity.Cluster)
	urlHostname := base64.URLEncoding.EncodeToString([]byte(identity.Hostname))
	return fmt.Sprintf("%s%s/", clusterPrefix, urlHostname)
}

func (c *KeyStore) absoluteKeyForManifestTimeRange(identity manifests.NodeIdentity, boundary unixtime.Seconds) string {
	return c.absoluteKeyPrefixForManifests(identity) + boundary.Decimal()
}

func (c *KeyStore) AbsoluteKeyForManifest(identity manifests.NodeIdentity, manifestKey manifests.ManifestKey) string {
	return c.absoluteKeyPrefixForManifests(identity) + manifestKey.FileName()
}
