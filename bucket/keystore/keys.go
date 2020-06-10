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

package keystore

import (
	"bytes"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/retailnext/cassandrabackup/digest"
	"github.com/retailnext/cassandrabackup/manifests"
	"github.com/retailnext/cassandrabackup/unixtime"
)

type KeyStore struct {
	Bucket string
	Prefix string
}

func NewKeyStore(bucket, prefix string) KeyStore {
	return KeyStore{bucket, prefix}
}

func (c *KeyStore) keyWithPrefix(key string) string {
	if c.Prefix == "" {
		return key
	}
	var buffer bytes.Buffer
	buffer.WriteString(c.Prefix)
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

func (c *KeyStore) AbsoluteKeyPrefixForClusters() string {
	return c.keyWithPrefix("manifests/")
}

func (c *KeyStore) AbsoluteKeyPrefixForClusterHosts(cluster string) string {
	if cluster == "" {
		panic("empty cluster")
	}
	urlCluster := base64.URLEncoding.EncodeToString([]byte(cluster))
	clustersPrefix := c.AbsoluteKeyPrefixForClusters()
	return fmt.Sprintf("%s%s/", clustersPrefix, urlCluster)
}

func (c *KeyStore) DecodeCluster(key string) (string, error) {
	clustersPrefix := c.AbsoluteKeyPrefixForClusters()
	urlCluster := strings.TrimSuffix(strings.TrimPrefix(key, clustersPrefix), "/")
	cluster, err := base64.URLEncoding.DecodeString(urlCluster)
	return string(cluster), err
}

func (c *KeyStore) AbsoluteKeyPrefixForManifests(identity manifests.NodeIdentity) string {
	if identity.Hostname == "" {
		panic("empty Hostname")
	}
	clusterPrefix := c.AbsoluteKeyPrefixForClusterHosts(identity.Cluster)
	urlHostname := base64.URLEncoding.EncodeToString([]byte(identity.Hostname))
	return fmt.Sprintf("%s%s/", clusterPrefix, urlHostname)
}

func (c *KeyStore) AbsoluteKeyForManifestTimeRange(identity manifests.NodeIdentity, boundary unixtime.Seconds) string {
	return c.AbsoluteKeyPrefixForManifests(identity) + boundary.Decimal()
}

func (c *KeyStore) AbsoluteKeyForManifest(identity manifests.NodeIdentity, manifestKey manifests.ManifestKey) string {
	return c.AbsoluteKeyPrefixForManifests(identity) + manifestKey.FileName()
}

func (c *KeyStore) NodeIdentityFromKey(key string) (manifests.NodeIdentity, error) {
	clustersPrefix := c.AbsoluteKeyPrefixForClusters()
	trimmed := strings.TrimSuffix(strings.TrimPrefix(key, clustersPrefix), "/")
	parts := strings.Split(trimmed, "/")
	if len(parts) != 2 {
		return manifests.NodeIdentity{}, errors.New("Invalid number of parts")
	}
	cluster, err := base64.URLEncoding.DecodeString(parts[0])
	if err != nil {
		return manifests.NodeIdentity{}, err
	}
	hostname, err := base64.URLEncoding.DecodeString(parts[1])
	if err != nil {
		return manifests.NodeIdentity{}, err

	}
	return manifests.NodeIdentity{
		Cluster:  string(cluster),
		Hostname: string(hostname),
	}, nil
}
