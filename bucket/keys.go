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

func (c *Client) absoluteKeyPrefixForManifests(identity manifests.NodeIdentity) string {
	if identity.Hostname == "" {
		panic("empty Hostname")
	}
	if identity.Cluster == "" {
		panic("empty cluster")
	}
	urlCluster := base64.URLEncoding.EncodeToString([]byte(identity.Cluster))
	urlHostname := base64.URLEncoding.EncodeToString([]byte(identity.Hostname))
	relative := fmt.Sprintf("manifests/%s/%s/", urlCluster, urlHostname)
	return c.keyWithPrefix(relative)
}

func (c *Client) absoluteKeyForManifestTimeRange(identity manifests.NodeIdentity, boundary unixtime.Seconds) string {
	return c.absoluteKeyPrefixForManifests(identity) + boundary.Decimal()
}

func (c *Client) absoluteKeyForManifest(identity manifests.NodeIdentity, manifestKey manifests.ManifestKey) string {
	return c.absoluteKeyPrefixForManifests(identity) + manifestKey.FileName()
}
