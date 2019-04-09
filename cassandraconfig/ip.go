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

package cassandraconfig

import (
	"net"
	"os"
)

func (r Raw) IPForPeers() string {
	if r.BroadcastAddress != "" {
		return r.BroadcastAddress
	}
	if r.ListenAddress != "" {
		return r.ListenAddress
	}
	if r.ListenInterface != "" {
		if i := ipForInterface(r.ListenInterface); i != "" {
			return i
		}
	}
	return compatDefaultIP()
}

func (r Raw) IPForClients() string {
	if r.BroadcastRPCAddress != "" {
		return r.BroadcastRPCAddress
	}
	if r.BroadcastAddress != "" {
		return r.BroadcastAddress
	}
	if r.RPCAddress != "" {
		return r.RPCAddress
	}
	if r.RPCInterface != "" {
		if i := ipForInterface(r.RPCInterface); i != "" {
			return i
		}
	}
	return compatDefaultIP()
}

func compatDefaultIP() string {
	name, _ := os.Hostname()
	addr, _ := net.ResolveIPAddr("ip", name)
	if addr != nil && len(addr.IP) > 0 && !addr.IP.IsUnspecified() {
		return addr.IP.String()
	}
	addrs, _ := net.InterfaceAddrs()
	if ip := suitableIP(addrs); ip != "" {
		return ip
	}
	return "127.0.0.1"
}

func ipForInterface(name string) string {
	i, err := net.InterfaceByName(name)
	if err != nil {
		return ""
	}
	addrs, err := i.Addrs()
	if err != nil {
		return ""
	}
	return suitableIP(addrs)
}

func suitableIP(addrs []net.Addr) string {
	for _, a := range addrs {
		if addr, ok := a.(*net.IPNet); ok && addr.IP.IsGlobalUnicast() {
			if v4 := addr.IP.To4(); v4 != nil {
				return v4.String()
			}
		}
	}
	for _, a := range addrs {
		if addr, ok := a.(*net.IPNet); ok && addr.IP.IsGlobalUnicast() {
			return addr.IP.String()
		}
	}
	for _, a := range addrs {
		if addr, ok := a.(*net.IPNet); ok && len(addr.IP) != 0 {
			return addr.IP.String()
		}
	}
	return ""
}
