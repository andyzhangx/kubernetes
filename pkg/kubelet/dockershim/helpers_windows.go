// +build windows

/*
Copyright 2015 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package dockershim

import (
	"fmt"
	"os"

	"github.com/blang/semver"
	dockertypes "github.com/docker/docker/api/types"
	dockercontainer "github.com/docker/docker/api/types/container"
	dockerfilters "github.com/docker/docker/api/types/filters"
	"github.com/golang/glog"
	runtimeapi "k8s.io/kubernetes/pkg/kubelet/apis/cri/v1alpha1/runtime"
)

func DefaultMemorySwap() int64 {
	return 0
}

func (ds *dockerService) getSecurityOpts(seccompProfile string, separator rune) ([]string, error) {
	if seccompProfile != "" {
		glog.Warningf("seccomp annotations are not supported on windows")
	}
	return nil, nil
}

func (ds *dockerService) updateCreateConfig(
	createConfig *dockertypes.ContainerCreateConfig,
	config *runtimeapi.ContainerConfig,
	sandboxConfig *runtimeapi.PodSandboxConfig,
	podSandboxID string, securityOptSep rune, apiVersion *semver.Version) error {
	if networkMode := os.Getenv("CONTAINER_NETWORK"); networkMode != "" {
		createConfig.HostConfig.NetworkMode = dockercontainer.NetworkMode(networkMode)
	}

	return nil
}

func (ds *dockerService) determinePodIPBySandboxID(sandboxID string) string {
	opts := dockertypes.ContainerListOptions{
		All:     true,
		Filters: dockerfilters.NewArgs(),
	}

	f := newDockerFilter(&opts.Filters)
	f.AddLabel(containerTypeLabelKey, containerTypeLabelContainer)
	f.AddLabel(sandboxIDLabelKey, sandboxID)
	containers, err := ds.client.ListContainers(opts)
	if err != nil {
		return ""
	}

	for _, c := range containers {
		r, err := ds.client.InspectContainer(c.ID)
		if err != nil {
			continue
		}
		if containerIP := getContainerIP(r); containerIP != "" {
			return containerIP
		}
	}

	return ""
}

// Configure Infra Networking post Container Creation, before the container starts
func (ds *dockerService) configureInfraContainerNetworkConfig(containerID string) {
	// Attach a second Nat network endpoint to the container to allow outbound internet traffic
	netMode := os.Getenv("NAT_NETWORK")
	if netMode == "" {
		netMode = "nat"
	}
	ds.client.ConnectNetwork(netMode, containerID, nil)
}

// Configure Infra Networking post Container Creation, after the container starts
func (ds *dockerService) FinalizeInfraContainerNetwork(containerID string, DNS string) {
	podGW := os.Getenv("POD_GW")
	vipCidr := os.Getenv("VIP_CIDR")

	// Execute the below inside the container
	// Remove duplicate default gateway (0.0.0.0/0) because of 2 network endpoints
	// Add a route to the Vip CIDR via the POD CIDR transparent network
	pscmd := fmt.Sprintf("$ifIndex=(get-netroute -NextHop %s).IfIndex;", podGW) +
		fmt.Sprintf("netsh interface ipv4 delete route 0.0.0.0/0 $ifIndex %s;", podGW) +
		fmt.Sprintf("netsh interface ipv4 add route %s $ifIndex %s;", vipCidr, podGW)
	if DNS != "" {
		pscmd += fmt.Sprintf("Get-NetAdapter | foreach { netsh interface ipv4 set dns $_.ifIndex static none };")
		pscmd += fmt.Sprintf("netsh interface ipv4 set dns $ifIndex static %s;", DNS)
	}

	cmd := []string{
		"powershell.exe",
		"-command",
		pscmd,
	}

	ds.ExecSync(containerID, cmd, 30)
}

func getContainerIP(container *dockertypes.ContainerJSON) string {
	ipFound := ""
	containerNetworkName := os.Getenv("CONTAINER_NETWORK")
	if container.NetworkSettings != nil {
		for name, network := range container.NetworkSettings.Networks {
			if network.IPAddress != "" {
				ipFound = network.IPAddress
				if name == containerNetworkName {
					return network.IPAddress
				}
			}
		}
	}

	return ipFound
}
