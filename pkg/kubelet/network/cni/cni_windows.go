// +build windows

/*
Copyright 2014 The Kubernetes Authors.

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

package cni

import (
	"fmt"
	cniTypes020 "github.com/containernetworking/cni/pkg/types/020"
	"github.com/golang/glog"
	"k8s.io/kubernetes/pkg/apis/componentconfig"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/network"
)

func (plugin *cniNetworkPlugin) Init(host network.Host, hairpinMode componentconfig.HairpinMode, nonMasqueradeCIDR string, mtu int) error {

	plugin.host = host

	plugin.syncNetworkConfig()
	return nil
}

func getLoNetwork(binDir, vendorDirPrefix string) *cniNetwork {
	return nil
}

// GetPodNetworkStatus : Assuming addToNetwork is idempotent, we can call this API as many times as required to get the IPAddress
func (plugin *cniNetworkPlugin) GetPodNetworkStatus(namespace string, name string, id kubecontainer.ContainerID) (*network.PodNetworkStatus, error) {
	netnsPath, err := plugin.host.GetNetNS(id.ID)
	if err != nil {
		return nil, fmt.Errorf("CNI failed to retrieve network namespace path: %v", err)
	}

	result, err := plugin.addToNetwork(plugin.getDefaultNetwork(), name, namespace, id, netnsPath)

	glog.V(5).Infof("GetPodNetworkStatus result %+v", result)
	if err != nil {
		glog.Errorf("Error while adding to cni network: %s", err)
		return nil, err
	}
	var result020 *cniTypes020.Result
	result020, err = cniTypes020.GetResult(result)
	if err != nil {
		glog.Errorf("Error while cni parsing result: %s", err)
		return nil, err
	}
	// Parse the result and get the IPAddress
	return &network.PodNetworkStatus{IP: result020.IP4.IP.IP}, nil
}
