// +build windows

/*
Copyright 2017 The Kubernetes Authors.

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

package winstats

import (
	"errors"
	"k8s.io/kubernetes/pkg/kubelet/winstats/win"
)

func getPhysicallyInstalledSystemMemoryBytes() (uint64, error) {
	var physicalMemoryKiloBytes uint64

	if ok := win.GetPhysicallyInstalledSystemMemory(&physicalMemoryKiloBytes); !ok {
		return 0, errors.New("Unable to read physical memory")
	}

	return physicalMemoryKiloBytes * 1024, nil // convert kilobytes to bytes
}
