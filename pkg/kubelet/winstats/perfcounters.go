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
	"fmt"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/pkg/kubelet/winstats/win"
	"time"
	"unsafe"
)

const (
	cpuQuery                  = "\\Processor(_Total)\\% Processor Time"
	memoryPrivWorkingSetQuery = "\\Process(_Total)\\Working Set - Private"
	memoryCommittedBytesQuery = "\\Memory\\Committed Bytes"
	perfCounterUpdatePeriod   = 1 * time.Second
)

// Metric collected from a counter
type Metric struct {
	Name  string
	Value float64
}

// NewMetric construct a Metric struct
func NewMetric(name string, value float64) Metric {
	return Metric{
		Name:  name,
		Value: value,
	}
}

func (metric Metric) String() string {
	return fmt.Sprintf(
		"Name: %s | Value: %s",
		metric.Name,
		metric.Value,
	)
}

func readPerformanceCounter(counter string) (chan Metric, error) {

	var queryHandle win.PDH_HQUERY
	var counterHandle win.PDH_HCOUNTER

	ret := win.PdhOpenQuery(0, 0, &queryHandle)
	if ret != win.ERROR_SUCCESS {
		return nil, errors.New("Unable to open query through DLL call")
	}

	// test path
	ret = win.PdhValidatePath(counter)

	if ret != win.ERROR_SUCCESS {
		return nil, fmt.Errorf("Unable to valid path to counter. Error code is %x", ret)
	}

	ret = win.PdhAddEnglishCounter(queryHandle, counter, 0, &counterHandle)
	if ret != win.ERROR_SUCCESS {
		return nil, fmt.Errorf("Unable to add process counter. Error code is %x", ret)
	}

	ret = win.PdhCollectQueryData(queryHandle)
	if ret != win.ERROR_SUCCESS {
		return nil, fmt.Errorf("Unable to collect data from counter. Error code is %x", ret)
	}

	out := make(chan Metric)

	go wait.Forever(func() {
		collectCounterData(queryHandle, counterHandle, counter, out)
	}, perfCounterUpdatePeriod)

	return out, nil
}

func collectCounterData(queryHandle win.PDH_HQUERY, counterHandle win.PDH_HCOUNTER, counter string, ch chan<- Metric) {

	ret := win.PdhCollectQueryData(queryHandle)
	if ret == win.ERROR_SUCCESS {
		var metric Metric
		var bufSize uint32
		var bufCount uint32
		var size = uint32(unsafe.Sizeof(win.PDH_FMT_COUNTERVALUE_ITEM_DOUBLE{}))
		var emptyBuf [1]win.PDH_FMT_COUNTERVALUE_ITEM_DOUBLE // need at least 1 addressable null ptr.

		ret = win.PdhGetFormattedCounterArrayDouble(counterHandle, &bufSize, &bufCount, &emptyBuf[0])
		if ret == win.PDH_MORE_DATA {
			filledBuf := make([]win.PDH_FMT_COUNTERVALUE_ITEM_DOUBLE, bufCount*size)
			ret = win.PdhGetFormattedCounterArrayDouble(counterHandle, &bufSize, &bufCount, &filledBuf[0])
			if ret == win.ERROR_SUCCESS {
				for i := 0; i < int(bufCount); i++ {
					c := filledBuf[i]

					metric = Metric{
						counter,
						c.FmtValue.DoubleValue,
					}
				}
			}
		}
		ch <- metric
	}
}
