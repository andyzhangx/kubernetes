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

// Package winstats provides a client to get node and pod level stats on windows
package winstats

import (
	"context"
	"encoding/json"
	dockerapi "github.com/docker/engine-api/client"
	dockertypes "github.com/docker/engine-api/types"
	cadvisorapi "github.com/google/cadvisor/info/v1"
	cadvisorapiv2 "github.com/google/cadvisor/info/v2"
	"k8s.io/kubernetes/pkg/kubelet/network"
	"os"
	"runtime"
	"sync"
	"time"
)

// Client is an object that is used to get stats information.
type Client struct {
	dockerClient                *dockerapi.Client
	cpuUsageCoreNanoSeconds     uint64
	memoryPrivWorkingSetBytes   uint64
	memoryCommittedBytes        uint64
	mu                          sync.Mutex
	memoryPhysicalCapacityBytes uint64
	timeStarted                 time.Time
}

// NewClient constructs a Client.
func NewClient() (*Client, error) {
	client := new(Client)

	dockerClient, _ := dockerapi.NewEnvClient()
	client.dockerClient = dockerClient
	client.timeStarted = time.Now()

	// create physical memory
	memory, err := getPhysicallyInstalledSystemMemoryBytes()

	if err != nil {
		return nil, err
	}

	client.memoryPhysicalCapacityBytes = memory

	// start node monitoring (reading perf counters)
	errChan := make(chan error, 1)
	go client.startNodeMonitoring(errChan)

	err = <-errChan
	return client, err
}

// startNodeMonitoring starts reading perf counters of the node and updates
// the client struct with cpu and memory stats
func (c *Client) startNodeMonitoring(errChan chan error) {
	cpuChan, err := readPerformanceCounter(cpuQuery)

	if err != nil {
		errChan <- err
		return
	}

	memWorkingSetChan, err := readPerformanceCounter(memoryPrivWorkingSetQuery)

	if err != nil {
		errChan <- err
		return
	}

	memCommittedBytesChan, err := readPerformanceCounter(memoryCommittedBytesQuery)

	if err != nil {
		errChan <- err
		return
	}

	// no error, send nil over channel
	errChan <- nil

	for {
		select {
		case cpu := <-cpuChan:
			c.updateCPU(cpu)
		case mWorkingSet := <-memWorkingSetChan:
			c.updateMemoryWorkingSet(mWorkingSet)
		case mCommittedBytes := <-memCommittedBytesChan:
			c.updateMemoryCommittedBytes(mCommittedBytes)
		}
	}
}

func (c *Client) updateCPU(cpu Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()

	cpuCores := runtime.NumCPU()
	// This converts perf counter data which is cpu percentage for all cores into nanoseconds.
	// The formula is (cpuPercentage / 100.0) * #cores * 1e+9 (nano seconds). More info here:
	// https://github.com/kubernetes/heapster/issues/650
	c.cpuUsageCoreNanoSeconds += uint64((cpu.Value / 100.0) * float64(cpuCores) * 1000000000)
}

func (c *Client) updateMemoryWorkingSet(mWorkingSet Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.memoryPrivWorkingSetBytes = uint64(mWorkingSet.Value)
}

func (c *Client) updateMemoryCommittedBytes(mCommittedBytes Metric) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.memoryCommittedBytes = uint64(mCommittedBytes.Value)
}

// WinContainerInfos returns a map of container infos. The map contains node and
// pod level stats. Analogous to cadvisor GetContainerInfoV2 method.
func (c *Client) WinContainerInfos() (map[string]cadvisorapiv2.ContainerInfo, error) {
	infos := make(map[string]cadvisorapiv2.ContainerInfo)

	// root (node) container
	infos["/"] = *c.createRootContainerInfo()

	containers, err := c.dockerClient.ContainerList(context.Background(), dockertypes.ContainerListOptions{})

	if err != nil {
		return nil, err
	}

	for idx := range containers {
		container := &containers[idx]

		containerInfo, err := c.createContainerInfo(container)

		if err != nil {
			return nil, err
		}

		infos[container.ID] = *containerInfo
	}

	return infos, nil
}

// WinMachineInfo returns a cadvisorapi.MachineInfo with details about the
// node machine. Analogous to cadvisor MachineInfo method.
func (c *Client) WinMachineInfo() (*cadvisorapi.MachineInfo, error) {
	hostname, err := os.Hostname()

	if err != nil {
		return nil, err
	}

	return &cadvisorapi.MachineInfo{
		NumCores:       runtime.NumCPU(),
		MemoryCapacity: c.memoryPhysicalCapacityBytes,
		MachineID:      hostname,
	}, nil
}

// WinVersionInfo returns a  cadvisorapi.VersionInfo with version info of
// the kernel and docker runtime. Analogous to cadvisor VersionInfo method.
func (c *Client) WinVersionInfo() (*cadvisorapi.VersionInfo, error) {
	dockerServerVersion, err := c.dockerClient.ServerVersion(context.Background())

	if err != nil {
		return nil, err
	}

	return &cadvisorapi.VersionInfo{
		KernelVersion:    dockerServerVersion.KernelVersion,
		DockerVersion:    dockerServerVersion.Version,
		DockerAPIVersion: dockerServerVersion.APIVersion,
	}, nil
}

func (c *Client) createRootContainerInfo() *cadvisorapiv2.ContainerInfo {
	c.mu.Lock()
	defer c.mu.Unlock()

	var stats []*cadvisorapiv2.ContainerStats

	stats = append(stats, &cadvisorapiv2.ContainerStats{
		Timestamp: time.Now(),
		Cpu: &cadvisorapi.CpuStats{
			Usage: cadvisorapi.CpuUsage{
				Total: c.cpuUsageCoreNanoSeconds,
			},
		},
		Memory: &cadvisorapi.MemoryStats{
			WorkingSet: c.memoryPrivWorkingSetBytes,
			Usage:      c.memoryCommittedBytes,
		},
	})

	rootInfo := cadvisorapiv2.ContainerInfo{
		Spec: cadvisorapiv2.ContainerSpec{
			CreationTime: c.timeStarted,
			HasCpu:       true,
			HasMemory:    true,
			Memory: cadvisorapiv2.MemorySpec{
				Limit: c.memoryPhysicalCapacityBytes,
			},
		},
		Stats: stats,
	}

	return &rootInfo
}
func (c *Client) createContainerInfo(container *dockertypes.Container) (*cadvisorapiv2.ContainerInfo, error) {

	spec := cadvisorapiv2.ContainerSpec{
		CreationTime:     time.Unix(container.Created, 0),
		Aliases:          []string{},
		Namespace:        "docker",
		Labels:           container.Labels,
		Envs:             map[string]string{},
		HasCpu:           true,
		Cpu:              cadvisorapiv2.CpuSpec{},
		HasMemory:        true,
		Memory:           cadvisorapiv2.MemorySpec{},
		HasCustomMetrics: false,
		CustomMetrics:    []cadvisorapi.MetricSpec{},
		HasNetwork:       true,
		HasFilesystem:    false,
		HasDiskIo:        false,
		Image:            container.Image,
	}

	var stats []*cadvisorapiv2.ContainerStats
	containerStats, err := c.createContainerStats(container)

	if err != nil {
		return nil, err
	}

	stats = append(stats, containerStats)
	return &cadvisorapiv2.ContainerInfo{Spec: spec, Stats: stats}, nil
}

func (c *Client) createContainerStats(container *dockertypes.Container) (*cadvisorapiv2.ContainerStats, error) {
	dockerStatsJSON, err := c.getStatsForContainer(container.ID)

	if err != nil {
		return nil, err
	}

	dockerStats := dockerStatsJSON.Stats
	// create network stats

	var networkInterfaces []cadvisorapi.InterfaceStats

	for _, networkStats := range dockerStatsJSON.Networks {
		networkInterfaces = append(networkInterfaces, cadvisorapi.InterfaceStats{
			Name:      network.DefaultInterfaceName,
			RxBytes:   networkStats.RxBytes,
			RxPackets: networkStats.RxPackets,
			RxErrors:  networkStats.RxErrors,
			RxDropped: networkStats.RxDropped,
			TxBytes:   networkStats.TxBytes,
			TxPackets: networkStats.TxPackets,
			TxErrors:  networkStats.TxErrors,
			TxDropped: networkStats.TxDropped,
		})
	}

	stats := cadvisorapiv2.ContainerStats{
		Timestamp: time.Now(),
		// have to multiply cpu usage by 100 since docker stats units is in 100's of nano seconds for Windows
		// see https://github.com/moby/moby/blob/master/api/types/stats.go#L22
		Cpu:     &cadvisorapi.CpuStats{Usage: cadvisorapi.CpuUsage{Total: dockerStats.CPUStats.CPUUsage.TotalUsage * 100}},
		CpuInst: &cadvisorapiv2.CpuInstStats{},
		Memory:  &cadvisorapi.MemoryStats{WorkingSet: dockerStats.MemoryStats.PrivateWorkingSet, Usage: dockerStats.MemoryStats.Commit},
		Network: &cadvisorapiv2.NetworkStats{Interfaces: networkInterfaces},
		// TODO: ... diskio, filesystem, etc...
	}
	return &stats, nil
}

func (c *Client) getStatsForContainer(containerID string) (*StatsJSON, error) {
	response, err := c.dockerClient.ContainerStats(context.Background(), containerID, false)
	defer response.Close()

	if err != nil {
		return nil, err
	}
	dec := json.NewDecoder(response)

	var stats StatsJSON
	err = dec.Decode(&stats)

	if err != nil {
		return nil, err
	}

	return &stats, nil
}
