/*
Copyright 2016 The Kubernetes Authors.

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

package azure

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/golang/glog"
	kwait "k8s.io/apimachinery/pkg/util/wait"
)

//ManagedDiskController : managed disk controller struct
type ManagedDiskController struct {
	common *controllerCommon
}

func newManagedDiskController(common *controllerCommon) (*ManagedDiskController, error) {
	return &ManagedDiskController{common: common}, nil
}

//AttachManagedDisk : attach managed disk
func (c *ManagedDiskController) AttachManagedDisk(nodeName string, diskURI string, cacheMode string) (int, error) {
	// We don't need to validate if the disk is already attached
	// to a different VM. The VM update call below will fail if
	// it was attached somewhere else

	// this behaviour is expected in (i.e during a k8s drain node call)
	// k8s will evantually call detach->oldnode followed by attach->newnode
	var vmData interface{}
	vm, err := c.common.getArmVM(nodeName)

	if err != nil {
		return -1, err
	}

	if err := json.Unmarshal(vm, &vmData); err != nil {
		return -1, err
	}

	fragment, ok := vmData.(map[string]interface{})
	if !ok {
		return -1, fmt.Errorf("convert vmData to map error")
	}
	// remove "resources" as ARM does not support PUT with "resources"
	delete(fragment, "resources")

	dataDisks, storageProfile, hardwareProfile, err := ExtractVMData(fragment)
	if err != nil {
		return -1, err
	}
	vmSize := hardwareProfile["vmSize"].(string)

	managedVM := c.common.isManagedArmVM(storageProfile)
	if !managedVM {
		return -1, fmt.Errorf("azureDisk - error: attempt to attach managed disk %s to an unmanaged node  %s ", diskURI, nodeName)
	}

	lun, err := findEmptyLun(vmSize, dataDisks)

	if err != nil {
		return -1, err
	}

	managedDiskInfo := &armVMManagedDiskInfo{ID: diskURI}
	newDisk := &armVMDataDisk{
		Caching:      cacheMode,
		CreateOption: "Attach",
		ManagedDisk:  managedDiskInfo,
		Lun:          lun,
	}
	dataDisks = append(dataDisks, newDisk)

	storageProfile["dataDisks"] = dataDisks // -> store back

	payload := new(bytes.Buffer)
	err = json.NewEncoder(payload).Encode(fragment)

	if err != nil {
		return -1, err
	}

	if err = c.common.updateArmVM(nodeName, payload); err != nil {
		return -1, err
	}

	// We don't need to poll ARM here, since WaitForAttach (running on node) will
	// be looping on the node to get devicepath /dev/sd* by lun#
	glog.V(2).Infof("azureDisk - Attached disk %s to node %s", diskURI, nodeName)

	return lun, err
}

//DetachManagedDisk : detach managed disk
func (c *ManagedDiskController) DetachManagedDisk(nodeName string, hashedDiskID string) error {
	diskID := ""
	var vmData interface{}
	vm, err := c.common.getArmVM(nodeName)

	if err != nil {
		return err
	}

	if err := json.Unmarshal(vm, &vmData); err != nil {
		return err
	}

	fragment, ok := vmData.(map[string]interface{})
	if !ok {
		return fmt.Errorf("convert vmData to map error")
	}

	// remove "resources" as ARM does not support PUT with "resources"
	delete(fragment, "resources")
	dataDisks, storageProfile, _, err := ExtractVMData(fragment)
	if err != nil {
		return err
	}

	var newDataDisks []interface{}
	for _, v := range dataDisks {
		d := v.(map[string]interface{})
		md, ok := d["managedDisk"].(map[string]interface{})
		if !ok {
			return fmt.Errorf("convert vmData(managedDisk) to map error")
		}

		currentDiskID := strings.ToLower(md["id"].(string))
		hashedCurrentDiskID := MakeCRC32(currentDiskID)

		if hashedDiskID != hashedCurrentDiskID {
			newDataDisks = append(newDataDisks, d)
		} else {
			diskID = currentDiskID
		}
	}

	if diskID == "" {
		glog.Warningf("azureDisk - disk with hash %s was not found atached on node %s", hashedDiskID, nodeName)
		return nil
	}

	//get Disk Name
	diskName := path.Base(diskID)

	storageProfile["dataDisks"] = newDataDisks // -> store back
	payload := new(bytes.Buffer)
	err = json.NewEncoder(payload).Encode(fragment)

	if err != nil {
		return err
	}
	updateErr := c.common.updateArmVM(nodeName, payload)

	if updateErr != nil {
		glog.Infof("azureDisk - error while detaching a managed disk disk(%s) node(%s) error(%s)", diskID, nodeName, updateErr.Error())
		return updateErr
	}
	// poll
	// This is critical case, if this was a PVC, k8s will immediatly
	// attempt to delete the disk (according to policy)
	// a race condition will occure if we returned before
	// 1) disk is cleared from VM "dataDisks"
	// 2) disk status is not: unattached
	err = kwait.ExponentialBackoff(defaultBackOff, func() (bool, error) {
		// confirm that it is attached to the machine
		attached, _, err := c.common.IsDiskAttached(hashedDiskID, nodeName, true)
		if err == nil && !attached {
			// confirm that the disk status has changed
			_, _, aState, err := c.getDisk(diskName)

			if err == nil && aState == "Unattached" {
				return true, nil
			}
			return false, err
		}
		return false, err
	})

	if err != nil {
		glog.V(2).Infof("azureDisk - detached disk %s from node %s but was unable to confirm complete complete-detach during poll", diskName, nodeName)
	} else {
		glog.V(2).Infof("azureDisk - detached disk %s from node %s", diskName, nodeName)
	}

	return nil
}

//CreateManagedDisk : create managed disk
func (c *ManagedDiskController) CreateManagedDisk(diskName string, storageAccountType string, sizeGB int, tags map[string]string) (string, error) {
	glog.V(4).Infof("azureDisk - dreating new managed Name:%s StorageAccountType:%s Size:%v", diskName, storageAccountType, sizeGB)

	if tags == nil {
		tags = make(map[string]string)
	}
	tags["created-by"] = "kubernetes/azure-dd"

	tagsPayload := new(bytes.Buffer)
	e := json.NewEncoder(tagsPayload).Encode(tags)

	if e != nil {
		glog.Infof("azureDisk - failed to encode tags for Azure Disk %s error: %s", diskName, e)
		return "", e
	}

	tagsString := tagsPayload.String()

	// Azure won't allow / (forward slash) in tags
	tagsString = strings.Replace(tagsString, "/", "-", -1)

	uri := fmt.Sprintf(diskEndPointTemplate, c.common.managementEndpoint, c.common.subscriptionID, c.common.resourceGroup, diskName, apiversion)

	requestData := `{ "tags" : ` + tagsString + `,   "location" : "` + c.common.location + `", "properties":  { "creationData":  {"createOption": "Empty" }, "accountType"  : "` + storageAccountType + `", "diskSizeGB": "` + strconv.Itoa(sizeGB) + `"  } }`

	client := &http.Client{}
	content := bytes.NewBufferString(requestData)
	r, err := http.NewRequest("PUT", uri, content)
	if err != nil {
		return "", err
	}

	token, err := c.common.getToken()
	if err != nil {
		return "", err
	}
	r.Header.Add("Content-Type", "application/json")
	r.Header.Add("Authorization", "Bearer "+token)

	resp, err := client.Do(r)

	if err != nil || resp.StatusCode != 202 {
		defer resp.Body.Close()
		return "", getRestError(fmt.Sprintf("Create Managed Disk: %s", diskName), err, 202, resp.StatusCode, resp.Body)
	}
	defer resp.Body.Close()

	diskID := fmt.Sprintf(diskIDTemplate, c.common.subscriptionID, c.common.resourceGroup, diskName)

	err = kwait.ExponentialBackoff(defaultBackOff, func() (bool, error) {
		exists, pState, _, err := c.getDisk(diskName)
		// We are waiting for Exists, provisioningState==Succeeded
		// We don't want to hand-off managed disks to k8s while they are
		//still being provisioned, this is to avoid some racy conditions
		if err != nil {
			return false, err
		}
		if exists && pState == "Succeeded" {
			return true, nil
		}
		return false, nil
	})

	if err != nil {
		glog.V(2).Infof("azureDisk - created new MD Name:%s StorageAccountType:%s Size:%v but was unable to confirm provisioningState in poll process", diskName, storageAccountType, sizeGB)
	} else {
		glog.V(2).Infof("azureDisk - created new MD Name:%s StorageAccountType:%s Size:%v", diskName, storageAccountType, sizeGB)
	}

	return diskID, nil
}

//DeleteManagedDisk : delete managed disk
func (c *ManagedDiskController) DeleteManagedDisk(diskURI string) error {
	diskName := path.Base(diskURI)
	uri := fmt.Sprintf(diskEndPointTemplate, c.common.managementEndpoint, c.common.subscriptionID, c.common.resourceGroup, diskName, apiversion)

	client := &http.Client{}
	r, err := http.NewRequest("DELETE", uri, nil)
	if err != nil {
		return nil
	}

	token, err := c.common.getToken()
	if err != nil {
		return err
	}

	r.Header.Add("Authorization", "Bearer "+token)

	resp, err := client.Do(r)

	if err != nil || resp.StatusCode != 202 {
		return getRestError(fmt.Sprintf("Delete Managed Disk: %s", diskURI), err, 202, resp.StatusCode, resp.Body)
	}

	defer resp.Body.Close()
	// We don't need poll here, k8s will immediatly stop referencing the disk
	// the disk will be evantually deleted - cleanly - by ARM

	glog.V(2).Infof("azureDisk - deleted a managed disk: %s", diskURI)

	return nil
}

func (c *ManagedDiskController) getDisk(diskName string) (bool, string, string, error) {
	uri := fmt.Sprintf(diskEndPointTemplate, c.common.managementEndpoint, c.common.subscriptionID, c.common.resourceGroup, diskName, apiversion)

	client := &http.Client{}
	r, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return false, "", "", nil
	}

	token, err := c.common.getToken()
	if err != nil {
		return false, "", "", err
	}

	r.Header.Add("Authorization", "Bearer "+token)

	resp, err := client.Do(r)

	if err != nil || resp.StatusCode != 200 {
		defer resp.Body.Close()
		newError := getRestError("Get Managed Disk", err, 200, resp.StatusCode, resp.Body)
		// log the new formatted error and return the original error
		glog.Infof(newError.Error())
		return false, "", "", err
	}
	defer resp.Body.Close()

	bodyBytes, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return false, "", "", err
	}

	var disk interface{}

	if err := json.Unmarshal(bodyBytes, &disk); err != nil {
		return false, "", "", err
	}

	// Extract Provisioning State & Disk State
	provisioningState, diskState, err := ExtractDiskData(disk)
	if err != nil {
		return false, "", "", err
	}

	return true, provisioningState, diskState, nil
}
