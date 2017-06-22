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

package azure

import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	kwait "k8s.io/apimachinery/pkg/util/wait"

	"github.com/golang/glog"
)

type armVMVhdDiskInfo struct {
	URI string `json:"uri"`
}
type armVMManagedDiskInfo struct {
	ID string `json:"id"`
}

type armVMDataDisk struct {
	Lun          int                   `json:"lun"`
	Name         string                `json:"name,omitempty"`
	CreateOption string                `json:"createOption"`
	ManagedDisk  *armVMManagedDiskInfo `json:"managedDisk,omitempty"`
	Vhd          *armVMVhdDiskInfo     `json:"vhd,omitempty"`
	Caching      string                `json:"caching"`
	DiskSizeGB   int                   `json:"diskSizeGB,omitempty"`
}

const (
	aadTokenEndPointPath string = "%s/oauth2/token/"
	apiversion           string = "2016-04-30-preview"
	diskEndPointTemplate string = "%ssubscriptions/%s/resourcegroups/%s/providers/microsoft.compute/disks/%s?api-version=%s"
	vMEndPointTemplate   string = "%ssubscriptions/%s/resourcegroups/%s/providers/microsoft.compute/virtualmachines/%s?api-version=%s"
	diskIDTemplate       string = "/subscriptions/%s/resourcegroups/%s/providers/microsoft.compute/disks/%s"
	defaultDataDiskCount int    = 16 // which will allow you to work with most medium size VMs (if not found in map)

	storageAccountNameTemplate     = "pvc%s"
	storageAccountEndPointTemplate = "%ssubscriptions/%s/resourcegroups/%s/providers/microsoft.storage/storageaccounts/%s?api-version=2016-01-01"

	// for limits check https://docs.microsoft.com/en-us/azure/azure-subscription-service-limits#storage-limits
	maxStorageAccounts                     = 100 // max # is 200 (250 with special request). this allows 100 for everything else including stand alone disks
	maxDisksPerStorageAccounts             = 60
	storageAccountUtilizationBeforeGrowing = 0.5
	storageAccountsCountInit               = 2 // When the plug-in is init-ed, 2 storage accounts will be created to allow fast pvc create/attach/mount
)

var defaultBackOff = kwait.Backoff{
	Steps:    20,
	Duration: 2 * time.Second,
	Factor:   1.5,
	Jitter:   0.0,
}

var time1970 = time.Date(1970, 1, 1, 0, 0, 0, 0, time.UTC)
var polyTable = crc32.MakeTable(crc32.Koopman)

type controllerCommon struct {
	tenantID              string
	subscriptionID        string
	location              string
	storageEndpointSuffix string
	resourceGroup         string
	clientID              string
	clientSecret          string
	managementEndpoint    string
	tokenEndPoint         string
	aadResourceEndPoint   string
	aadToken              string
	expiresOn             time.Time
}

func (c *controllerCommon) isManagedArmVM(storageProfile map[string]interface{}) bool {
	osDisk := storageProfile["osDisk"].(map[string]interface{})
	if _, ok := osDisk["managedDisk"]; ok {
		return true
	}
	return false
}

func (c *controllerCommon) GetAttachedDisks(nodeName string) ([]string, error) {
	var disks []string
	var vmData interface{}
	vm, err := c.getArmVM(nodeName)

	if err != nil {
		return nil, err
	}

	if err := json.Unmarshal(vm, &vmData); err != nil {
		return disks, err
	}

	fragment, ok := vmData.(map[string]interface{})
	if !ok {
		return disks, fmt.Errorf("convert vmData to map error")
	}

	dataDisks, _, _, err := ExtractVMData(fragment)
	if err != nil {
		return disks, err
	}

	// we silently ignore, if VM does not have the disk attached
	for _, v := range dataDisks {
		d := v.(map[string]interface{})
		if _, ok := d["vhd"]; ok {
			// this is a blob disk
			vhdInfo, ok := d["vhd"].(map[string]interface{})
			if !ok {
				return disks, fmt.Errorf("convert vmData(vhd) to map error")
			}
			vhdURI := vhdInfo["uri"].(string)
			disks = append(disks, vhdURI)
		} else {
			// this is managed disk
			managedDiskInfo, ok := d["managedDisk"].(map[string]interface{})
			if !ok {
				return disks, fmt.Errorf("convert vmData(managedDisk) to map error")
			}
			managedDiskID := managedDiskInfo["id"].(string)
			disks = append(disks, managedDiskID)
		}
	}
	return disks, nil
}

//IsDiskAttached : if disk attached returns bool + lun attached to
func (c *controllerCommon) IsDiskAttached(hashedDiskURI, nodeName string, isManaged bool) (attached bool, lun int, err error) {
	attached = false
	lun = -1

	var vmData interface{}

	vm, err := c.getArmVM(nodeName)

	if err != nil {
		return attached, lun, err
	}

	if err := json.Unmarshal(vm, &vmData); err != nil {
		return attached, lun, err
	}

	fragment, ok := vmData.(map[string]interface{})
	if !ok {
		return attached, lun, fmt.Errorf("convert vmData to map error")
	}

	dataDisks, _, _, err := ExtractVMData(fragment)
	if err != nil {
		return attached, lun, err
	}

	for _, v := range dataDisks {
		d := v.(map[string]interface{})
		if isManaged {
			md, ok := d["managedDisk"].(map[string]interface{})
			if !ok {
				return attached, lun, fmt.Errorf("convert vmData(managedDisk) to map error")
			}
			currentDiskID := strings.ToLower(md["id"].(string))
			hashedCurrentDiskID := MakeCRC32(currentDiskID)
			if hashedCurrentDiskID == hashedDiskURI {
				attached = true
				lun = int(d["lun"].(float64))
				break
			}
		} else {
			blobDisk, ok := d["vhd"].(map[string]interface{})
			if !ok {
				return attached, lun, fmt.Errorf("convert vmData(vhd) to map error")
			}
			blobDiskURI := blobDisk["uri"].(string)
			hashedBlobDiskURI := MakeCRC32(blobDiskURI)
			if hashedBlobDiskURI == hashedDiskURI {
				attached = true
				lun = int(d["lun"].(float64))
				break
			}
		}
	}

	return attached, lun, nil
}

// Updates an arm VM based on the payload
func (c *controllerCommon) updateArmVM(armVMName string, buffer *bytes.Buffer) error {
	uri := fmt.Sprintf(vMEndPointTemplate, c.managementEndpoint, c.subscriptionID, c.resourceGroup, armVMName, apiversion)
	client := &http.Client{}
	r, err := http.NewRequest("PUT", uri, buffer)
	if err != nil {
		return err
	}

	token, err := c.getToken()
	if err != nil {
		return err
	}

	r.Header.Add("Content-Type", "application/json")
	r.Header.Add("Authorization", "Bearer "+token)
	resp, err := client.Do(r)

	if err != nil || resp.StatusCode != 200 {
		return getRestError(fmt.Sprintf("Update ARM VM: %s", armVMName), err, 200, resp.StatusCode, resp.Body)
	}
	return nil
}

// Gets ARM VM
func (c *controllerCommon) getArmVM(armVMName string) ([]byte, error) {
	uri := fmt.Sprintf(vMEndPointTemplate, c.managementEndpoint, c.subscriptionID, c.resourceGroup, armVMName, apiversion)
	client := &http.Client{}
	r, err := http.NewRequest("GET", uri, nil)
	if err != nil {
		return nil, err
	}

	token, err := c.getToken()
	if err != nil {
		return nil, err
	}

	r.Header.Add("Authorization", "Bearer "+token)
	resp, err := client.Do(r)
	if err != nil {
		return nil, err
	}

	defer resp.Body.Close()
	if err != nil || resp.StatusCode != 200 {
		return nil, getRestError(fmt.Sprintf("Get ARM VM: %s", armVMName), err, 200, resp.StatusCode, resp.Body)
	}

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	return body, nil
}

func (c *controllerCommon) getToken() (token string, err error) {
	if c.aadToken != "" && time.Now().UTC().Sub(c.expiresOn).Seconds() <= 10 {
		// token cached and is valid.
		return c.aadToken, nil
	}

	apiURL := c.tokenEndPoint
	resource := fmt.Sprintf(aadTokenEndPointPath, c.tenantID)
	// create form urlencoded post data
	formData := url.Values{}

	formData.Add("grant_type", "client_credentials")
	formData.Add("client_id", c.clientID)
	formData.Add("client_secret", c.clientSecret)
	formData.Add("resource", c.aadResourceEndPoint)

	urlStr := ""
	u := &url.URL{}
	if u, err = url.ParseRequestURI(apiURL); err != nil {
		return "", err
	}

	u.Path = resource
	urlStr = fmt.Sprintf("%v", u)
	client := &http.Client{}

	r, err := http.NewRequest("POST", urlStr, bytes.NewBufferString(formData.Encode()))
	if err != nil {
		return "", err
	}

	// add headers
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Add("Content-Length", strconv.Itoa(len(formData.Encode())))
	resp, err := client.Do(r)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	payload, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	c.aadToken, c.expiresOn, err = parseAADToken(payload)
	return c.aadToken, err
}

func parseAADToken(payload []byte) (string, time.Time, error) {
	var f interface{}
	var sToken string
	var expiresOn time.Time
	var expiresSec int
	var ok bool
	var expires string

	if err := json.Unmarshal(payload, &f); err != nil {
		return "", expiresOn, err
	}

	fragment, ok := f.(map[string]interface{})
	if !ok {
		return "", expiresOn, fmt.Errorf("convert vmData to map error")
	}
	if sToken, ok = fragment["access_token"].(string); ok != true {
		return "", expiresOn, fmt.Errorf("Disk controller (ARM Client) cannot parse AAD token - access_token field")
	}
	if expires, ok = fragment["expires_on"].(string); ok != true {
		return "", expiresOn, fmt.Errorf("Disk controller (ARM Client) cannot parse AAD token - expires_on field")
	}

	expiresSec, _ = strconv.Atoi(expires)
	// expires_on is seconds since 1970-01-01T0:0:0Z UTC
	expiresOn = time1970.UTC().Add(time.Duration(expiresSec) * time.Second)

	return sToken, expiresOn, nil
}

// Creats a slice  luns based on the VM size, used the static map declared on this package
func getLunMapForVM(vmSize string) []bool {
	count, ok := dataDisksPerVM[vmSize]
	if !ok {
		glog.Warningf("azureDisk - VM Size %s found no static lun count will use default which  %v", vmSize, defaultDataDiskCount)
		count = defaultDataDiskCount
	}

	m := make([]bool, count)
	return m
}

// finds an empty based on VM size and current attached disks
func findEmptyLun(vmSize string, dataDisks []interface{}) (int, error) {
	vmLuns := getLunMapForVM(vmSize)
	selectedLun := -1

	for _, v := range dataDisks {
		current := v.(map[string]interface{})
		lun := int(current["lun"].(float64))
		vmLuns[lun] = true
	}

	//find first empty lun
	for i, v := range vmLuns {
		if v == false {
			selectedLun = i
			break
		}
	}

	if selectedLun == -1 {
		return selectedLun, fmt.Errorf("azureDisk - failed to find empty lun on VM type:%s total-luns-checked:%v", vmSize, len(vmLuns))
	}

	return selectedLun, nil
}

func getRestError(operation string, restError error, expectedStatus int, actualStatus int, body io.ReadCloser) error {
	if restError != nil {
		return fmt.Errorf("azureDisk - %s - Rest Error: %s", operation, restError)
	}

	bodystr := ""
	if body != nil {
		bodyBytes, _ := ioutil.ReadAll(body)
		if bodyBytes != nil {
			bodystr = string(bodyBytes)
		}
	}
	return fmt.Errorf("azureDisk - %s - Rest Status Error:\n Expected: %v\n Got: %v\n ResponseBody:%s", operation, expectedStatus, actualStatus, bodystr)
}
