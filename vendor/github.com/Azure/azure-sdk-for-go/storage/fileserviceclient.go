package storage

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/url"
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	"strconv"
=======
	"strings"
>>>>>>> fix godeps issue and change azure_file code due to api change
)

// FileServiceClient contains operations for Microsoft Azure File Service.
type FileServiceClient struct {
	client Client
	auth   authentication
}

// ListSharesParameters defines the set of customizable parameters to make a
// List Shares call.
//
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
// See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/List-Shares
=======
// See https://msdn.microsoft.com/en-us/library/azure/dn167009.aspx
>>>>>>> fix godeps issue and change azure_file code due to api change
type ListSharesParameters struct {
	Prefix     string
	Marker     string
	Include    string
	MaxResults uint
	Timeout    uint
}

// ShareListResponse contains the response fields from
// ListShares call.
//
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
// See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/List-Shares
=======
// See https://msdn.microsoft.com/en-us/library/azure/dn167009.aspx
>>>>>>> fix godeps issue and change azure_file code due to api change
type ShareListResponse struct {
	XMLName    xml.Name `xml:"EnumerationResults"`
	Xmlns      string   `xml:"xmlns,attr"`
	Prefix     string   `xml:"Prefix"`
	Marker     string   `xml:"Marker"`
	NextMarker string   `xml:"NextMarker"`
	MaxResults int64    `xml:"MaxResults"`
	Shares     []Share  `xml:"Shares>Share"`
}

type compType string

const (
	compNone       compType = ""
	compList       compType = "list"
	compMetadata   compType = "metadata"
	compProperties compType = "properties"
	compRangeList  compType = "rangelist"
)

func (ct compType) String() string {
	return string(ct)
}

type resourceType string

const (
	resourceDirectory resourceType = "directory"
	resourceFile      resourceType = ""
	resourceShare     resourceType = "share"
)

func (rt resourceType) String() string {
	return string(rt)
}

func (p ListSharesParameters) getParameters() url.Values {
	out := url.Values{}

	if p.Prefix != "" {
		out.Set("prefix", p.Prefix)
	}
	if p.Marker != "" {
		out.Set("marker", p.Marker)
	}
	if p.Include != "" {
		out.Set("include", p.Include)
	}
	if p.MaxResults != 0 {
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
		out.Set("maxresults", strconv.FormatUint(uint64(p.MaxResults), 10))
	}
	if p.Timeout != 0 {
		out.Set("timeout", strconv.FormatUint(uint64(p.Timeout), 10))
=======
		out.Set("maxresults", fmt.Sprintf("%v", p.MaxResults))
	}
	if p.Timeout != 0 {
		out.Set("timeout", fmt.Sprintf("%v", p.Timeout))
>>>>>>> fix godeps issue and change azure_file code due to api change
	}

	return out
}

func (p ListDirsAndFilesParameters) getParameters() url.Values {
	out := url.Values{}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	if p.Prefix != "" {
		out.Set("prefix", p.Prefix)
	}
=======
>>>>>>> fix godeps issue and change azure_file code due to api change
	if p.Marker != "" {
		out.Set("marker", p.Marker)
	}
	if p.MaxResults != 0 {
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
		out.Set("maxresults", strconv.FormatUint(uint64(p.MaxResults), 10))
	}
	out = addTimeout(out, p.Timeout)
=======
		out.Set("maxresults", fmt.Sprintf("%v", p.MaxResults))
	}
	if p.Timeout != 0 {
		out.Set("timeout", fmt.Sprintf("%v", p.Timeout))
	}
>>>>>>> fix godeps issue and change azure_file code due to api change

	return out
}

// returns url.Values for the specified types
func getURLInitValues(comp compType, res resourceType) url.Values {
	values := url.Values{}
	if comp != compNone {
		values.Set("comp", comp.String())
	}
	if res != resourceFile {
		values.Set("restype", res.String())
	}
	return values
}

// GetShareReference returns a Share object for the specified share name.
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
func (f *FileServiceClient) GetShareReference(name string) *Share {
	return &Share{
		fsc:  f,
=======
func (f FileServiceClient) GetShareReference(name string) Share {
	return Share{
		fsc:  &f,
>>>>>>> fix godeps issue and change azure_file code due to api change
		Name: name,
		Properties: ShareProperties{
			Quota: -1,
		},
	}
}

// ListShares returns the list of shares in a storage account along with
// pagination token and other response details.
//
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
// See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/list-shares
=======
// See https://msdn.microsoft.com/en-us/library/azure/dd179352.aspx
>>>>>>> fix godeps issue and change azure_file code due to api change
func (f FileServiceClient) ListShares(params ListSharesParameters) (*ShareListResponse, error) {
	q := mergeParams(params.getParameters(), url.Values{"comp": {"list"}})

	var out ShareListResponse
	resp, err := f.listContent("", q, nil)
	if err != nil {
		return nil, err
	}
	defer resp.body.Close()
	err = xmlUnmarshal(resp.body, &out)

	// assign our client to the newly created Share objects
	for i := range out.Shares {
		out.Shares[i].fsc = &f
	}
	return &out, err
}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
// GetServiceProperties gets the properties of your storage account's file service.
// File service does not support logging
// See: https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/get-file-service-properties
func (f *FileServiceClient) GetServiceProperties() (*ServiceProperties, error) {
	return f.client.getServiceProperties(fileServiceName, f.auth)
}

// SetServiceProperties sets the properties of your storage account's file service.
// File service does not support logging
// See: https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/set-file-service-properties
func (f *FileServiceClient) SetServiceProperties(props ServiceProperties) error {
	return f.client.setServiceProperties(props, fileServiceName, f.auth)
}

=======
>>>>>>> fix godeps issue and change azure_file code due to api change
// retrieves directory or share content
func (f FileServiceClient) listContent(path string, params url.Values, extraHeaders map[string]string) (*storageResponse, error) {
	if err := f.checkForStorageEmulator(); err != nil {
		return nil, err
	}

	uri := f.client.getEndpoint(fileServiceName, path, params)
	extraHeaders = f.client.protectUserAgent(extraHeaders)
	headers := mergeHeaders(f.client.getStandardHeaders(), extraHeaders)

	resp, err := f.client.exec(http.MethodGet, uri, headers, nil, f.auth)
	if err != nil {
		return nil, err
	}

	if err = checkRespCode(resp.statusCode, []int{http.StatusOK}); err != nil {
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
		readAndCloseBody(resp.body)
=======
		resp.body.Close()
>>>>>>> fix godeps issue and change azure_file code due to api change
		return nil, err
	}

	return resp, nil
}

// returns true if the specified resource exists
func (f FileServiceClient) resourceExists(path string, res resourceType) (bool, http.Header, error) {
	if err := f.checkForStorageEmulator(); err != nil {
		return false, nil, err
	}

	uri := f.client.getEndpoint(fileServiceName, path, getURLInitValues(compNone, res))
	headers := f.client.getStandardHeaders()

	resp, err := f.client.exec(http.MethodHead, uri, headers, nil, f.auth)
	if resp != nil {
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
		defer readAndCloseBody(resp.body)
=======
		defer resp.body.Close()
>>>>>>> fix godeps issue and change azure_file code due to api change
		if resp.statusCode == http.StatusOK || resp.statusCode == http.StatusNotFound {
			return resp.statusCode == http.StatusOK, resp.headers, nil
		}
	}
	return false, nil, err
}

// creates a resource depending on the specified resource type
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
func (f FileServiceClient) createResource(path string, res resourceType, urlParams url.Values, extraHeaders map[string]string, expectedResponseCodes []int) (http.Header, error) {
	resp, err := f.createResourceNoClose(path, res, urlParams, extraHeaders)
	if err != nil {
		return nil, err
	}
	defer readAndCloseBody(resp.body)
	return resp.headers, checkRespCode(resp.statusCode, expectedResponseCodes)
}

// creates a resource depending on the specified resource type, doesn't close the response body
func (f FileServiceClient) createResourceNoClose(path string, res resourceType, urlParams url.Values, extraHeaders map[string]string) (*storageResponse, error) {
=======
func (f FileServiceClient) createResource(path string, res resourceType, extraHeaders map[string]string) (http.Header, error) {
	resp, err := f.createResourceNoClose(path, res, extraHeaders)
	if err != nil {
		return nil, err
	}
	defer resp.body.Close()
	return resp.headers, checkRespCode(resp.statusCode, []int{http.StatusCreated})
}

// creates a resource depending on the specified resource type, doesn't close the response body
func (f FileServiceClient) createResourceNoClose(path string, res resourceType, extraHeaders map[string]string) (*storageResponse, error) {
>>>>>>> fix godeps issue and change azure_file code due to api change
	if err := f.checkForStorageEmulator(); err != nil {
		return nil, err
	}

	values := getURLInitValues(compNone, res)
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	combinedParams := mergeParams(values, urlParams)
	uri := f.client.getEndpoint(fileServiceName, path, combinedParams)
=======
	uri := f.client.getEndpoint(fileServiceName, path, values)
>>>>>>> fix godeps issue and change azure_file code due to api change
	extraHeaders = f.client.protectUserAgent(extraHeaders)
	headers := mergeHeaders(f.client.getStandardHeaders(), extraHeaders)

	return f.client.exec(http.MethodPut, uri, headers, nil, f.auth)
}

// returns HTTP header data for the specified directory or share
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
func (f FileServiceClient) getResourceHeaders(path string, comp compType, res resourceType, params url.Values, verb string) (http.Header, error) {
	resp, err := f.getResourceNoClose(path, comp, res, params, verb, nil)
	if err != nil {
		return nil, err
	}
	defer readAndCloseBody(resp.body)
=======
func (f FileServiceClient) getResourceHeaders(path string, comp compType, res resourceType, verb string) (http.Header, error) {
	resp, err := f.getResourceNoClose(path, comp, res, verb, nil)
	if err != nil {
		return nil, err
	}
	defer resp.body.Close()
>>>>>>> fix godeps issue and change azure_file code due to api change

	if err = checkRespCode(resp.statusCode, []int{http.StatusOK}); err != nil {
		return nil, err
	}

	return resp.headers, nil
}

// gets the specified resource, doesn't close the response body
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
func (f FileServiceClient) getResourceNoClose(path string, comp compType, res resourceType, params url.Values, verb string, extraHeaders map[string]string) (*storageResponse, error) {
=======
func (f FileServiceClient) getResourceNoClose(path string, comp compType, res resourceType, verb string, extraHeaders map[string]string) (*storageResponse, error) {
>>>>>>> fix godeps issue and change azure_file code due to api change
	if err := f.checkForStorageEmulator(); err != nil {
		return nil, err
	}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	params = mergeParams(params, getURLInitValues(comp, res))
	uri := f.client.getEndpoint(fileServiceName, path, params)
=======
	params := getURLInitValues(comp, res)
	uri := f.client.getEndpoint(fileServiceName, path, params)
	extraHeaders = f.client.protectUserAgent(extraHeaders)
>>>>>>> fix godeps issue and change azure_file code due to api change
	headers := mergeHeaders(f.client.getStandardHeaders(), extraHeaders)

	return f.client.exec(verb, uri, headers, nil, f.auth)
}

// deletes the resource and returns the response
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
func (f FileServiceClient) deleteResource(path string, res resourceType, options *FileRequestOptions) error {
	resp, err := f.deleteResourceNoClose(path, res, options)
	if err != nil {
		return err
	}
	defer readAndCloseBody(resp.body)
=======
func (f FileServiceClient) deleteResource(path string, res resourceType) error {
	resp, err := f.deleteResourceNoClose(path, res)
	if err != nil {
		return err
	}
	defer resp.body.Close()
>>>>>>> fix godeps issue and change azure_file code due to api change
	return checkRespCode(resp.statusCode, []int{http.StatusAccepted})
}

// deletes the resource and returns the response, doesn't close the response body
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
func (f FileServiceClient) deleteResourceNoClose(path string, res resourceType, options *FileRequestOptions) (*storageResponse, error) {
=======
func (f FileServiceClient) deleteResourceNoClose(path string, res resourceType) (*storageResponse, error) {
>>>>>>> fix godeps issue and change azure_file code due to api change
	if err := f.checkForStorageEmulator(); err != nil {
		return nil, err
	}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	values := mergeParams(getURLInitValues(compNone, res), prepareOptions(options))
=======
	values := getURLInitValues(compNone, res)
>>>>>>> fix godeps issue and change azure_file code due to api change
	uri := f.client.getEndpoint(fileServiceName, path, values)
	return f.client.exec(http.MethodDelete, uri, f.client.getStandardHeaders(), nil, f.auth)
}

// merges metadata into extraHeaders and returns extraHeaders
func mergeMDIntoExtraHeaders(metadata, extraHeaders map[string]string) map[string]string {
	if metadata == nil && extraHeaders == nil {
		return nil
	}
	if extraHeaders == nil {
		extraHeaders = make(map[string]string)
	}
	for k, v := range metadata {
		extraHeaders[userDefinedMetadataHeaderPrefix+k] = v
	}
	return extraHeaders
}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
// sets extra header data for the specified resource
func (f FileServiceClient) setResourceHeaders(path string, comp compType, res resourceType, extraHeaders map[string]string, options *FileRequestOptions) (http.Header, error) {
=======
// merges extraHeaders into headers and returns headers
func mergeHeaders(headers, extraHeaders map[string]string) map[string]string {
	for k, v := range extraHeaders {
		headers[k] = v
	}
	return headers
}

// sets extra header data for the specified resource
func (f FileServiceClient) setResourceHeaders(path string, comp compType, res resourceType, extraHeaders map[string]string) (http.Header, error) {
>>>>>>> fix godeps issue and change azure_file code due to api change
	if err := f.checkForStorageEmulator(); err != nil {
		return nil, err
	}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	params := mergeParams(getURLInitValues(comp, res), prepareOptions(options))
=======
	params := getURLInitValues(comp, res)
>>>>>>> fix godeps issue and change azure_file code due to api change
	uri := f.client.getEndpoint(fileServiceName, path, params)
	extraHeaders = f.client.protectUserAgent(extraHeaders)
	headers := mergeHeaders(f.client.getStandardHeaders(), extraHeaders)

	resp, err := f.client.exec(http.MethodPut, uri, headers, nil, f.auth)
	if err != nil {
		return nil, err
	}
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	defer readAndCloseBody(resp.body)
=======
	defer resp.body.Close()
>>>>>>> fix godeps issue and change azure_file code due to api change

	return resp.headers, checkRespCode(resp.statusCode, []int{http.StatusOK})
}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
=======
// gets metadata for the specified resource
func (f FileServiceClient) getMetadata(path string, res resourceType) (map[string]string, error) {
	if err := f.checkForStorageEmulator(); err != nil {
		return nil, err
	}

	headers, err := f.getResourceHeaders(path, compMetadata, res, http.MethodGet)
	if err != nil {
		return nil, err
	}

	return getMetadataFromHeaders(headers), nil
}

// returns a map of custom metadata values from the specified HTTP header
func getMetadataFromHeaders(header http.Header) map[string]string {
	metadata := make(map[string]string)
	for k, v := range header {
		// Can't trust CanonicalHeaderKey() to munge case
		// reliably. "_" is allowed in identifiers:
		// https://msdn.microsoft.com/en-us/library/azure/dd179414.aspx
		// https://msdn.microsoft.com/library/aa664670(VS.71).aspx
		// http://tools.ietf.org/html/rfc7230#section-3.2
		// ...but "_" is considered invalid by
		// CanonicalMIMEHeaderKey in
		// https://golang.org/src/net/textproto/reader.go?s=14615:14659#L542
		// so k can be "X-Ms-Meta-Foo" or "x-ms-meta-foo_bar".
		k = strings.ToLower(k)
		if len(v) == 0 || !strings.HasPrefix(k, strings.ToLower(userDefinedMetadataHeaderPrefix)) {
			continue
		}
		// metadata["foo"] = content of the last X-Ms-Meta-Foo header
		k = k[len(userDefinedMetadataHeaderPrefix):]
		metadata[k] = v[len(v)-1]
	}

	if len(metadata) == 0 {
		return nil
	}

	return metadata
}

>>>>>>> fix godeps issue and change azure_file code due to api change
//checkForStorageEmulator determines if the client is setup for use with
//Azure Storage Emulator, and returns a relevant error
func (f FileServiceClient) checkForStorageEmulator() error {
	if f.client.accountName == StorageEmulatorAccountName {
		return fmt.Errorf("Error: File service is not currently supported by Azure Storage Emulator")
	}
	return nil
}
