package storage

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
=======
// BlobStorageClient contains operations for Microsoft Azure Blob Storage
// Service.
type BlobStorageClient struct {
	client Client
	auth   authentication
}

// A Container is an entry in ContainerListResponse.
type Container struct {
	Name       string              `xml:"Name"`
	Properties ContainerProperties `xml:"Properties"`
	// TODO (ahmetalpbalkan) Metadata
}

// ContainerProperties contains various properties of a container returned from
// various endpoints like ListContainers.
type ContainerProperties struct {
	LastModified  string `xml:"Last-Modified"`
	Etag          string `xml:"Etag"`
	LeaseStatus   string `xml:"LeaseStatus"`
	LeaseState    string `xml:"LeaseState"`
	LeaseDuration string `xml:"LeaseDuration"`
	// TODO (ahmetalpbalkan) remaining fields
}

// ContainerListResponse contains the response fields from
// ListContainers call.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179352.aspx
type ContainerListResponse struct {
	XMLName    xml.Name    `xml:"EnumerationResults"`
	Xmlns      string      `xml:"xmlns,attr"`
	Prefix     string      `xml:"Prefix"`
	Marker     string      `xml:"Marker"`
	NextMarker string      `xml:"NextMarker"`
	MaxResults int64       `xml:"MaxResults"`
	Containers []Container `xml:"Containers>Container"`
}

>>>>>>> fix godeps issue and change azure_file code due to api change
// A Blob is an entry in BlobListResponse.
type Blob struct {
	Container  *Container
	Name       string         `xml:"Name"`
	Snapshot   time.Time      `xml:"Snapshot"`
	Properties BlobProperties `xml:"Properties"`
	Metadata   BlobMetadata   `xml:"Metadata"`
}

// PutBlobOptions includes the options any put blob operation
// (page, block, append)
type PutBlobOptions struct {
	Timeout           uint
	LeaseID           string     `header:"x-ms-lease-id"`
	Origin            string     `header:"Origin"`
	IfModifiedSince   *time.Time `header:"If-Modified-Since"`
	IfUnmodifiedSince *time.Time `header:"If-Unmodified-Since"`
	IfMatch           string     `header:"If-Match"`
	IfNoneMatch       string     `header:"If-None-Match"`
	RequestID         string     `header:"x-ms-client-request-id"`
}

// BlobMetadata is a set of custom name/value pairs.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179404.aspx
type BlobMetadata map[string]string

type blobMetadataEntries struct {
	Entries []blobMetadataEntry `xml:",any"`
}
type blobMetadataEntry struct {
	XMLName xml.Name
	Value   string `xml:",chardata"`
}

// UnmarshalXML converts the xml:Metadata into Metadata map
func (bm *BlobMetadata) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var entries blobMetadataEntries
	if err := d.DecodeElement(&entries, &start); err != nil {
		return err
	}
	for _, entry := range entries.Entries {
		if *bm == nil {
			*bm = make(BlobMetadata)
		}
		(*bm)[strings.ToLower(entry.XMLName.Local)] = entry.Value
	}
	return nil
}

// MarshalXML implements the xml.Marshaler interface. It encodes
// metadata name/value pairs as they would appear in an Azure
// ListBlobs response.
func (bm BlobMetadata) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	entries := make([]blobMetadataEntry, 0, len(bm))
	for k, v := range bm {
		entries = append(entries, blobMetadataEntry{
			XMLName: xml.Name{Local: http.CanonicalHeaderKey(k)},
			Value:   v,
		})
	}
	return enc.EncodeElement(blobMetadataEntries{
		Entries: entries,
	}, start)
}

// BlobProperties contains various properties of a blob
// returned in various endpoints like ListBlobs or GetBlobProperties.
type BlobProperties struct {
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	LastModified          TimeRFC1123 `xml:"Last-Modified"`
	Etag                  string      `xml:"Etag"`
	ContentMD5            string      `xml:"Content-MD5" header:"x-ms-blob-content-md5"`
	ContentLength         int64       `xml:"Content-Length"`
	ContentType           string      `xml:"Content-Type" header:"x-ms-blob-content-type"`
	ContentEncoding       string      `xml:"Content-Encoding" header:"x-ms-blob-content-encoding"`
	CacheControl          string      `xml:"Cache-Control" header:"x-ms-blob-cache-control"`
	ContentLanguage       string      `xml:"Cache-Language" header:"x-ms-blob-content-language"`
	ContentDisposition    string      `xml:"Content-Disposition" header:"x-ms-blob-content-disposition"`
	BlobType              BlobType    `xml:"x-ms-blob-blob-type"`
	SequenceNumber        int64       `xml:"x-ms-blob-sequence-number"`
	CopyID                string      `xml:"CopyId"`
	CopyStatus            string      `xml:"CopyStatus"`
	CopySource            string      `xml:"CopySource"`
	CopyProgress          string      `xml:"CopyProgress"`
	CopyCompletionTime    TimeRFC1123 `xml:"CopyCompletionTime"`
	CopyStatusDescription string      `xml:"CopyStatusDescription"`
	LeaseStatus           string      `xml:"LeaseStatus"`
	LeaseState            string      `xml:"LeaseState"`
	LeaseDuration         string      `xml:"LeaseDuration"`
	ServerEncrypted       bool        `xml:"ServerEncrypted"`
	IncrementalCopy       bool        `xml:"IncrementalCopy"`
=======
	LastModified          string   `xml:"Last-Modified"`
	Etag                  string   `xml:"Etag"`
	ContentMD5            string   `xml:"Content-MD5"`
	ContentLength         int64    `xml:"Content-Length"`
	ContentType           string   `xml:"Content-Type"`
	ContentEncoding       string   `xml:"Content-Encoding"`
	CacheControl          string   `xml:"Cache-Control"`
	ContentLanguage       string   `xml:"Cache-Language"`
	BlobType              BlobType `xml:"x-ms-blob-blob-type"`
	SequenceNumber        int64    `xml:"x-ms-blob-sequence-number"`
	CopyID                string   `xml:"CopyId"`
	CopyStatus            string   `xml:"CopyStatus"`
	CopySource            string   `xml:"CopySource"`
	CopyProgress          string   `xml:"CopyProgress"`
	CopyCompletionTime    string   `xml:"CopyCompletionTime"`
	CopyStatusDescription string   `xml:"CopyStatusDescription"`
	LeaseStatus           string   `xml:"LeaseStatus"`
	LeaseState            string   `xml:"LeaseState"`
}

// BlobHeaders contains various properties of a blob and is an entry
// in SetBlobProperties
type BlobHeaders struct {
	ContentMD5      string `header:"x-ms-blob-content-md5"`
	ContentLanguage string `header:"x-ms-blob-content-language"`
	ContentEncoding string `header:"x-ms-blob-content-encoding"`
	ContentType     string `header:"x-ms-blob-content-type"`
	CacheControl    string `header:"x-ms-blob-cache-control"`
}

// BlobListResponse contains the response fields from ListBlobs call.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd135734.aspx
type BlobListResponse struct {
	XMLName    xml.Name `xml:"EnumerationResults"`
	Xmlns      string   `xml:"xmlns,attr"`
	Prefix     string   `xml:"Prefix"`
	Marker     string   `xml:"Marker"`
	NextMarker string   `xml:"NextMarker"`
	MaxResults int64    `xml:"MaxResults"`
	Blobs      []Blob   `xml:"Blobs>Blob"`

	// BlobPrefix is used to traverse blobs as if it were a file system.
	// It is returned if ListBlobsParameters.Delimiter is specified.
	// The list here can be thought of as "folders" that may contain
	// other folders or blobs.
	BlobPrefixes []string `xml:"Blobs>BlobPrefix>Name"`

	// Delimiter is used to traverse blobs as if it were a file system.
	// It is returned if ListBlobsParameters.Delimiter is specified.
	Delimiter string `xml:"Delimiter"`
}

// ListContainersParameters defines the set of customizable parameters to make a
// List Containers call.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179352.aspx
type ListContainersParameters struct {
	Prefix     string
	Marker     string
	Include    string
	MaxResults uint
	Timeout    uint
}

func (p ListContainersParameters) getParameters() url.Values {
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
		out.Set("maxresults", fmt.Sprintf("%v", p.MaxResults))
	}
	if p.Timeout != 0 {
		out.Set("timeout", fmt.Sprintf("%v", p.Timeout))
	}

	return out
}

// ListBlobsParameters defines the set of customizable
// parameters to make a List Blobs call.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd135734.aspx
type ListBlobsParameters struct {
	Prefix     string
	Delimiter  string
	Marker     string
	Include    string
	MaxResults uint
	Timeout    uint
}

func (p ListBlobsParameters) getParameters() url.Values {
	out := url.Values{}

	if p.Prefix != "" {
		out.Set("prefix", p.Prefix)
	}
	if p.Delimiter != "" {
		out.Set("delimiter", p.Delimiter)
	}
	if p.Marker != "" {
		out.Set("marker", p.Marker)
	}
	if p.Include != "" {
		out.Set("include", p.Include)
	}
	if p.MaxResults != 0 {
		out.Set("maxresults", fmt.Sprintf("%v", p.MaxResults))
	}
	if p.Timeout != 0 {
		out.Set("timeout", fmt.Sprintf("%v", p.Timeout))
	}

	return out
>>>>>>> fix godeps issue and change azure_file code due to api change
}

// BlobType defines the type of the Azure Blob.
type BlobType string

// Types of page blobs
const (
	BlobTypeBlock  BlobType = "BlockBlob"
	BlobTypePage   BlobType = "PageBlob"
	BlobTypeAppend BlobType = "AppendBlob"
)

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
func (b *Blob) buildPath() string {
	return b.Container.buildPath() + "/" + b.Name
}

// Exists returns true if a blob with given name exists on the specified
// container of the storage account.
func (b *Blob) Exists() (bool, error) {
	uri := b.Container.bsc.client.getEndpoint(blobServiceName, b.buildPath(), nil)
	headers := b.Container.bsc.client.getStandardHeaders()
	resp, err := b.Container.bsc.client.exec(http.MethodHead, uri, headers, nil, b.Container.bsc.auth)
=======
// PageWriteType defines the type updates that are going to be
// done on the page blob.
type PageWriteType string

// Types of operations on page blobs
const (
	PageWriteTypeUpdate PageWriteType = "update"
	PageWriteTypeClear  PageWriteType = "clear"
)

const (
	blobCopyStatusPending = "pending"
	blobCopyStatusSuccess = "success"
	blobCopyStatusAborted = "aborted"
	blobCopyStatusFailed  = "failed"
)

// lease constants.
const (
	leaseHeaderPrefix = "x-ms-lease-"
	headerLeaseID     = "x-ms-lease-id"
	leaseAction       = "x-ms-lease-action"
	leaseBreakPeriod  = "x-ms-lease-break-period"
	leaseDuration     = "x-ms-lease-duration"
	leaseProposedID   = "x-ms-proposed-lease-id"
	leaseTime         = "x-ms-lease-time"

	acquireLease = "acquire"
	renewLease   = "renew"
	changeLease  = "change"
	releaseLease = "release"
	breakLease   = "break"
)

// BlockListType is used to filter out types of blocks in a Get Blocks List call
// for a block blob.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179400.aspx for all
// block types.
type BlockListType string

// Filters for listing blocks in block blobs
const (
	BlockListTypeAll         BlockListType = "all"
	BlockListTypeCommitted   BlockListType = "committed"
	BlockListTypeUncommitted BlockListType = "uncommitted"
)

// ContainerAccessType defines the access level to the container from a public
// request.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179468.aspx and "x-ms-
// blob-public-access" header.
type ContainerAccessType string

// Access options for containers
const (
	ContainerAccessTypePrivate   ContainerAccessType = ""
	ContainerAccessTypeBlob      ContainerAccessType = "blob"
	ContainerAccessTypeContainer ContainerAccessType = "container"
)

// ContainerAccessPolicyDetails are used for SETTING container policies
type ContainerAccessPolicyDetails struct {
	ID         string
	StartTime  time.Time
	ExpiryTime time.Time
	CanRead    bool
	CanWrite   bool
	CanDelete  bool
}

// ContainerPermissions is used when setting permissions and Access Policies for containers.
type ContainerPermissions struct {
	AccessType     ContainerAccessType
	AccessPolicies []ContainerAccessPolicyDetails
}

// ContainerAccessHeader references header used when setting/getting container ACL
const (
	ContainerAccessHeader string = "x-ms-blob-public-access"
)

// Maximum sizes (per REST API) for various concepts
const (
	MaxBlobBlockSize = 4 * 1024 * 1024
	MaxBlobPageSize  = 4 * 1024 * 1024
)

// BlockStatus defines states a block for a block blob can
// be in.
type BlockStatus string

// List of statuses that can be used to refer to a block in a block list
const (
	BlockStatusUncommitted BlockStatus = "Uncommitted"
	BlockStatusCommitted   BlockStatus = "Committed"
	BlockStatusLatest      BlockStatus = "Latest"
)

// Block is used to create Block entities for Put Block List
// call.
type Block struct {
	ID     string
	Status BlockStatus
}

// BlockListResponse contains the response fields from Get Block List call.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179400.aspx
type BlockListResponse struct {
	XMLName           xml.Name        `xml:"BlockList"`
	CommittedBlocks   []BlockResponse `xml:"CommittedBlocks>Block"`
	UncommittedBlocks []BlockResponse `xml:"UncommittedBlocks>Block"`
}

// BlockResponse contains the block information returned
// in the GetBlockListCall.
type BlockResponse struct {
	Name string `xml:"Name"`
	Size int64  `xml:"Size"`
}

// GetPageRangesResponse contains the response fields from
// Get Page Ranges call.
//
// See https://msdn.microsoft.com/en-us/library/azure/ee691973.aspx
type GetPageRangesResponse struct {
	XMLName  xml.Name    `xml:"PageList"`
	PageList []PageRange `xml:"PageRange"`
}

// PageRange contains information about a page of a page blob from
// Get Pages Range call.
//
// See https://msdn.microsoft.com/en-us/library/azure/ee691973.aspx
type PageRange struct {
	Start int64 `xml:"Start"`
	End   int64 `xml:"End"`
}

var (
	errBlobCopyAborted    = errors.New("storage: blob copy is aborted")
	errBlobCopyIDMismatch = errors.New("storage: blob copy id is a mismatch")
)

// ListContainers returns the list of containers in a storage account along with
// pagination token and other response details.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179352.aspx
func (b BlobStorageClient) ListContainers(params ListContainersParameters) (ContainerListResponse, error) {
	q := mergeParams(params.getParameters(), url.Values{"comp": {"list"}})
	uri := b.client.getEndpoint(blobServiceName, "", q)
	headers := b.client.getStandardHeaders()

	var out ContainerListResponse
	resp, err := b.client.exec(http.MethodGet, uri, headers, nil, b.auth)
	if err != nil {
		return out, err
	}
	defer resp.body.Close()

	err = xmlUnmarshal(resp.body, &out)
	return out, err
}

// CreateContainer creates a blob container within the storage account
// with given name and access level. Returns error if container already exists.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179468.aspx
func (b BlobStorageClient) CreateContainer(name string, access ContainerAccessType) error {
	resp, err := b.createContainer(name, access)
	if err != nil {
		return err
	}
	defer resp.body.Close()
	return checkRespCode(resp.statusCode, []int{http.StatusCreated})
}

// CreateContainerIfNotExists creates a blob container if it does not exist. Returns
// true if container is newly created or false if container already exists.
func (b BlobStorageClient) CreateContainerIfNotExists(name string, access ContainerAccessType) (bool, error) {
	resp, err := b.createContainer(name, access)
	if resp != nil {
		defer resp.body.Close()
		if resp.statusCode == http.StatusCreated || resp.statusCode == http.StatusConflict {
			return resp.statusCode == http.StatusCreated, nil
		}
	}
	return false, err
}

func (b BlobStorageClient) createContainer(name string, access ContainerAccessType) (*storageResponse, error) {
	uri := b.client.getEndpoint(blobServiceName, pathForContainer(name), url.Values{"restype": {"container"}})

	headers := b.client.getStandardHeaders()
	if access != "" {
		headers[ContainerAccessHeader] = string(access)
	}
	return b.client.exec(http.MethodPut, uri, headers, nil, b.auth)
}

// ContainerExists returns true if a container with given name exists
// on the storage account, otherwise returns false.
func (b BlobStorageClient) ContainerExists(name string) (bool, error) {
	uri := b.client.getEndpoint(blobServiceName, pathForContainer(name), url.Values{"restype": {"container"}})
	headers := b.client.getStandardHeaders()

	resp, err := b.client.exec(http.MethodHead, uri, headers, nil, b.auth)
>>>>>>> fix godeps issue and change azure_file code due to api change
	if resp != nil {
		defer readAndCloseBody(resp.body)
		if resp.statusCode == http.StatusOK || resp.statusCode == http.StatusNotFound {
			return resp.statusCode == http.StatusOK, nil
		}
	}
	return false, err
}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
// GetURL gets the canonical URL to the blob with the specified name in the
// specified container. If name is not specified, the canonical URL for the entire
// container is obtained.
// This method does not create a publicly accessible URL if the blob or container
// is private and this method does not check if the blob exists.
func (b *Blob) GetURL() string {
	container := b.Container.Name
	if container == "" {
		container = "$root"
=======
// SetContainerPermissions sets up container permissions as per https://msdn.microsoft.com/en-us/library/azure/dd179391.aspx
func (b BlobStorageClient) SetContainerPermissions(container string, containerPermissions ContainerPermissions, timeout int, leaseID string) (err error) {
	params := url.Values{
		"restype": {"container"},
		"comp":    {"acl"},
	}

	if timeout > 0 {
		params.Add("timeout", strconv.Itoa(timeout))
	}

	uri := b.client.getEndpoint(blobServiceName, pathForContainer(container), params)
	headers := b.client.getStandardHeaders()
	if containerPermissions.AccessType != "" {
		headers[ContainerAccessHeader] = string(containerPermissions.AccessType)
	}

	if leaseID != "" {
		headers[headerLeaseID] = leaseID
	}

	body, length, err := generateContainerACLpayload(containerPermissions.AccessPolicies)
	headers["Content-Length"] = strconv.Itoa(length)
	resp, err := b.client.exec(http.MethodPut, uri, headers, body, b.auth)

	if err != nil {
		return err
	}

	if resp != nil {
		defer resp.body.Close()

		if resp.statusCode != http.StatusOK {
			return errors.New("Unable to set permissions")
		}
>>>>>>> fix godeps issue and change azure_file code due to api change
	}
	return b.Container.bsc.client.getEndpoint(blobServiceName, pathForResource(container, b.Name), nil)
}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
// GetBlobRangeOptions includes the options for a get blob range operation
type GetBlobRangeOptions struct {
	Range              *BlobRange
	GetRangeContentMD5 bool
	*GetBlobOptions
=======
// GetContainerPermissions gets the container permissions as per https://msdn.microsoft.com/en-us/library/azure/dd179469.aspx
// If timeout is 0 then it will not be passed to Azure
// leaseID will only be passed to Azure if populated
// Returns permissionResponse which is combined permissions and AccessPolicy
func (b BlobStorageClient) GetContainerPermissions(container string, timeout int, leaseID string) (*ContainerPermissions, error) {
	params := url.Values{"restype": {"container"},
		"comp": {"acl"}}

	if timeout > 0 {
		params.Add("timeout", strconv.Itoa(timeout))
	}

	uri := b.client.getEndpoint(blobServiceName, pathForContainer(container), params)
	headers := b.client.getStandardHeaders()

	if leaseID != "" {
		headers[headerLeaseID] = leaseID
	}

	resp, err := b.client.exec(http.MethodGet, uri, headers, nil, b.auth)
	if err != nil {
		return nil, err
	}
	defer resp.body.Close()

	var out AccessPolicy
	err = xmlUnmarshal(resp.body, &out.SignedIdentifiersList)
	if err != nil {
		return nil, err
	}

	permissionResponse := updateContainerAccessPolicy(out, &resp.headers)
	return &permissionResponse, nil
}

func updateContainerAccessPolicy(ap AccessPolicy, headers *http.Header) ContainerPermissions {
	// containerAccess. Blob, Container, empty
	containerAccess := headers.Get(http.CanonicalHeaderKey(ContainerAccessHeader))

	var cp ContainerPermissions
	cp.AccessType = ContainerAccessType(containerAccess)
	for _, policy := range ap.SignedIdentifiersList.SignedIdentifiers {
		capd := ContainerAccessPolicyDetails{
			ID:         policy.ID,
			StartTime:  policy.AccessPolicy.StartTime,
			ExpiryTime: policy.AccessPolicy.ExpiryTime,
		}
		capd.CanRead = updatePermissions(policy.AccessPolicy.Permission, "r")
		capd.CanWrite = updatePermissions(policy.AccessPolicy.Permission, "w")
		capd.CanDelete = updatePermissions(policy.AccessPolicy.Permission, "d")

		cp.AccessPolicies = append(cp.AccessPolicies, capd)
	}

	return cp
>>>>>>> fix godeps issue and change azure_file code due to api change
}

// GetBlobOptions includes the options for a get blob operation
type GetBlobOptions struct {
	Timeout           uint
	Snapshot          *time.Time
	LeaseID           string     `header:"x-ms-lease-id"`
	Origin            string     `header:"Origin"`
	IfModifiedSince   *time.Time `header:"If-Modified-Since"`
	IfUnmodifiedSince *time.Time `header:"If-Unmodified-Since"`
	IfMatch           string     `header:"If-Match"`
	IfNoneMatch       string     `header:"If-None-Match"`
	RequestID         string     `header:"x-ms-client-request-id"`
}

// BlobRange represents the bytes range to be get
type BlobRange struct {
	Start uint64
	End   uint64
}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
func (br BlobRange) String() string {
	return fmt.Sprintf("bytes=%d-%d", br.Start, br.End)
}

// Get returns a stream to read the blob. Caller must call both Read and Close()
// to correctly close the underlying connection.
// See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Get-Blob
func (b *Blob) Get(options *GetBlobOptions) (io.ReadCloser, error) {
	rangeOptions := GetBlobRangeOptions{
		GetBlobOptions: options,
	}
	resp, err := b.getRange(&rangeOptions)
=======
func (b BlobStorageClient) deleteContainer(name string) (*storageResponse, error) {
	uri := b.client.getEndpoint(blobServiceName, pathForContainer(name), url.Values{"restype": {"container"}})

	headers := b.client.getStandardHeaders()
	return b.client.exec(http.MethodDelete, uri, headers, nil, b.auth)
}

// ListBlobs returns an object that contains list of blobs in the container,
// pagination token and other information in the response of List Blobs call.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd135734.aspx
func (b BlobStorageClient) ListBlobs(container string, params ListBlobsParameters) (BlobListResponse, error) {
	q := mergeParams(params.getParameters(), url.Values{
		"restype": {"container"},
		"comp":    {"list"}})
	uri := b.client.getEndpoint(blobServiceName, pathForContainer(container), q)
	headers := b.client.getStandardHeaders()

	var out BlobListResponse
	resp, err := b.client.exec(http.MethodGet, uri, headers, nil, b.auth)
	if err != nil {
		return out, err
	}
	defer resp.body.Close()

	err = xmlUnmarshal(resp.body, &out)
	return out, err
}

// BlobExists returns true if a blob with given name exists on the specified
// container of the storage account.
func (b BlobStorageClient) BlobExists(container, name string) (bool, error) {
	uri := b.client.getEndpoint(blobServiceName, pathForBlob(container, name), url.Values{})
	headers := b.client.getStandardHeaders()
	resp, err := b.client.exec(http.MethodHead, uri, headers, nil, b.auth)
	if resp != nil {
		defer resp.body.Close()
		if resp.statusCode == http.StatusOK || resp.statusCode == http.StatusNotFound {
			return resp.statusCode == http.StatusOK, nil
		}
	}
	return false, err
}

// GetBlobURL gets the canonical URL to the blob with the specified name in the
// specified container. This method does not create a publicly accessible URL if
// the blob or container is private and this method does not check if the blob
// exists.
func (b BlobStorageClient) GetBlobURL(container, name string) string {
	if container == "" {
		container = "$root"
	}
	return b.client.getEndpoint(blobServiceName, pathForBlob(container, name), url.Values{})
}

// GetBlob returns a stream to read the blob. Caller must call Close() the
// reader to close on the underlying connection.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179440.aspx
func (b BlobStorageClient) GetBlob(container, name string) (io.ReadCloser, error) {
	resp, err := b.getBlobRange(container, name, "", nil)
>>>>>>> fix godeps issue and change azure_file code due to api change
	if err != nil {
		return nil, err
	}

	if err := checkRespCode(resp.statusCode, []int{http.StatusOK}); err != nil {
		return nil, err
	}
	if err := b.writePropoerties(resp.headers); err != nil {
		return resp.body, err
	}
	return resp.body, nil
}

// GetRange reads the specified range of a blob to a stream. The bytesRange
// string must be in a format like "0-", "10-100" as defined in HTTP 1.1 spec.
// Caller must call both Read and Close()// to correctly close the underlying
// connection.
// See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Get-Blob
func (b *Blob) GetRange(options *GetBlobRangeOptions) (io.ReadCloser, error) {
	resp, err := b.getRange(options)
	if err != nil {
		return nil, err
	}

	if err := checkRespCode(resp.statusCode, []int{http.StatusPartialContent}); err != nil {
		return nil, err
	}
	if err := b.writePropoerties(resp.headers); err != nil {
		return resp.body, err
	}
	return resp.body, nil
}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
func (b *Blob) getRange(options *GetBlobRangeOptions) (*storageResponse, error) {
	params := url.Values{}
	headers := b.Container.bsc.client.getStandardHeaders()
=======
func (b BlobStorageClient) getBlobRange(container, name, bytesRange string, extraHeaders map[string]string) (*storageResponse, error) {
	uri := b.client.getEndpoint(blobServiceName, pathForBlob(container, name), url.Values{})

	extraHeaders = b.client.protectUserAgent(extraHeaders)
	headers := b.client.getStandardHeaders()
	if bytesRange != "" {
		headers["Range"] = fmt.Sprintf("bytes=%s", bytesRange)
	}
>>>>>>> fix godeps issue and change azure_file code due to api change

	if options != nil {
		if options.Range != nil {
			headers["Range"] = options.Range.String()
			headers["x-ms-range-get-content-md5"] = fmt.Sprintf("%v", options.GetRangeContentMD5)
		}
		if options.GetBlobOptions != nil {
			headers = mergeHeaders(headers, headersFromStruct(*options.GetBlobOptions))
			params = addTimeout(params, options.Timeout)
			params = addSnapshot(params, options.Snapshot)
		}
	}
	uri := b.Container.bsc.client.getEndpoint(blobServiceName, b.buildPath(), params)

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	resp, err := b.Container.bsc.client.exec(http.MethodGet, uri, headers, nil, b.Container.bsc.auth)
=======
	resp, err := b.client.exec(http.MethodGet, uri, headers, nil, b.auth)
>>>>>>> fix godeps issue and change azure_file code due to api change
	if err != nil {
		return nil, err
	}
	return resp, err
}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
// SnapshotOptions includes the options for a snapshot blob operation
type SnapshotOptions struct {
	Timeout           uint
	LeaseID           string     `header:"x-ms-lease-id"`
	IfModifiedSince   *time.Time `header:"If-Modified-Since"`
	IfUnmodifiedSince *time.Time `header:"If-Unmodified-Since"`
	IfMatch           string     `header:"If-Match"`
	IfNoneMatch       string     `header:"If-None-Match"`
	RequestID         string     `header:"x-ms-client-request-id"`
}

// CreateSnapshot creates a snapshot for a blob
// See https://msdn.microsoft.com/en-us/library/azure/ee691971.aspx
func (b *Blob) CreateSnapshot(options *SnapshotOptions) (snapshotTimestamp *time.Time, err error) {
=======
// leasePut is common PUT code for the various acquire/release/break etc functions.
func (b BlobStorageClient) leaseCommonPut(container string, name string, headers map[string]string, expectedStatus int) (http.Header, error) {
	params := url.Values{"comp": {"lease"}}
	uri := b.client.getEndpoint(blobServiceName, pathForBlob(container, name), params)

	resp, err := b.client.exec(http.MethodPut, uri, headers, nil, b.auth)
	if err != nil {
		return nil, err
	}
	defer resp.body.Close()

	if err := checkRespCode(resp.statusCode, []int{expectedStatus}); err != nil {
		return nil, err
	}

	return resp.headers, nil
}

// SnapshotBlob creates a snapshot for a blob as per https://msdn.microsoft.com/en-us/library/azure/ee691971.aspx
func (b BlobStorageClient) SnapshotBlob(container string, name string, timeout int, extraHeaders map[string]string) (snapshotTimestamp *time.Time, err error) {
	extraHeaders = b.client.protectUserAgent(extraHeaders)
	headers := b.client.getStandardHeaders()
>>>>>>> fix godeps issue and change azure_file code due to api change
	params := url.Values{"comp": {"snapshot"}}
	headers := b.Container.bsc.client.getStandardHeaders()
	headers = b.Container.bsc.client.addMetadataToHeaders(headers, b.Metadata)

	if options != nil {
		params = addTimeout(params, options.Timeout)
		headers = mergeHeaders(headers, headersFromStruct(*options))
	}
	uri := b.Container.bsc.client.getEndpoint(blobServiceName, b.buildPath(), params)

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	resp, err := b.Container.bsc.client.exec(http.MethodPut, uri, headers, nil, b.Container.bsc.auth)
	if err != nil || resp == nil {
=======
	for k, v := range extraHeaders {
		headers[k] = v
	}

	uri := b.client.getEndpoint(blobServiceName, pathForBlob(container, name), params)
	resp, err := b.client.exec(http.MethodPut, uri, headers, nil, b.auth)
	if err != nil {
>>>>>>> fix godeps issue and change azure_file code due to api change
		return nil, err
	}
	defer readAndCloseBody(resp.body)

	if err := checkRespCode(resp.statusCode, []int{http.StatusCreated}); err != nil {
		return nil, err
	}

	snapshotResponse := resp.headers.Get(http.CanonicalHeaderKey("x-ms-snapshot"))
	if snapshotResponse != "" {
		snapshotTimestamp, err := time.Parse(time.RFC3339, snapshotResponse)
		if err != nil {
			return nil, err
		}
		return &snapshotTimestamp, nil
	}

	return nil, errors.New("Snapshot not created")
}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
// GetBlobPropertiesOptions includes the options for a get blob properties operation
type GetBlobPropertiesOptions struct {
	Timeout           uint
	Snapshot          *time.Time
	LeaseID           string     `header:"x-ms-lease-id"`
	IfModifiedSince   *time.Time `header:"If-Modified-Since"`
	IfUnmodifiedSince *time.Time `header:"If-Unmodified-Since"`
	IfMatch           string     `header:"If-Match"`
	IfNoneMatch       string     `header:"If-None-Match"`
	RequestID         string     `header:"x-ms-client-request-id"`
=======
// AcquireLease creates a lease for a blob as per https://msdn.microsoft.com/en-us/library/azure/ee691972.aspx
// returns leaseID acquired
func (b BlobStorageClient) AcquireLease(container string, name string, leaseTimeInSeconds int, proposedLeaseID string) (returnedLeaseID string, err error) {
	headers := b.client.getStandardHeaders()
	headers[leaseAction] = acquireLease

	if leaseTimeInSeconds > 0 {
		headers[leaseDuration] = strconv.Itoa(leaseTimeInSeconds)
	}

	if proposedLeaseID != "" {
		headers[leaseProposedID] = proposedLeaseID
	}

	respHeaders, err := b.leaseCommonPut(container, name, headers, http.StatusCreated)
	if err != nil {
		return "", err
	}

	returnedLeaseID = respHeaders.Get(http.CanonicalHeaderKey(headerLeaseID))

	if returnedLeaseID != "" {
		return returnedLeaseID, nil
	}

	return "", errors.New("LeaseID not returned")
}

// BreakLease breaks the lease for a blob as per https://msdn.microsoft.com/en-us/library/azure/ee691972.aspx
// Returns the timeout remaining in the lease in seconds
func (b BlobStorageClient) BreakLease(container string, name string) (breakTimeout int, err error) {
	headers := b.client.getStandardHeaders()
	headers[leaseAction] = breakLease
	return b.breakLeaseCommon(container, name, headers)
}

// BreakLeaseWithBreakPeriod breaks the lease for a blob as per https://msdn.microsoft.com/en-us/library/azure/ee691972.aspx
// breakPeriodInSeconds is used to determine how long until new lease can be created.
// Returns the timeout remaining in the lease in seconds
func (b BlobStorageClient) BreakLeaseWithBreakPeriod(container string, name string, breakPeriodInSeconds int) (breakTimeout int, err error) {
	headers := b.client.getStandardHeaders()
	headers[leaseAction] = breakLease
	headers[leaseBreakPeriod] = strconv.Itoa(breakPeriodInSeconds)
	return b.breakLeaseCommon(container, name, headers)
>>>>>>> fix godeps issue and change azure_file code due to api change
}

// GetProperties provides various information about the specified blob.
// See https://msdn.microsoft.com/en-us/library/azure/dd179394.aspx
func (b *Blob) GetProperties(options *GetBlobPropertiesOptions) error {
	params := url.Values{}
	headers := b.Container.bsc.client.getStandardHeaders()

	if options != nil {
		params = addTimeout(params, options.Timeout)
		params = addSnapshot(params, options.Snapshot)
		headers = mergeHeaders(headers, headersFromStruct(*options))
	}
	uri := b.Container.bsc.client.getEndpoint(blobServiceName, b.buildPath(), params)

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	resp, err := b.Container.bsc.client.exec(http.MethodHead, uri, headers, nil, b.Container.bsc.auth)
=======
	breakTimeoutStr := respHeaders.Get(http.CanonicalHeaderKey(leaseTime))
	if breakTimeoutStr != "" {
		breakTimeout, err = strconv.Atoi(breakTimeoutStr)
		if err != nil {
			return 0, err
		}
	}

	return breakTimeout, nil
}

// ChangeLease changes a lease ID for a blob as per https://msdn.microsoft.com/en-us/library/azure/ee691972.aspx
// Returns the new LeaseID acquired
func (b BlobStorageClient) ChangeLease(container string, name string, currentLeaseID string, proposedLeaseID string) (newLeaseID string, err error) {
	headers := b.client.getStandardHeaders()
	headers[leaseAction] = changeLease
	headers[headerLeaseID] = currentLeaseID
	headers[leaseProposedID] = proposedLeaseID

	respHeaders, err := b.leaseCommonPut(container, name, headers, http.StatusOK)
	if err != nil {
		return "", err
	}

	newLeaseID = respHeaders.Get(http.CanonicalHeaderKey(headerLeaseID))
	if newLeaseID != "" {
		return newLeaseID, nil
	}

	return "", errors.New("LeaseID not returned")
}

// ReleaseLease releases the lease for a blob as per https://msdn.microsoft.com/en-us/library/azure/ee691972.aspx
func (b BlobStorageClient) ReleaseLease(container string, name string, currentLeaseID string) error {
	headers := b.client.getStandardHeaders()
	headers[leaseAction] = releaseLease
	headers[headerLeaseID] = currentLeaseID

	_, err := b.leaseCommonPut(container, name, headers, http.StatusOK)
>>>>>>> fix godeps issue and change azure_file code due to api change
	if err != nil {
		return err
	}
	defer readAndCloseBody(resp.body)

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	if err = checkRespCode(resp.statusCode, []int{http.StatusOK}); err != nil {
=======
	return nil
}

// RenewLease renews the lease for a blob as per https://msdn.microsoft.com/en-us/library/azure/ee691972.aspx
func (b BlobStorageClient) RenewLease(container string, name string, currentLeaseID string) error {
	headers := b.client.getStandardHeaders()
	headers[leaseAction] = renewLease
	headers[headerLeaseID] = currentLeaseID

	_, err := b.leaseCommonPut(container, name, headers, http.StatusOK)
	if err != nil {
>>>>>>> fix godeps issue and change azure_file code due to api change
		return err
	}
	return b.writePropoerties(resp.headers)
}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
func (b *Blob) writePropoerties(h http.Header) error {
	var err error
=======
// GetBlobProperties provides various information about the specified
// blob. See https://msdn.microsoft.com/en-us/library/azure/dd179394.aspx
func (b BlobStorageClient) GetBlobProperties(container, name string) (*BlobProperties, error) {
	uri := b.client.getEndpoint(blobServiceName, pathForBlob(container, name), url.Values{})

	headers := b.client.getStandardHeaders()
	resp, err := b.client.exec(http.MethodHead, uri, headers, nil, b.auth)
	if err != nil {
		return nil, err
	}
	defer resp.body.Close()

	if err = checkRespCode(resp.statusCode, []int{http.StatusOK}); err != nil {
		return nil, err
	}
>>>>>>> fix godeps issue and change azure_file code due to api change

	var contentLength int64
	contentLengthStr := h.Get("Content-Length")
	if contentLengthStr != "" {
		contentLength, err = strconv.ParseInt(contentLengthStr, 0, 64)
		if err != nil {
			return err
		}
	}

	var sequenceNum int64
	sequenceNumStr := h.Get("x-ms-blob-sequence-number")
	if sequenceNumStr != "" {
		sequenceNum, err = strconv.ParseInt(sequenceNumStr, 0, 64)
		if err != nil {
			return err
		}
	}

	lastModified, err := getTimeFromHeaders(h, "Last-Modified")
	if err != nil {
		return err
	}

	copyCompletionTime, err := getTimeFromHeaders(h, "x-ms-copy-completion-time")
	if err != nil {
		return err
	}

	b.Properties = BlobProperties{
		LastModified:          TimeRFC1123(*lastModified),
		Etag:                  h.Get("Etag"),
		ContentMD5:            h.Get("Content-MD5"),
		ContentLength:         contentLength,
		ContentEncoding:       h.Get("Content-Encoding"),
		ContentType:           h.Get("Content-Type"),
		ContentDisposition:    h.Get("Content-Disposition"),
		CacheControl:          h.Get("Cache-Control"),
		ContentLanguage:       h.Get("Content-Language"),
		SequenceNumber:        sequenceNum,
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
		CopyCompletionTime:    TimeRFC1123(*copyCompletionTime),
		CopyStatusDescription: h.Get("x-ms-copy-status-description"),
		CopyID:                h.Get("x-ms-copy-id"),
		CopyProgress:          h.Get("x-ms-copy-progress"),
		CopySource:            h.Get("x-ms-copy-source"),
		CopyStatus:            h.Get("x-ms-copy-status"),
		BlobType:              BlobType(h.Get("x-ms-blob-type")),
		LeaseStatus:           h.Get("x-ms-lease-status"),
		LeaseState:            h.Get("x-ms-lease-state"),
	}
	b.writeMetadata(h)
	return nil
}

// SetBlobPropertiesOptions contains various properties of a blob and is an entry
// in SetProperties
type SetBlobPropertiesOptions struct {
	Timeout              uint
	LeaseID              string     `header:"x-ms-lease-id"`
	Origin               string     `header:"Origin"`
	IfModifiedSince      *time.Time `header:"If-Modified-Since"`
	IfUnmodifiedSince    *time.Time `header:"If-Unmodified-Since"`
	IfMatch              string     `header:"If-Match"`
	IfNoneMatch          string     `header:"If-None-Match"`
	SequenceNumberAction *SequenceNumberAction
	RequestID            string `header:"x-ms-client-request-id"`
=======
		CopyCompletionTime:    resp.headers.Get("x-ms-copy-completion-time"),
		CopyStatusDescription: resp.headers.Get("x-ms-copy-status-description"),
		CopyID:                resp.headers.Get("x-ms-copy-id"),
		CopyProgress:          resp.headers.Get("x-ms-copy-progress"),
		CopySource:            resp.headers.Get("x-ms-copy-source"),
		CopyStatus:            resp.headers.Get("x-ms-copy-status"),
		BlobType:              BlobType(resp.headers.Get("x-ms-blob-type")),
		LeaseStatus:           resp.headers.Get("x-ms-lease-status"),
		LeaseState:            resp.headers.Get("x-ms-lease-state"),
	}, nil
>>>>>>> fix godeps issue and change azure_file code due to api change
}

// SequenceNumberAction defines how the blob's sequence number should be modified
type SequenceNumberAction string

// Options for sequence number action
const (
	SequenceNumberActionMax       SequenceNumberAction = "max"
	SequenceNumberActionUpdate    SequenceNumberAction = "update"
	SequenceNumberActionIncrement SequenceNumberAction = "increment"
)

// SetProperties replaces the BlobHeaders for the specified blob.
//
// Some keys may be converted to Camel-Case before sending. All keys
// are returned in lower case by GetBlobProperties. HTTP header names
// are case-insensitive so case munging should not matter to other
// applications either.
//
// See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Set-Blob-Properties
func (b *Blob) SetProperties(options *SetBlobPropertiesOptions) error {
	params := url.Values{"comp": {"properties"}}
	headers := b.Container.bsc.client.getStandardHeaders()
	headers = mergeHeaders(headers, headersFromStruct(b.Properties))

	if options != nil {
		params = addTimeout(params, options.Timeout)
		headers = mergeHeaders(headers, headersFromStruct(*options))
	}
	uri := b.Container.bsc.client.getEndpoint(blobServiceName, b.buildPath(), params)

	if b.Properties.BlobType == BlobTypePage {
		headers = addToHeaders(headers, "x-ms-blob-content-length", fmt.Sprintf("byte %v", b.Properties.ContentLength))
		if options != nil || options.SequenceNumberAction != nil {
			headers = addToHeaders(headers, "x-ms-sequence-number-action", string(*options.SequenceNumberAction))
			if *options.SequenceNumberAction != SequenceNumberActionIncrement {
				headers = addToHeaders(headers, "x-ms-blob-sequence-number", fmt.Sprintf("%v", b.Properties.SequenceNumber))
			}
		}
	}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	resp, err := b.Container.bsc.client.exec(http.MethodPut, uri, headers, nil, b.Container.bsc.auth)
=======
	resp, err := b.client.exec(http.MethodPut, uri, headers, nil, b.auth)
>>>>>>> fix godeps issue and change azure_file code due to api change
	if err != nil {
		return err
	}
	readAndCloseBody(resp.body)
	return checkRespCode(resp.statusCode, []int{http.StatusOK})
}

// SetBlobMetadataOptions includes the options for a set blob metadata operation
type SetBlobMetadataOptions struct {
	Timeout           uint
	LeaseID           string     `header:"x-ms-lease-id"`
	IfModifiedSince   *time.Time `header:"If-Modified-Since"`
	IfUnmodifiedSince *time.Time `header:"If-Unmodified-Since"`
	IfMatch           string     `header:"If-Match"`
	IfNoneMatch       string     `header:"If-None-Match"`
	RequestID         string     `header:"x-ms-client-request-id"`
}

// SetMetadata replaces the metadata for the specified blob.
//
// Some keys may be converted to Camel-Case before sending. All keys
// are returned in lower case by GetBlobMetadata. HTTP header names
// are case-insensitive so case munging should not matter to other
// applications either.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179414.aspx
func (b *Blob) SetMetadata(options *SetBlobMetadataOptions) error {
	params := url.Values{"comp": {"metadata"}}
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	headers := b.Container.bsc.client.getStandardHeaders()
	headers = b.Container.bsc.client.addMetadataToHeaders(headers, b.Metadata)
=======
	uri := b.client.getEndpoint(blobServiceName, pathForBlob(container, name), params)
	metadata = b.client.protectUserAgent(metadata)
	extraHeaders = b.client.protectUserAgent(extraHeaders)
	headers := b.client.getStandardHeaders()
	for k, v := range metadata {
		headers[userDefinedMetadataHeaderPrefix+k] = v
	}
>>>>>>> fix godeps issue and change azure_file code due to api change

	if options != nil {
		params = addTimeout(params, options.Timeout)
		headers = mergeHeaders(headers, headersFromStruct(*options))
	}
	uri := b.Container.bsc.client.getEndpoint(blobServiceName, b.buildPath(), params)

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	resp, err := b.Container.bsc.client.exec(http.MethodPut, uri, headers, nil, b.Container.bsc.auth)
=======
	resp, err := b.client.exec(http.MethodPut, uri, headers, nil, b.auth)
>>>>>>> fix godeps issue and change azure_file code due to api change
	if err != nil {
		return err
	}
	readAndCloseBody(resp.body)
	return checkRespCode(resp.statusCode, []int{http.StatusOK})
}

// GetBlobMetadataOptions includes the options for a get blob metadata operation
type GetBlobMetadataOptions struct {
	Timeout           uint
	Snapshot          *time.Time
	LeaseID           string     `header:"x-ms-lease-id"`
	IfModifiedSince   *time.Time `header:"If-Modified-Since"`
	IfUnmodifiedSince *time.Time `header:"If-Unmodified-Since"`
	IfMatch           string     `header:"If-Match"`
	IfNoneMatch       string     `header:"If-None-Match"`
	RequestID         string     `header:"x-ms-client-request-id"`
}

// GetMetadata returns all user-defined metadata for the specified blob.
//
// All metadata keys will be returned in lower case. (HTTP header
// names are case-insensitive.)
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179414.aspx
func (b *Blob) GetMetadata(options *GetBlobMetadataOptions) error {
	params := url.Values{"comp": {"metadata"}}
	headers := b.Container.bsc.client.getStandardHeaders()

	if options != nil {
		params = addTimeout(params, options.Timeout)
		params = addSnapshot(params, options.Snapshot)
		headers = mergeHeaders(headers, headersFromStruct(*options))
	}
	uri := b.Container.bsc.client.getEndpoint(blobServiceName, b.buildPath(), params)

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	resp, err := b.Container.bsc.client.exec(http.MethodGet, uri, headers, nil, b.Container.bsc.auth)
=======
	resp, err := b.client.exec(http.MethodGet, uri, headers, nil, b.auth)
>>>>>>> fix godeps issue and change azure_file code due to api change
	if err != nil {
		return err
	}
	defer readAndCloseBody(resp.body)

	if err := checkRespCode(resp.statusCode, []int{http.StatusOK}); err != nil {
		return err
	}

	b.writeMetadata(resp.headers)
	return nil
}

func (b *Blob) writeMetadata(h http.Header) {
	metadata := make(map[string]string)
	for k, v := range h {
		// Can't trust CanonicalHeaderKey() to munge case
		// reliably. "_" is allowed in identifiers:
		// https://msdn.microsoft.com/en-us/library/azure/dd179414.aspx
		// https://msdn.microsoft.com/library/aa664670(VS.71).aspx
		// http://tools.ietf.org/html/rfc7230#section-3.2
		// ...but "_" is considered invalid by
		// CanonicalMIMEHeaderKey in
		// https://golang.org/src/net/textproto/reader.go?s=14615:14659#L542
		// so k can be "X-Ms-Meta-Lol" or "x-ms-meta-lol_rofl".
		k = strings.ToLower(k)
		if len(v) == 0 || !strings.HasPrefix(k, strings.ToLower(userDefinedMetadataHeaderPrefix)) {
			continue
		}
		// metadata["lol"] = content of the last X-Ms-Meta-Lol header
		k = k[len(userDefinedMetadataHeaderPrefix):]
		metadata[k] = v[len(v)-1]
	}
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07

	b.Metadata = BlobMetadata(metadata)
}

// DeleteBlobOptions includes the options for a delete blob operation
type DeleteBlobOptions struct {
	Timeout           uint
	Snapshot          *time.Time
	LeaseID           string `header:"x-ms-lease-id"`
	DeleteSnapshots   *bool
	IfModifiedSince   *time.Time `header:"If-Modified-Since"`
	IfUnmodifiedSince *time.Time `header:"If-Unmodified-Since"`
	IfMatch           string     `header:"If-Match"`
	IfNoneMatch       string     `header:"If-None-Match"`
	RequestID         string     `header:"x-ms-client-request-id"`
}

// Delete deletes the given blob from the specified container.
=======
	return metadata, nil
}

// CreateBlockBlob initializes an empty block blob with no blocks.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179451.aspx
func (b BlobStorageClient) CreateBlockBlob(container, name string) error {
	return b.CreateBlockBlobFromReader(container, name, 0, nil, nil)
}

// CreateBlockBlobFromReader initializes a block blob using data from
// reader. Size must be the number of bytes read from reader. To
// create an empty blob, use size==0 and reader==nil.
//
// The API rejects requests with size > 64 MiB (but this limit is not
// checked by the SDK). To write a larger blob, use CreateBlockBlob,
// PutBlock, and PutBlockList.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179451.aspx
func (b BlobStorageClient) CreateBlockBlobFromReader(container, name string, size uint64, blob io.Reader, extraHeaders map[string]string) error {
	path := fmt.Sprintf("%s/%s", container, name)
	uri := b.client.getEndpoint(blobServiceName, path, url.Values{})
	extraHeaders = b.client.protectUserAgent(extraHeaders)
	headers := b.client.getStandardHeaders()
	headers["x-ms-blob-type"] = string(BlobTypeBlock)
	headers["Content-Length"] = fmt.Sprintf("%d", size)

	for k, v := range extraHeaders {
		headers[k] = v
	}

	resp, err := b.client.exec(http.MethodPut, uri, headers, blob, b.auth)
	if err != nil {
		return err
	}
	defer resp.body.Close()
	return checkRespCode(resp.statusCode, []int{http.StatusCreated})
}

// PutBlock saves the given data chunk to the specified block blob with
// given ID.
//
// The API rejects chunks larger than 4 MiB (but this limit is not
// checked by the SDK).
//
// See https://msdn.microsoft.com/en-us/library/azure/dd135726.aspx
func (b BlobStorageClient) PutBlock(container, name, blockID string, chunk []byte) error {
	return b.PutBlockWithLength(container, name, blockID, uint64(len(chunk)), bytes.NewReader(chunk), nil)
}

// PutBlockWithLength saves the given data stream of exactly specified size to
// the block blob with given ID. It is an alternative to PutBlocks where data
// comes as stream but the length is known in advance.
//
// The API rejects requests with size > 4 MiB (but this limit is not
// checked by the SDK).
//
// See https://msdn.microsoft.com/en-us/library/azure/dd135726.aspx
func (b BlobStorageClient) PutBlockWithLength(container, name, blockID string, size uint64, blob io.Reader, extraHeaders map[string]string) error {
	uri := b.client.getEndpoint(blobServiceName, pathForBlob(container, name), url.Values{"comp": {"block"}, "blockid": {blockID}})
	extraHeaders = b.client.protectUserAgent(extraHeaders)
	headers := b.client.getStandardHeaders()
	headers["x-ms-blob-type"] = string(BlobTypeBlock)
	headers["Content-Length"] = fmt.Sprintf("%v", size)

	for k, v := range extraHeaders {
		headers[k] = v
	}

	resp, err := b.client.exec(http.MethodPut, uri, headers, blob, b.auth)
	if err != nil {
		return err
	}

	defer resp.body.Close()
	return checkRespCode(resp.statusCode, []int{http.StatusCreated})
}

// PutBlockList saves list of blocks to the specified block blob.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179467.aspx
func (b BlobStorageClient) PutBlockList(container, name string, blocks []Block) error {
	blockListXML := prepareBlockListRequest(blocks)

	uri := b.client.getEndpoint(blobServiceName, pathForBlob(container, name), url.Values{"comp": {"blocklist"}})
	headers := b.client.getStandardHeaders()
	headers["Content-Length"] = fmt.Sprintf("%v", len(blockListXML))

	resp, err := b.client.exec(http.MethodPut, uri, headers, strings.NewReader(blockListXML), b.auth)
	if err != nil {
		return err
	}
	defer resp.body.Close()
	return checkRespCode(resp.statusCode, []int{http.StatusCreated})
}

// GetBlockList retrieves list of blocks in the specified block blob.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179400.aspx
func (b BlobStorageClient) GetBlockList(container, name string, blockType BlockListType) (BlockListResponse, error) {
	params := url.Values{"comp": {"blocklist"}, "blocklisttype": {string(blockType)}}
	uri := b.client.getEndpoint(blobServiceName, pathForBlob(container, name), params)
	headers := b.client.getStandardHeaders()

	var out BlockListResponse
	resp, err := b.client.exec(http.MethodGet, uri, headers, nil, b.auth)
	if err != nil {
		return out, err
	}
	defer resp.body.Close()

	err = xmlUnmarshal(resp.body, &out)
	return out, err
}

// PutPageBlob initializes an empty page blob with specified name and maximum
// size in bytes (size must be aligned to a 512-byte boundary). A page blob must
// be created using this method before writing pages.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179451.aspx
func (b BlobStorageClient) PutPageBlob(container, name string, size int64, extraHeaders map[string]string) error {
	path := fmt.Sprintf("%s/%s", container, name)
	uri := b.client.getEndpoint(blobServiceName, path, url.Values{})
	extraHeaders = b.client.protectUserAgent(extraHeaders)
	headers := b.client.getStandardHeaders()
	headers["x-ms-blob-type"] = string(BlobTypePage)
	headers["x-ms-blob-content-length"] = fmt.Sprintf("%v", size)

	for k, v := range extraHeaders {
		headers[k] = v
	}

	resp, err := b.client.exec(http.MethodPut, uri, headers, nil, b.auth)
	if err != nil {
		return err
	}
	defer resp.body.Close()

	return checkRespCode(resp.statusCode, []int{http.StatusCreated})
}

// PutPage writes a range of pages to a page blob or clears the given range.
// In case of 'clear' writes, given chunk is discarded. Ranges must be aligned
// with 512-byte boundaries and chunk must be of size multiplies by 512.
//
// See https://msdn.microsoft.com/en-us/library/ee691975.aspx
func (b BlobStorageClient) PutPage(container, name string, startByte, endByte int64, writeType PageWriteType, chunk []byte, extraHeaders map[string]string) error {
	path := fmt.Sprintf("%s/%s", container, name)
	uri := b.client.getEndpoint(blobServiceName, path, url.Values{"comp": {"page"}})
	extraHeaders = b.client.protectUserAgent(extraHeaders)
	headers := b.client.getStandardHeaders()
	headers["x-ms-blob-type"] = string(BlobTypePage)
	headers["x-ms-page-write"] = string(writeType)
	headers["x-ms-range"] = fmt.Sprintf("bytes=%v-%v", startByte, endByte)
	for k, v := range extraHeaders {
		headers[k] = v
	}
	var contentLength int64
	var data io.Reader
	if writeType == PageWriteTypeClear {
		contentLength = 0
		data = bytes.NewReader([]byte{})
	} else {
		contentLength = int64(len(chunk))
		data = bytes.NewReader(chunk)
	}
	headers["Content-Length"] = fmt.Sprintf("%v", contentLength)

	resp, err := b.client.exec(http.MethodPut, uri, headers, data, b.auth)
	if err != nil {
		return err
	}
	defer resp.body.Close()

	return checkRespCode(resp.statusCode, []int{http.StatusCreated})
}

// GetPageRanges returns the list of valid page ranges for a page blob.
//
// See https://msdn.microsoft.com/en-us/library/azure/ee691973.aspx
func (b BlobStorageClient) GetPageRanges(container, name string) (GetPageRangesResponse, error) {
	path := fmt.Sprintf("%s/%s", container, name)
	uri := b.client.getEndpoint(blobServiceName, path, url.Values{"comp": {"pagelist"}})
	headers := b.client.getStandardHeaders()

	var out GetPageRangesResponse
	resp, err := b.client.exec(http.MethodGet, uri, headers, nil, b.auth)
	if err != nil {
		return out, err
	}
	defer resp.body.Close()

	if err = checkRespCode(resp.statusCode, []int{http.StatusOK}); err != nil {
		return out, err
	}
	err = xmlUnmarshal(resp.body, &out)
	return out, err
}

// PutAppendBlob initializes an empty append blob with specified name. An
// append blob must be created using this method before appending blocks.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179451.aspx
func (b BlobStorageClient) PutAppendBlob(container, name string, extraHeaders map[string]string) error {
	path := fmt.Sprintf("%s/%s", container, name)
	uri := b.client.getEndpoint(blobServiceName, path, url.Values{})
	extraHeaders = b.client.protectUserAgent(extraHeaders)
	headers := b.client.getStandardHeaders()
	headers["x-ms-blob-type"] = string(BlobTypeAppend)

	for k, v := range extraHeaders {
		headers[k] = v
	}

	resp, err := b.client.exec(http.MethodPut, uri, headers, nil, b.auth)
	if err != nil {
		return err
	}
	defer resp.body.Close()

	return checkRespCode(resp.statusCode, []int{http.StatusCreated})
}

// AppendBlock appends a block to an append blob.
//
// See https://msdn.microsoft.com/en-us/library/azure/mt427365.aspx
func (b BlobStorageClient) AppendBlock(container, name string, chunk []byte, extraHeaders map[string]string) error {
	path := fmt.Sprintf("%s/%s", container, name)
	uri := b.client.getEndpoint(blobServiceName, path, url.Values{"comp": {"appendblock"}})
	extraHeaders = b.client.protectUserAgent(extraHeaders)
	headers := b.client.getStandardHeaders()
	headers["x-ms-blob-type"] = string(BlobTypeAppend)
	headers["Content-Length"] = fmt.Sprintf("%v", len(chunk))

	for k, v := range extraHeaders {
		headers[k] = v
	}

	resp, err := b.client.exec(http.MethodPut, uri, headers, bytes.NewReader(chunk), b.auth)
	if err != nil {
		return err
	}
	defer resp.body.Close()

	return checkRespCode(resp.statusCode, []int{http.StatusCreated})
}

// CopyBlob starts a blob copy operation and waits for the operation to
// complete. sourceBlob parameter must be a canonical URL to the blob (can be
// obtained using GetBlobURL method.) There is no SLA on blob copy and therefore
// this helper method works faster on smaller files.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd894037.aspx
func (b BlobStorageClient) CopyBlob(container, name, sourceBlob string) error {
	copyID, err := b.StartBlobCopy(container, name, sourceBlob)
	if err != nil {
		return err
	}

	return b.WaitForBlobCopy(container, name, copyID)
}

// StartBlobCopy starts a blob copy operation.
// sourceBlob parameter must be a canonical URL to the blob (can be
// obtained using GetBlobURL method.)
//
// See https://msdn.microsoft.com/en-us/library/azure/dd894037.aspx
func (b BlobStorageClient) StartBlobCopy(container, name, sourceBlob string) (string, error) {
	uri := b.client.getEndpoint(blobServiceName, pathForBlob(container, name), url.Values{})

	headers := b.client.getStandardHeaders()
	headers["x-ms-copy-source"] = sourceBlob

	resp, err := b.client.exec(http.MethodPut, uri, headers, nil, b.auth)
	if err != nil {
		return "", err
	}
	defer resp.body.Close()

	if err := checkRespCode(resp.statusCode, []int{http.StatusAccepted, http.StatusCreated}); err != nil {
		return "", err
	}

	copyID := resp.headers.Get("x-ms-copy-id")
	if copyID == "" {
		return "", errors.New("Got empty copy id header")
	}
	return copyID, nil
}

// AbortBlobCopy aborts a BlobCopy which has already been triggered by the StartBlobCopy function.
// copyID is generated from StartBlobCopy function.
// currentLeaseID is required IF the destination blob has an active lease on it.
// As defined in https://msdn.microsoft.com/en-us/library/azure/jj159098.aspx
func (b BlobStorageClient) AbortBlobCopy(container, name, copyID, currentLeaseID string, timeout int) error {
	params := url.Values{"comp": {"copy"}, "copyid": {copyID}}
	if timeout > 0 {
		params.Add("timeout", strconv.Itoa(timeout))
	}

	uri := b.client.getEndpoint(blobServiceName, pathForBlob(container, name), params)
	headers := b.client.getStandardHeaders()
	headers["x-ms-copy-action"] = "abort"

	if currentLeaseID != "" {
		headers[headerLeaseID] = currentLeaseID
	}

	resp, err := b.client.exec(http.MethodPut, uri, headers, nil, b.auth)
	if err != nil {
		return err
	}
	defer resp.body.Close()

	if err := checkRespCode(resp.statusCode, []int{http.StatusNoContent}); err != nil {
		return err
	}

	return nil
}

// WaitForBlobCopy loops until a BlobCopy operation is completed (or fails with error)
func (b BlobStorageClient) WaitForBlobCopy(container, name, copyID string) error {
	for {
		props, err := b.GetBlobProperties(container, name)
		if err != nil {
			return err
		}

		if props.CopyID != copyID {
			return errBlobCopyIDMismatch
		}

		switch props.CopyStatus {
		case blobCopyStatusSuccess:
			return nil
		case blobCopyStatusPending:
			continue
		case blobCopyStatusAborted:
			return errBlobCopyAborted
		case blobCopyStatusFailed:
			return fmt.Errorf("storage: blob copy failed. Id=%s Description=%s", props.CopyID, props.CopyStatusDescription)
		default:
			return fmt.Errorf("storage: unhandled blob copy status: '%s'", props.CopyStatus)
		}
	}
}

// DeleteBlob deletes the given blob from the specified container.
>>>>>>> fix godeps issue and change azure_file code due to api change
// If the blob does not exists at the time of the Delete Blob operation, it
// returns error.
// See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Delete-Blob
func (b *Blob) Delete(options *DeleteBlobOptions) error {
	resp, err := b.delete(options)
	if err != nil {
		return err
	}
	readAndCloseBody(resp.body)
	return checkRespCode(resp.statusCode, []int{http.StatusAccepted})
}

// DeleteIfExists deletes the given blob from the specified container If the
// blob is deleted with this call, returns true. Otherwise returns false.
//
// See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Delete-Blob
func (b *Blob) DeleteIfExists(options *DeleteBlobOptions) (bool, error) {
	resp, err := b.delete(options)
	if resp != nil {
		defer readAndCloseBody(resp.body)
		if resp.statusCode == http.StatusAccepted || resp.statusCode == http.StatusNotFound {
			return resp.statusCode == http.StatusAccepted, nil
		}
	}
	return false, err
}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
func (b *Blob) delete(options *DeleteBlobOptions) (*storageResponse, error) {
	params := url.Values{}
	headers := b.Container.bsc.client.getStandardHeaders()

	if options != nil {
		params = addTimeout(params, options.Timeout)
		params = addSnapshot(params, options.Snapshot)
		headers = mergeHeaders(headers, headersFromStruct(*options))
		if options.DeleteSnapshots != nil {
			if *options.DeleteSnapshots {
				headers["x-ms-delete-snapshots"] = "include"
			} else {
				headers["x-ms-delete-snapshots"] = "only"
			}
		}
	}
	uri := b.Container.bsc.client.getEndpoint(blobServiceName, b.buildPath(), params)
	return b.Container.bsc.client.exec(http.MethodDelete, uri, headers, nil, b.Container.bsc.auth)
}

// helper method to construct the path to either a blob or container
func pathForResource(container, name string) string {
	if name != "" {
		return fmt.Sprintf("/%s/%s", container, name)
	}
	return fmt.Sprintf("/%s", container)
}
=======
func (b BlobStorageClient) deleteBlob(container, name string, extraHeaders map[string]string) (*storageResponse, error) {
	uri := b.client.getEndpoint(blobServiceName, pathForBlob(container, name), url.Values{})
	extraHeaders = b.client.protectUserAgent(extraHeaders)
	headers := b.client.getStandardHeaders()
	for k, v := range extraHeaders {
		headers[k] = v
	}

	return b.client.exec(http.MethodDelete, uri, headers, nil, b.auth)
}

// helper method to construct the path to a container given its name
func pathForContainer(name string) string {
	return fmt.Sprintf("/%s", name)
}

// helper method to construct the path to a blob given its container and blob
// name
func pathForBlob(container, name string) string {
	return fmt.Sprintf("/%s/%s", container, name)
}

// GetBlobSASURIWithSignedIPAndProtocol creates an URL to the specified blob which contains the Shared
// Access Signature with specified permissions and expiration time. Also includes signedIPRange and allowed protocols.
// If old API version is used but no signedIP is passed (ie empty string) then this should still work.
// We only populate the signedIP when it non-empty.
//
// See https://msdn.microsoft.com/en-us/library/azure/ee395415.aspx
func (b BlobStorageClient) GetBlobSASURIWithSignedIPAndProtocol(container, name string, expiry time.Time, permissions string, signedIPRange string, HTTPSOnly bool) (string, error) {
	var (
		signedPermissions = permissions
		blobURL           = b.GetBlobURL(container, name)
	)
	canonicalizedResource, err := b.client.buildCanonicalizedResource(blobURL, b.auth)
	if err != nil {
		return "", err
	}

	// "The canonicalizedresouce portion of the string is a canonical path to the signed resource.
	// It must include the service name (blob, table, queue or file) for version 2015-02-21 or
	// later, the storage account name, and the resource name, and must be URL-decoded.
	// -- https://msdn.microsoft.com/en-us/library/azure/dn140255.aspx

	// We need to replace + with %2b first to avoid being treated as a space (which is correct for query strings, but not the path component).
	canonicalizedResource = strings.Replace(canonicalizedResource, "+", "%2b", -1)
	canonicalizedResource, err = url.QueryUnescape(canonicalizedResource)
	if err != nil {
		return "", err
	}

	signedExpiry := expiry.UTC().Format(time.RFC3339)
	signedResource := "b"

	protocols := "https,http"
	if HTTPSOnly {
		protocols = "https"
	}
	stringToSign, err := blobSASStringToSign(b.client.apiVersion, canonicalizedResource, signedExpiry, signedPermissions, signedIPRange, protocols)
	if err != nil {
		return "", err
	}

	sig := b.client.computeHmac256(stringToSign)
	sasParams := url.Values{
		"sv":  {b.client.apiVersion},
		"se":  {signedExpiry},
		"sr":  {signedResource},
		"sp":  {signedPermissions},
		"sig": {sig},
	}

	if b.client.apiVersion >= "2015-04-05" {
		sasParams.Add("spr", protocols)
		if signedIPRange != "" {
			sasParams.Add("sip", signedIPRange)
		}
	}

	sasURL, err := url.Parse(blobURL)
	if err != nil {
		return "", err
	}
	sasURL.RawQuery = sasParams.Encode()
	return sasURL.String(), nil
}

// GetBlobSASURI creates an URL to the specified blob which contains the Shared
// Access Signature with specified permissions and expiration time.
//
// See https://msdn.microsoft.com/en-us/library/azure/ee395415.aspx
func (b BlobStorageClient) GetBlobSASURI(container, name string, expiry time.Time, permissions string) (string, error) {
	url, err := b.GetBlobSASURIWithSignedIPAndProtocol(container, name, expiry, permissions, "", false)
	return url, err
}

func blobSASStringToSign(signedVersion, canonicalizedResource, signedExpiry, signedPermissions string, signedIP string, protocols string) (string, error) {
	var signedStart, signedIdentifier, rscc, rscd, rsce, rscl, rsct string

	if signedVersion >= "2015-02-21" {
		canonicalizedResource = "/blob" + canonicalizedResource
	}

	// https://msdn.microsoft.com/en-us/library/azure/dn140255.aspx#Anchor_12
	if signedVersion >= "2015-04-05" {
		return fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s", signedPermissions, signedStart, signedExpiry, canonicalizedResource, signedIdentifier, signedIP, protocols, signedVersion, rscc, rscd, rsce, rscl, rsct), nil
	}

	// reference: http://msdn.microsoft.com/en-us/library/azure/dn140255.aspx
	if signedVersion >= "2013-08-15" {
		return fmt.Sprintf("%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s\n%s", signedPermissions, signedStart, signedExpiry, canonicalizedResource, signedIdentifier, signedVersion, rscc, rscd, rsce, rscl, rsct), nil
	}

	return "", errors.New("storage: not implemented SAS for versions earlier than 2013-08-15")
}

func generateContainerACLpayload(policies []ContainerAccessPolicyDetails) (io.Reader, int, error) {
	sil := SignedIdentifiers{
		SignedIdentifiers: []SignedIdentifier{},
	}
	for _, capd := range policies {
		permission := capd.generateContainerPermissions()
		signedIdentifier := convertAccessPolicyToXMLStructs(capd.ID, capd.StartTime, capd.ExpiryTime, permission)
		sil.SignedIdentifiers = append(sil.SignedIdentifiers, signedIdentifier)
	}
	return xmlMarshal(sil)
}

func (capd *ContainerAccessPolicyDetails) generateContainerPermissions() (permissions string) {
	// generate the permissions string (rwd).
	// still want the end user API to have bool flags.
	permissions = ""

	if capd.CanRead {
		permissions += "r"
	}

	if capd.CanWrite {
		permissions += "w"
	}

	if capd.CanDelete {
		permissions += "d"
	}

	return permissions
}
>>>>>>> fix godeps issue and change azure_file code due to api change
