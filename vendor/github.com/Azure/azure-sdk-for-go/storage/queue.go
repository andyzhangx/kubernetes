package storage

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

const (
	// casing is per Golang's http.Header canonicalizing the header names.
	approximateMessagesCountHeader = "X-Ms-Approximate-Messages-Count"
)

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
// QueueAccessPolicy represents each access policy in the queue ACL.
type QueueAccessPolicy struct {
	ID         string
	StartTime  time.Time
	ExpiryTime time.Time
	CanRead    bool
	CanAdd     bool
	CanUpdate  bool
	CanProcess bool
=======
// QueueServiceClient contains operations for Microsoft Azure Queue Storage
// Service.
type QueueServiceClient struct {
	client Client
	auth   authentication
>>>>>>> fix godeps issue and change azure_file code due to api change
}

// QueuePermissions represents the queue ACLs.
type QueuePermissions struct {
	AccessPolicies []QueueAccessPolicy
}

// SetQueuePermissionOptions includes options for a set queue permissions operation
type SetQueuePermissionOptions struct {
	Timeout   uint
	RequestID string `header:"x-ms-client-request-id"`
}

// Queue represents an Azure queue.
type Queue struct {
	qsc               *QueueServiceClient
	Name              string
	Metadata          map[string]string
	AproxMessageCount uint64
}

func (q *Queue) buildPath() string {
	return fmt.Sprintf("/%s", q.Name)
}

func (q *Queue) buildPathMessages() string {
	return fmt.Sprintf("%s/messages", q.buildPath())
}

// QueueServiceOptions includes options for some queue service operations
type QueueServiceOptions struct {
	Timeout   uint
	RequestID string `header:"x-ms-client-request-id"`
}

// Create operation creates a queue under the given account.
//
// See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Create-Queue4
func (q *Queue) Create(options *QueueServiceOptions) error {
	params := url.Values{}
	headers := q.qsc.client.getStandardHeaders()
	headers = q.qsc.client.addMetadataToHeaders(headers, q.Metadata)

	if options != nil {
		params = addTimeout(params, options.Timeout)
		headers = mergeHeaders(headers, headersFromStruct(*options))
	}
	uri := q.qsc.client.getEndpoint(queueServiceName, q.buildPath(), params)

	resp, err := q.qsc.client.exec(http.MethodPut, uri, headers, nil, q.qsc.auth)
	if err != nil {
		return err
	}
	readAndCloseBody(resp.body)
	return checkRespCode(resp.statusCode, []int{http.StatusCreated})
}

// Delete operation permanently deletes the specified queue.
//
// See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Delete-Queue3
func (q *Queue) Delete(options *QueueServiceOptions) error {
	params := url.Values{}
	headers := q.qsc.client.getStandardHeaders()

	if options != nil {
		params = addTimeout(params, options.Timeout)
		headers = mergeHeaders(headers, headersFromStruct(*options))
	}
	uri := q.qsc.client.getEndpoint(queueServiceName, q.buildPath(), params)
	resp, err := q.qsc.client.exec(http.MethodDelete, uri, headers, nil, q.qsc.auth)
	if err != nil {
		return err
	}
	readAndCloseBody(resp.body)
	return checkRespCode(resp.statusCode, []int{http.StatusNoContent})
}

// Exists returns true if a queue with given name exists.
func (q *Queue) Exists() (bool, error) {
	uri := q.qsc.client.getEndpoint(queueServiceName, q.buildPath(), url.Values{"comp": {"metadata"}})
	resp, err := q.qsc.client.exec(http.MethodGet, uri, q.qsc.client.getStandardHeaders(), nil, q.qsc.auth)
	if resp != nil {
		defer readAndCloseBody(resp.body)
		if resp.statusCode == http.StatusOK || resp.statusCode == http.StatusNotFound {
			return resp.statusCode == http.StatusOK, nil
		}
	}
	return false, err
}

// SetMetadata operation sets user-defined metadata on the specified queue.
// Metadata is associated with the queue as name-value pairs.
//
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
// See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Set-Queue-Metadata
func (q *Queue) SetMetadata(options *QueueServiceOptions) error {
	params := url.Values{"comp": {"metadata"}}
	headers := q.qsc.client.getStandardHeaders()
	headers = q.qsc.client.addMetadataToHeaders(headers, q.Metadata)

	if options != nil {
		params = addTimeout(params, options.Timeout)
		headers = mergeHeaders(headers, headersFromStruct(*options))
=======
// See https://msdn.microsoft.com/en-us/library/azure/dd179348.aspx
func (c QueueServiceClient) SetMetadata(name string, metadata map[string]string) error {
	uri := c.client.getEndpoint(queueServiceName, pathForQueue(name), url.Values{"comp": []string{"metadata"}})
	metadata = c.client.protectUserAgent(metadata)
	headers := c.client.getStandardHeaders()
	for k, v := range metadata {
		headers[userDefinedMetadataHeaderPrefix+k] = v
>>>>>>> fix godeps issue and change azure_file code due to api change
	}
	uri := q.qsc.client.getEndpoint(queueServiceName, q.buildPath(), params)

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	resp, err := q.qsc.client.exec(http.MethodPut, uri, headers, nil, q.qsc.auth)
=======
	resp, err := c.client.exec(http.MethodPut, uri, headers, nil, c.auth)
>>>>>>> fix godeps issue and change azure_file code due to api change
	if err != nil {
		return err
	}
	readAndCloseBody(resp.body)
	return checkRespCode(resp.statusCode, []int{http.StatusNoContent})
}

// GetMetadata operation retrieves user-defined metadata and queue
// properties on the specified queue. Metadata is associated with
// the queue as name-values pairs.
//
// See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Set-Queue-Metadata
//
// Because the way Golang's http client (and http.Header in particular)
// canonicalize header names, the returned metadata names would always
// be all lower case.
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
func (q *Queue) GetMetadata(options *QueueServiceOptions) error {
	params := url.Values{"comp": {"metadata"}}
	headers := q.qsc.client.getStandardHeaders()

	if options != nil {
		params = addTimeout(params, options.Timeout)
		headers = mergeHeaders(headers, headersFromStruct(*options))
=======
func (c QueueServiceClient) GetMetadata(name string) (QueueMetadataResponse, error) {
	qm := QueueMetadataResponse{}
	qm.UserDefinedMetadata = make(map[string]string)
	uri := c.client.getEndpoint(queueServiceName, pathForQueue(name), url.Values{"comp": []string{"metadata"}})
	headers := c.client.getStandardHeaders()
	resp, err := c.client.exec(http.MethodGet, uri, headers, nil, c.auth)
	if err != nil {
		return qm, err
>>>>>>> fix godeps issue and change azure_file code due to api change
	}
	uri := q.qsc.client.getEndpoint(queueServiceName, q.buildPath(), url.Values{"comp": {"metadata"}})

	resp, err := q.qsc.client.exec(http.MethodGet, uri, headers, nil, q.qsc.auth)
	if err != nil {
		return err
	}
	defer readAndCloseBody(resp.body)

	if err := checkRespCode(resp.statusCode, []int{http.StatusOK}); err != nil {
		return err
	}

	aproxMessagesStr := resp.headers.Get(http.CanonicalHeaderKey(approximateMessagesCountHeader))
	if aproxMessagesStr != "" {
		aproxMessages, err := strconv.ParseUint(aproxMessagesStr, 10, 64)
		if err != nil {
			return err
		}
		q.AproxMessageCount = aproxMessages
	}

	q.Metadata = getMetadataFromHeaders(resp.headers)
	return nil
}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
// GetMessageReference returns a message object with the specified text.
func (q *Queue) GetMessageReference(text string) *Message {
	return &Message{
		Queue: q,
		Text:  text,
=======
// CreateQueue operation creates a queue under the given account.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179342.aspx
func (c QueueServiceClient) CreateQueue(name string) error {
	uri := c.client.getEndpoint(queueServiceName, pathForQueue(name), url.Values{})
	headers := c.client.getStandardHeaders()
	resp, err := c.client.exec(http.MethodPut, uri, headers, nil, c.auth)
	if err != nil {
		return err
>>>>>>> fix godeps issue and change azure_file code due to api change
	}
}

// GetMessagesOptions is the set of options can be specified for Get
// Messsages operation. A zero struct does not use any preferences for the
// request.
type GetMessagesOptions struct {
	Timeout           uint
	NumOfMessages     int
	VisibilityTimeout int
	RequestID         string `header:"x-ms-client-request-id"`
}

type messages struct {
	XMLName  xml.Name  `xml:"QueueMessagesList"`
	Messages []Message `xml:"QueueMessage"`
}

// GetMessages operation retrieves one or more messages from the front of the
// queue.
//
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
// See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Get-Messages
func (q *Queue) GetMessages(options *GetMessagesOptions) ([]Message, error) {
	query := url.Values{}
	headers := q.qsc.client.getStandardHeaders()

	if options != nil {
		if options.NumOfMessages != 0 {
			query.Set("numofmessages", strconv.Itoa(options.NumOfMessages))
		}
		if options.VisibilityTimeout != 0 {
			query.Set("visibilitytimeout", strconv.Itoa(options.VisibilityTimeout))
		}
		query = addTimeout(query, options.Timeout)
		headers = mergeHeaders(headers, headersFromStruct(*options))
	}
	uri := q.qsc.client.getEndpoint(queueServiceName, q.buildPathMessages(), query)

	resp, err := q.qsc.client.exec(http.MethodGet, uri, headers, nil, q.qsc.auth)
=======
// See https://msdn.microsoft.com/en-us/library/azure/dd179436.aspx
func (c QueueServiceClient) DeleteQueue(name string) error {
	uri := c.client.getEndpoint(queueServiceName, pathForQueue(name), url.Values{})
	resp, err := c.client.exec(http.MethodDelete, uri, c.client.getStandardHeaders(), nil, c.auth)
>>>>>>> fix godeps issue and change azure_file code due to api change
	if err != nil {
		return []Message{}, err
	}
	defer readAndCloseBody(resp.body)

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	var out messages
	err = xmlUnmarshal(resp.body, &out)
	if err != nil {
		return []Message{}, err
	}
	for i := range out.Messages {
		out.Messages[i].Queue = q
=======
// QueueExists returns true if a queue with given name exists.
func (c QueueServiceClient) QueueExists(name string) (bool, error) {
	uri := c.client.getEndpoint(queueServiceName, pathForQueue(name), url.Values{"comp": {"metadata"}})
	resp, err := c.client.exec(http.MethodGet, uri, c.client.getStandardHeaders(), nil, c.auth)
	if resp != nil && (resp.statusCode == http.StatusOK || resp.statusCode == http.StatusNotFound) {
		return resp.statusCode == http.StatusOK, nil
>>>>>>> fix godeps issue and change azure_file code due to api change
	}
	return out.Messages, err
}

// PeekMessagesOptions is the set of options can be specified for Peek
// Messsage operation. A zero struct does not use any preferences for the
// request.
type PeekMessagesOptions struct {
	Timeout       uint
	NumOfMessages int
	RequestID     string `header:"x-ms-client-request-id"`
}

// PeekMessages retrieves one or more messages from the front of the queue, but
// does not alter the visibility of the message.
//
// See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Peek-Messages
func (q *Queue) PeekMessages(options *PeekMessagesOptions) ([]Message, error) {
	query := url.Values{"peekonly": {"true"}} // Required for peek operation
	headers := q.qsc.client.getStandardHeaders()

	if options != nil {
		if options.NumOfMessages != 0 {
			query.Set("numofmessages", strconv.Itoa(options.NumOfMessages))
		}
		query = addTimeout(query, options.Timeout)
		headers = mergeHeaders(headers, headersFromStruct(*options))
	}
	uri := q.qsc.client.getEndpoint(queueServiceName, q.buildPathMessages(), query)

	resp, err := q.qsc.client.exec(http.MethodGet, uri, headers, nil, q.qsc.auth)
	if err != nil {
		return []Message{}, err
	}
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	defer readAndCloseBody(resp.body)

	var out messages
	err = xmlUnmarshal(resp.body, &out)
=======
	headers := c.client.getStandardHeaders()
	headers["Content-Length"] = strconv.Itoa(nn)
	resp, err := c.client.exec(http.MethodPost, uri, headers, body, c.auth)
>>>>>>> fix godeps issue and change azure_file code due to api change
	if err != nil {
		return []Message{}, err
	}
	for i := range out.Messages {
		out.Messages[i].Queue = q
	}
	return out.Messages, err
}

// ClearMessages operation deletes all messages from the specified queue.
//
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
// See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/Clear-Messages
func (q *Queue) ClearMessages(options *QueueServiceOptions) error {
	params := url.Values{}
	headers := q.qsc.client.getStandardHeaders()

	if options != nil {
		params = addTimeout(params, options.Timeout)
		headers = mergeHeaders(headers, headersFromStruct(*options))
	}
	uri := q.qsc.client.getEndpoint(queueServiceName, q.buildPathMessages(), params)

	resp, err := q.qsc.client.exec(http.MethodDelete, uri, headers, nil, q.qsc.auth)
=======
// See https://msdn.microsoft.com/en-us/library/azure/dd179454.aspx
func (c QueueServiceClient) ClearMessages(queue string) error {
	uri := c.client.getEndpoint(queueServiceName, pathForQueueMessages(queue), url.Values{})
	resp, err := c.client.exec(http.MethodDelete, uri, c.client.getStandardHeaders(), nil, c.auth)
>>>>>>> fix godeps issue and change azure_file code due to api change
	if err != nil {
		return err
	}
	readAndCloseBody(resp.body)
	return checkRespCode(resp.statusCode, []int{http.StatusNoContent})
}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
// SetPermissions sets up queue permissions
// See https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/set-queue-acl
func (q *Queue) SetPermissions(permissions QueuePermissions, options *SetQueuePermissionOptions) error {
	body, length, err := generateQueueACLpayload(permissions.AccessPolicies)
=======
// GetMessages operation retrieves one or more messages from the front of the
// queue.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179474.aspx
func (c QueueServiceClient) GetMessages(queue string, params GetMessagesParameters) (GetMessagesResponse, error) {
	var r GetMessagesResponse
	uri := c.client.getEndpoint(queueServiceName, pathForQueueMessages(queue), params.getParameters())
	resp, err := c.client.exec(http.MethodGet, uri, c.client.getStandardHeaders(), nil, c.auth)
>>>>>>> fix godeps issue and change azure_file code due to api change
	if err != nil {
		return err
	}

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	params := url.Values{
		"comp": {"acl"},
=======
// PeekMessages retrieves one or more messages from the front of the queue, but
// does not alter the visibility of the message.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179472.aspx
func (c QueueServiceClient) PeekMessages(queue string, params PeekMessagesParameters) (PeekMessagesResponse, error) {
	var r PeekMessagesResponse
	uri := c.client.getEndpoint(queueServiceName, pathForQueueMessages(queue), params.getParameters())
	resp, err := c.client.exec(http.MethodGet, uri, c.client.getStandardHeaders(), nil, c.auth)
	if err != nil {
		return r, err
>>>>>>> fix godeps issue and change azure_file code due to api change
	}
	headers := q.qsc.client.getStandardHeaders()
	headers["Content-Length"] = strconv.Itoa(length)

<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	if options != nil {
		params = addTimeout(params, options.Timeout)
		headers = mergeHeaders(headers, headersFromStruct(*options))
	}
	uri := q.qsc.client.getEndpoint(queueServiceName, q.buildPath(), params)
	resp, err := q.qsc.client.exec(http.MethodPut, uri, headers, body, q.qsc.auth)
=======
// DeleteMessage operation deletes the specified message.
//
// See https://msdn.microsoft.com/en-us/library/azure/dd179347.aspx
func (c QueueServiceClient) DeleteMessage(queue, messageID, popReceipt string) error {
	uri := c.client.getEndpoint(queueServiceName, pathForMessage(queue, messageID), url.Values{
		"popreceipt": {popReceipt}})
	resp, err := c.client.exec(http.MethodDelete, uri, c.client.getStandardHeaders(), nil, c.auth)
>>>>>>> fix godeps issue and change azure_file code due to api change
	if err != nil {
		return err
	}
	defer readAndCloseBody(resp.body)

	if err := checkRespCode(resp.statusCode, []int{http.StatusNoContent}); err != nil {
		return errors.New("Unable to set permissions")
	}

	return nil
}

func generateQueueACLpayload(policies []QueueAccessPolicy) (io.Reader, int, error) {
	sil := SignedIdentifiers{
		SignedIdentifiers: []SignedIdentifier{},
	}
	for _, qapd := range policies {
		permission := qapd.generateQueuePermissions()
		signedIdentifier := convertAccessPolicyToXMLStructs(qapd.ID, qapd.StartTime, qapd.ExpiryTime, permission)
		sil.SignedIdentifiers = append(sil.SignedIdentifiers, signedIdentifier)
	}
	return xmlMarshal(sil)
}

func (qapd *QueueAccessPolicy) generateQueuePermissions() (permissions string) {
	// generate the permissions string (raup).
	// still want the end user API to have bool flags.
	permissions = ""

	if qapd.CanRead {
		permissions += "r"
	}

	if qapd.CanAdd {
		permissions += "a"
	}

	if qapd.CanUpdate {
		permissions += "u"
	}

	if qapd.CanProcess {
		permissions += "p"
	}

	return permissions
}

// GetQueuePermissionOptions includes options for a get queue permissions operation
type GetQueuePermissionOptions struct {
	Timeout   uint
	RequestID string `header:"x-ms-client-request-id"`
}

// GetPermissions gets the queue permissions as per https://docs.microsoft.com/en-us/rest/api/storageservices/fileservices/get-queue-acl
// If timeout is 0 then it will not be passed to Azure
func (q *Queue) GetPermissions(options *GetQueuePermissionOptions) (*QueuePermissions, error) {
	params := url.Values{
		"comp": {"acl"},
	}
<<<<<<< fc3349606cf7a073eac1d4f2e805a04b7e282d07
	headers := q.qsc.client.getStandardHeaders()

	if options != nil {
		params = addTimeout(params, options.Timeout)
		headers = mergeHeaders(headers, headersFromStruct(*options))
	}
	uri := q.qsc.client.getEndpoint(queueServiceName, q.buildPath(), params)
	resp, err := q.qsc.client.exec(http.MethodGet, uri, headers, nil, q.qsc.auth)
=======
	headers := c.client.getStandardHeaders()
	headers["Content-Length"] = fmt.Sprintf("%d", nn)
	resp, err := c.client.exec(http.MethodPut, uri, headers, body, c.auth)
>>>>>>> fix godeps issue and change azure_file code due to api change
	if err != nil {
		return nil, err
	}
	defer resp.body.Close()

	var ap AccessPolicy
	err = xmlUnmarshal(resp.body, &ap.SignedIdentifiersList)
	if err != nil {
		return nil, err
	}
	return buildQueueAccessPolicy(ap, &resp.headers), nil
}

func buildQueueAccessPolicy(ap AccessPolicy, headers *http.Header) *QueuePermissions {
	permissions := QueuePermissions{
		AccessPolicies: []QueueAccessPolicy{},
	}

	for _, policy := range ap.SignedIdentifiersList.SignedIdentifiers {
		qapd := QueueAccessPolicy{
			ID:         policy.ID,
			StartTime:  policy.AccessPolicy.StartTime,
			ExpiryTime: policy.AccessPolicy.ExpiryTime,
		}
		qapd.CanRead = updatePermissions(policy.AccessPolicy.Permission, "r")
		qapd.CanAdd = updatePermissions(policy.AccessPolicy.Permission, "a")
		qapd.CanUpdate = updatePermissions(policy.AccessPolicy.Permission, "u")
		qapd.CanProcess = updatePermissions(policy.AccessPolicy.Permission, "p")

		permissions.AccessPolicies = append(permissions.AccessPolicies, qapd)
	}
	return &permissions
}
