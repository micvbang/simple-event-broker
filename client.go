package seb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/url"
	"time"

	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/seberr"
)

type RecordClient struct {
	client  *http.Client
	baseURL *url.URL
	apiKey  string
}

// NewRecordClient initializes and returns a *RecordClient.
func NewRecordClient(baseURL string, apiKey string) (*RecordClient, error) {
	bURL, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("parsing base url: %w", err)
	}

	return &RecordClient{
		client: &http.Client{
			Transport: &http.Transport{
				MaxIdleConnsPerHost: 10,
				IdleConnTimeout:     time.Second,
			},
		},
		baseURL: bURL,
		apiKey:  apiKey,
	}, nil
}

func (c *RecordClient) AddRecords(topicName string, recordSizes []uint32, recordsData []byte) error {
	buf := bytes.NewBuffer(make([]byte, 0, len(recordsData)+4096))
	contentType, err := httphelpers.RecordsToMultipartFormData(buf, recordSizes, recordsData)
	if err != nil {
		return err
	}

	req, err := c.request("POST", "/records", buf)
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}
	req.Header.Add("Content-Type", contentType)
	httphelpers.AddQueryParams(req, map[string]string{"topic-name": topicName})

	res, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}
	defer res.Body.Close()
	io.Copy(io.Discard, res.Body)

	err = c.statusCode(res)
	if err != nil {
		return err
	}

	return nil
}

func (c *RecordClient) GetRecord(topicName string, offset uint64) ([]byte, error) {
	req, err := c.request("GET", "/record", nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	httphelpers.AddQueryParams(req, map[string]string{
		"topic-name": topicName,
		"offset":     fmt.Sprintf("%d", offset),
	})

	res, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}
	defer res.Body.Close()

	err = c.statusCode(res)
	if err != nil {
		return nil, err
	}

	buf, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, fmt.Errorf("reading body: %w", err)
	}

	return buf, nil
}

type GetRecordsInput struct {
	// MaxRecords is the maximum number of records to return. Defaults to 10
	MaxRecords int

	// SoftMaxBytes is the maximum number of bytes to return. If the first
	// record is larger than SoftMaxBytes, that record is returned anyway.
	// Defaults to infinity.
	SoftMaxBytes int

	// Timeout is the amount of time to allow the server (on the server side) to
	// collect records for. If this timeout is exceeded, the number of records
	// collected so far will be returned. Defaults to 10s.
	Timeout time.Duration
}

const multipartFormData = "multipart/form-data"

func (c *RecordClient) GetRecords(topicName string, offset uint64, input GetRecordsInput) ([][]byte, error) {
	if input.MaxRecords == 0 {
		input.MaxRecords = 10
	}

	req, err := c.request("GET", "/records", nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Add("Accept", "multipart/form-data")

	httphelpers.AddQueryParams(req, map[string]string{
		"topic-name":  topicName,
		"offset":      fmt.Sprintf("%d", offset),
		"max-records": fmt.Sprintf("%d", input.MaxRecords),
		"max-bytes":   fmt.Sprintf("%d", input.SoftMaxBytes),
	})

	if input.Timeout != 0 {
		httphelpers.AddQueryParams(req, map[string]string{
			"timeout": input.Timeout.String(),
		})
	}

	res, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending request: %w", err)
	}
	defer res.Body.Close()

	err = c.statusCode(res)
	if err != nil {
		return nil, err
	}

	mediaType, params, err := mime.ParseMediaType(res.Header.Get("Content-Type"))
	if err != nil {
		return nil, fmt.Errorf("parsing media type: %w", err)
	}
	if mediaType != multipartFormData {
		return nil, fmt.Errorf("expected mediatype '%s', got '%s'", multipartFormData, mediaType)
	}
	mr := multipart.NewReader(res.Body, params["boundary"])

	records := make([][]byte, 0, input.MaxRecords)
	for part, err := mr.NextPart(); err == nil; part, err = mr.NextPart() {
		record, err := io.ReadAll(part)
		if err != nil {
			return records, err
		}
		records = append(records, record)
	}

	return records, nil
}

// CloseIdleConnections closes unused, idle connections on the underlying
// http.Client.
func (c *RecordClient) CloseIdleConnections() {
	c.client.CloseIdleConnections()
}

func (c *RecordClient) statusCode(res *http.Response) error {
	if res.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("status code %d: %w", res.StatusCode, seberr.ErrNotAuthorized)
	}

	if res.StatusCode == http.StatusNotFound {
		return fmt.Errorf("status code %d: %w", res.StatusCode, seberr.ErrNotFound)
	}

	if res.StatusCode == http.StatusRequestEntityTooLarge {
		return fmt.Errorf("status code %d: %w", res.StatusCode, seberr.ErrPayloadTooLarge)
	}

	return nil
}

func (c *RecordClient) request(method string, path string, body io.Reader) (*http.Request, error) {
	url := c.baseURL.JoinPath(path)

	req, err := http.NewRequest(method, url.String(), body)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	req.Header.Add("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))

	return req, nil
}
