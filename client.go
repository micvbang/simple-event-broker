package seb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"time"

	"github.com/micvbang/go-helpy/sizey"
	"github.com/micvbang/go-helpy/syncy"
	"github.com/micvbang/simple-event-broker/internal/infrastructure/httphelpers"
	"github.com/micvbang/simple-event-broker/internal/sebrecords"
	"github.com/micvbang/simple-event-broker/seberr"
)

type RecordClient struct {
	client  *http.Client
	bufPool *syncy.Pool[*bytes.Buffer]
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
		bufPool: syncy.NewPool(func() *bytes.Buffer {
			return bytes.NewBuffer(make([]byte, 5*sizey.MB))
		}),
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

	err = c.statusCode(res.StatusCode)
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

	err = c.statusCode(res.StatusCode)
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

	// Buffer is used as the backing storage for the returned records, and
	// therefore limits how many bytes are requested.
	//
	// NOTE: since this will be the backing storage for the returned records
	// (slice-of-byte-slices), it therefore must not be reused until the records
	// are not used anymore.
	//
	// Defaults to 1 MiB.
	Buffer []byte

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

	if input.Buffer == nil {
		input.Buffer = make([]byte, 0, sizey.MB)
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
		"max-bytes":   fmt.Sprintf("%d", cap(input.Buffer)),
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

	err = c.statusCode(res.StatusCode)
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

	batch := sebrecords.NewBatch(make([]uint32, 0, input.MaxRecords), input.Buffer)
	err = httphelpers.MultipartFormDataToRecords(res.Body, params["boundary"], &batch)
	if err != nil {
		// NOTE: when partial content is received the request was accepted by
		// the server, and the ErrBadInput error is just telling us that there's
		// no data in the response.
		if res.StatusCode == http.StatusPartialContent && errors.Is(err, seberr.ErrBadInput) {
			return batch.IndividualRecords(), nil
		}

		return nil, fmt.Errorf("parsing multipart form data: %w", err)
	}
	return batch.IndividualRecords(), nil
}

// CloseIdleConnections closes unused, idle connections on the underlying
// http.Client.
func (c *RecordClient) CloseIdleConnections() {
	c.client.CloseIdleConnections()
}

func (c *RecordClient) statusCode(statusCode int) error {
	switch statusCode {
	case http.StatusUnauthorized:
		return fmt.Errorf("status code %d: %w", statusCode, seberr.ErrNotAuthorized)
	case http.StatusNotFound:
		return fmt.Errorf("status code %d: %w", statusCode, seberr.ErrNotFound)
	case http.StatusRequestEntityTooLarge:
		return fmt.Errorf("status code %d: %w", statusCode, seberr.ErrPayloadTooLarge)
	default:
		return nil
	}
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
