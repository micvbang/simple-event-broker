package seb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/micvbang/simple-event-broker/internal/recordbatch"
)

var ErrNotAuthorized = errors.New("not authorized")

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
		client:  &http.Client{},
		baseURL: bURL,
		apiKey:  apiKey,
	}, nil
}

func (c *RecordClient) Add(topicName string, record []byte) error {
	req, err := c.request("POST", "/record", bytes.NewBuffer(record))
	if err != nil {
		return fmt.Errorf("creating request: %w", err)
	}

	c.addQueryParams(req, map[string]string{"topic-name": topicName})

	res, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("sending request: %w", err)
	}

	err = c.statusCode(res)
	if err != nil {
		return err
	}

	return nil
}

func (c *RecordClient) Get(topicName string, recordID uint64) (recordbatch.Record, error) {
	req, err := c.request("GET", "/record", nil)
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}

	c.addQueryParams(req, map[string]string{
		"topic-name": topicName,
		"record-id":  fmt.Sprintf("%d", recordID),
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

	return recordbatch.Record(buf), nil
}

func (c *RecordClient) addQueryParams(r *http.Request, params map[string]string) {
	url := r.URL

	query := url.Query()
	for key, value := range params {
		query.Add(key, value)
	}
	url.RawQuery = query.Encode()
}

func (c *RecordClient) statusCode(res *http.Response) error {
	if res.StatusCode == http.StatusUnauthorized {
		return fmt.Errorf("status code %d: %w", res.StatusCode, ErrNotAuthorized)
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
