package httputil

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
)

// DoJSON executes an HTTP request with optional JSON body and response
// decoding. body can be nil (no request body). result can be nil (response
// body is discarded). Returns the HTTP status code. Non-2xx status codes
// are NOT treated as errors — the caller decides what status codes are
// acceptable. An error is returned only for transport/marshaling failures.
func DoJSON(ctx context.Context, client *http.Client, method, url string,
	headers map[string]string, body, result interface{}) (int, error) {

	var bodyReader io.Reader
	if body != nil {
		data, err := json.Marshal(body)
		if err != nil {
			return 0, fmt.Errorf("marshal request body: %w", err)
		}
		bodyReader = bytes.NewReader(data)
	}

	req, err := http.NewRequestWithContext(ctx, method, url, bodyReader)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("execute request: %w", err)
	}
	defer resp.Body.Close()

	if result != nil && resp.ContentLength != 0 {
		respBody, err := io.ReadAll(resp.Body)
		if err != nil {
			return resp.StatusCode, fmt.Errorf("read response body: %w", err)
		}
		if len(respBody) > 0 {
			if err := json.Unmarshal(respBody, result); err != nil {
				return resp.StatusCode, fmt.Errorf("unmarshal response: %w", err)
			}
		}
	} else {
		io.Copy(io.Discard, resp.Body)
	}

	return resp.StatusCode, nil
}
