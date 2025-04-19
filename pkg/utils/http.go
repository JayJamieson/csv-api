package utils

import (
	"fmt"
	"io"
	"net/http"
	"time"
)

var HTTPClient = &http.Client{
	Timeout: 30 * time.Second,
}

func DownloadFile(url string) (io.ReadCloser, error) {
	resp, err := HTTPClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to download file: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return resp.Body, nil
}
