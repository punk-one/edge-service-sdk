package file

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// Client provides HTTP-based file transfer capabilities using presigned URLs.
// It is not aware of MinIO/S3/Azure Blob and does not handle authorization.
type Client interface {
	// UploadByURL uploads a local file to the given presigned PUT URL.
	UploadByURL(ctx context.Context, url string, filePath string) error

	// DownloadByURL downloads a file from the given presigned GET URL to a local path.
	DownloadByURL(ctx context.Context, url string, targetPath string) error
}

// NewClient creates a new Client with a 10-minute HTTP timeout.
func NewClient() Client {
	return &httpClient{
		client: &http.Client{Timeout: 10 * time.Minute},
	}
}

type httpClient struct {
	client *http.Client
}

func (c *httpClient) UploadByURL(ctx context.Context, url string, filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("open file for upload: %w", err)
	}
	defer file.Close()

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, url, file)
	if err != nil {
		return fmt.Errorf("create upload request: %w", err)
	}
	req.ContentLength = stat.Size()
	req.Header.Set("Content-Type", "application/octet-stream")

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("upload request: %w", err)
	}
	resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("upload failed: HTTP %s", resp.Status)
	}
	return nil
}

func (c *httpClient) DownloadByURL(ctx context.Context, url string, targetPath string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("create download request: %w", err)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("download request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download failed: HTTP %s", resp.Status)
	}

	out, err := os.Create(targetPath)
	if err != nil {
		return fmt.Errorf("create target file: %w", err)
	}
	defer out.Close()

	if _, err := io.Copy(out, resp.Body); err != nil {
		return fmt.Errorf("write target file: %w", err)
	}
	return nil
}
