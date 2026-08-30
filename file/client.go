package file

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/punk-one/edge-service-sdk/internal/atomicfile"
)

const defaultMaxDownloadBytes int64 = 1 << 30

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
		client:           &http.Client{Timeout: 10 * time.Minute},
		maxDownloadBytes: defaultMaxDownloadBytes,
	}
}

// NewClientWithMaxDownloadSize creates a client with an explicit download
// limit. Non-positive values retain the 1 GiB safety default.
func NewClientWithMaxDownloadSize(maxBytes int64) Client {
	if maxBytes <= 0 {
		maxBytes = defaultMaxDownloadBytes
	}
	return &httpClient{client: &http.Client{Timeout: 10 * time.Minute}, maxDownloadBytes: maxBytes}
}

type httpClient struct {
	client           *http.Client
	maxDownloadBytes int64
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
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
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

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("download failed: HTTP %s", resp.Status)
	}
	maxBytes := c.maxDownloadBytes
	if maxBytes <= 0 {
		maxBytes = defaultMaxDownloadBytes
	}
	if resp.ContentLength > maxBytes {
		return fmt.Errorf("download exceeds %d byte limit", maxBytes)
	}
	if err := os.MkdirAll(filepath.Dir(targetPath), 0o755); err != nil {
		return fmt.Errorf("create target directory: %w", err)
	}
	out, err := os.CreateTemp(filepath.Dir(targetPath), ".download-*.tmp")
	if err != nil {
		return fmt.Errorf("create temporary target file: %w", err)
	}
	temporary := out.Name()
	committed := false
	defer func() {
		_ = out.Close()
		if !committed {
			_ = os.Remove(temporary)
		}
	}()
	written, err := io.Copy(out, io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return fmt.Errorf("write target file: %w", err)
	}
	if written > maxBytes {
		return fmt.Errorf("download exceeds %d byte limit", maxBytes)
	}
	if err := out.Sync(); err != nil {
		return fmt.Errorf("flush target file: %w", err)
	}
	if err := out.Close(); err != nil {
		return fmt.Errorf("close target file: %w", err)
	}
	if err := atomicfile.Replace(temporary, targetPath); err != nil {
		return fmt.Errorf("commit target file: %w", err)
	}
	committed = true
	return nil
}
