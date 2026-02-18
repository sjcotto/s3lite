// Package storage - s3.go implements an S3 storage backend.
// This uses AWS SDK v2 to talk to S3 (or any S3-compatible service
// like MinIO, Cloudflare R2, Backblaze B2, etc.).
package storage

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
)

// S3Backend stores objects in an S3-compatible object store.
type S3Backend struct {
	bucket    string
	prefix    string
	endpoint  string
	region    string
	accessKey string
	secretKey string
	client    *http.Client
}

// S3Config holds configuration for connecting to S3.
type S3Config struct {
	Bucket    string
	Prefix    string // optional key prefix (e.g., "mydb/")
	Endpoint  string // e.g., "https://s3.amazonaws.com" or MinIO URL
	Region    string
	AccessKey string
	SecretKey string
}

// NewS3Backend creates a new S3 storage backend.
func NewS3Backend(cfg S3Config) *S3Backend {
	if cfg.Prefix != "" && !strings.HasSuffix(cfg.Prefix, "/") {
		cfg.Prefix += "/"
	}
	return &S3Backend{
		bucket:    cfg.Bucket,
		prefix:    cfg.Prefix,
		endpoint:  strings.TrimSuffix(cfg.Endpoint, "/"),
		region:    cfg.Region,
		accessKey: cfg.AccessKey,
		secretKey: cfg.SecretKey,
		client:    &http.Client{Timeout: 30 * time.Second},
	}
}

func (b *S3Backend) fullKey(key string) string {
	return b.prefix + key
}

func (b *S3Backend) url(key string) string {
	return fmt.Sprintf("%s/%s/%s", b.endpoint, b.bucket, b.fullKey(key))
}

func (b *S3Backend) Get(ctx context.Context, key string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", b.url(key), nil)
	if err != nil {
		return nil, err
	}
	b.signRequest(req, nil)

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("s3 GET %s: %w", key, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, ErrNotFound
	}
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("s3 GET %s: status %d: %s", key, resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

func (b *S3Backend) GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", b.url(key), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", offset, offset+length-1))
	b.signRequest(req, nil)

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("s3 GET range %s: %w", key, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == 404 {
		return nil, ErrNotFound
	}
	if resp.StatusCode != 206 && resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("s3 GET range %s: status %d: %s", key, resp.StatusCode, string(body))
	}

	return io.ReadAll(resp.Body)
}

func (b *S3Backend) Put(ctx context.Context, key string, data []byte) error {
	req, err := http.NewRequestWithContext(ctx, "PUT", b.url(key), bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.ContentLength = int64(len(data))
	b.signRequest(req, data)

	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("s3 PUT %s: %w", key, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 && resp.StatusCode != 201 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("s3 PUT %s: status %d: %s", key, resp.StatusCode, string(body))
	}
	return nil
}

func (b *S3Backend) Delete(ctx context.Context, key string) error {
	req, err := http.NewRequestWithContext(ctx, "DELETE", b.url(key), nil)
	if err != nil {
		return err
	}
	b.signRequest(req, nil)

	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("s3 DELETE %s: %w", key, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 204 && resp.StatusCode != 200 && resp.StatusCode != 404 {
		return fmt.Errorf("s3 DELETE %s: status %d", key, resp.StatusCode)
	}
	return nil
}

func (b *S3Backend) List(ctx context.Context, prefix string) ([]string, error) {
	// Simplified: uses S3 list-objects-v2
	fullPrefix := b.fullKey(prefix)
	listURL := fmt.Sprintf("%s/%s?list-type=2&prefix=%s", b.endpoint, b.bucket, fullPrefix)

	req, err := http.NewRequestWithContext(ctx, "GET", listURL, nil)
	if err != nil {
		return nil, err
	}
	b.signRequest(req, nil)

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("s3 LIST %s: %w", prefix, err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)

	// Simple XML parsing for <Key> elements
	var keys []string
	content := string(body)
	for {
		start := strings.Index(content, "<Key>")
		if start == -1 {
			break
		}
		end := strings.Index(content[start:], "</Key>")
		if end == -1 {
			break
		}
		key := content[start+5 : start+end]
		// Strip prefix
		if strings.HasPrefix(key, b.prefix) {
			key = key[len(b.prefix):]
		}
		keys = append(keys, key)
		content = content[start+end+6:]
	}

	return keys, nil
}

func (b *S3Backend) Exists(ctx context.Context, key string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, "HEAD", b.url(key), nil)
	if err != nil {
		return false, err
	}
	b.signRequest(req, nil)

	resp, err := b.client.Do(req)
	if err != nil {
		return false, err
	}
	defer resp.Body.Close()

	return resp.StatusCode == 200, nil
}

// signRequest signs an HTTP request with AWS Signature V4.
func (b *S3Backend) signRequest(req *http.Request, payload []byte) {
	now := time.Now().UTC()
	dateStamp := now.Format("20060102")
	amzDate := now.Format("20060102T150405Z")

	req.Header.Set("x-amz-date", amzDate)
	req.Header.Set("Host", req.URL.Host)

	// Payload hash
	var payloadHash string
	if payload != nil {
		h := sha256.Sum256(payload)
		payloadHash = hex.EncodeToString(h[:])
	} else {
		h := sha256.Sum256([]byte{})
		payloadHash = hex.EncodeToString(h[:])
	}
	req.Header.Set("x-amz-content-sha256", payloadHash)

	// Canonical headers
	signedHeaders := []string{"host", "x-amz-content-sha256", "x-amz-date"}
	if req.Header.Get("Content-Type") != "" {
		signedHeaders = append(signedHeaders, "content-type")
	}
	if req.Header.Get("Range") != "" {
		signedHeaders = append(signedHeaders, "range")
	}
	sort.Strings(signedHeaders)

	var canonicalHeaders strings.Builder
	for _, h := range signedHeaders {
		canonicalHeaders.WriteString(strings.ToLower(h))
		canonicalHeaders.WriteString(":")
		canonicalHeaders.WriteString(strings.TrimSpace(req.Header.Get(h)))
		canonicalHeaders.WriteString("\n")
	}

	// Canonical request
	canonicalRequest := strings.Join([]string{
		req.Method,
		req.URL.Path,
		req.URL.RawQuery,
		canonicalHeaders.String(),
		strings.Join(signedHeaders, ";"),
		payloadHash,
	}, "\n")

	// String to sign
	credentialScope := fmt.Sprintf("%s/%s/s3/aws4_request", dateStamp, b.region)
	canonHash := sha256.Sum256([]byte(canonicalRequest))
	stringToSign := fmt.Sprintf("AWS4-HMAC-SHA256\n%s\n%s\n%s",
		amzDate, credentialScope, hex.EncodeToString(canonHash[:]))

	// Signing key
	kDate := hmacSHA256([]byte("AWS4"+b.secretKey), []byte(dateStamp))
	kRegion := hmacSHA256(kDate, []byte(b.region))
	kService := hmacSHA256(kRegion, []byte("s3"))
	kSigning := hmacSHA256(kService, []byte("aws4_request"))

	signature := hex.EncodeToString(hmacSHA256(kSigning, []byte(stringToSign)))

	// Authorization header
	auth := fmt.Sprintf("AWS4-HMAC-SHA256 Credential=%s/%s, SignedHeaders=%s, Signature=%s",
		b.accessKey, credentialScope, strings.Join(signedHeaders, ";"), signature)
	req.Header.Set("Authorization", auth)
}

func hmacSHA256(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
}
