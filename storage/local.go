// Package storage - local.go implements a local filesystem storage backend.
// This simulates S3's object-store semantics using local files, making
// development and testing possible without AWS credentials.
package storage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// LocalBackend stores objects as files on the local filesystem.
// Each object key maps to a file path under the base directory.
type LocalBackend struct {
	baseDir string
}

// NewLocalBackend creates a new local filesystem backend.
func NewLocalBackend(baseDir string) (*LocalBackend, error) {
	if err := os.MkdirAll(baseDir, 0755); err != nil {
		return nil, fmt.Errorf("creating base dir: %w", err)
	}
	return &LocalBackend{baseDir: baseDir}, nil
}

func (b *LocalBackend) path(key string) string {
	return filepath.Join(b.baseDir, key)
}

func (b *LocalBackend) Get(_ context.Context, key string) ([]byte, error) {
	data, err := os.ReadFile(b.path(key))
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	return data, err
}

func (b *LocalBackend) GetRange(_ context.Context, key string, offset, length int64) ([]byte, error) {
	f, err := os.Open(b.path(key))
	if os.IsNotExist(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()

	buf := make([]byte, length)
	n, err := f.ReadAt(buf, offset)
	if err != nil && n == 0 {
		return nil, err
	}
	return buf[:n], nil
}

func (b *LocalBackend) Put(_ context.Context, key string, data []byte) error {
	p := b.path(key)
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	return os.WriteFile(p, data, 0644)
}

func (b *LocalBackend) Delete(_ context.Context, key string) error {
	err := os.Remove(b.path(key))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

func (b *LocalBackend) List(_ context.Context, prefix string) ([]string, error) {
	var keys []string
	root := b.baseDir

	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		// Normalize path separators to forward slashes (S3-style)
		rel = filepath.ToSlash(rel)
		if strings.HasPrefix(rel, prefix) {
			keys = append(keys, rel)
		}
		return nil
	})
	return keys, err
}

func (b *LocalBackend) Exists(_ context.Context, key string) (bool, error) {
	_, err := os.Stat(b.path(key))
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}
