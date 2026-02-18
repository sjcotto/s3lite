// Package storage defines the storage backend interface and chunk format.
// Storage backends abstract away the difference between local filesystem
// and S3, presenting a simple object-store interface.
package storage

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/sjcotto/s3lite/page"
)

const (
	ChunkMagic      = 0x53334442 // "S3DB" in little-endian
	ChunkVersion    = 1
	ChunkHeaderSize = 64
	ChunkFooterSize = 4 // CRC32
	PagesPerChunk   = 256
)

var (
	ErrChunkCorrupt = errors.New("storage: chunk data corrupted")
	ErrChunkMagic   = errors.New("storage: invalid chunk magic")
	ErrNotFound     = errors.New("storage: object not found")
)

// Backend is the interface for object storage (S3 or local filesystem).
type Backend interface {
	// Get retrieves an object by key.
	Get(ctx context.Context, key string) ([]byte, error)

	// GetRange retrieves a byte range of an object.
	GetRange(ctx context.Context, key string, offset, length int64) ([]byte, error)

	// Put stores an object.
	Put(ctx context.Context, key string, data []byte) error

	// Delete removes an object.
	Delete(ctx context.Context, key string) error

	// List lists objects with the given prefix.
	List(ctx context.Context, prefix string) ([]string, error)

	// Exists checks if an object exists.
	Exists(ctx context.Context, key string) (bool, error)
}

// Chunk represents a group of pages stored as a single object.
//
// Format:
//   Header (64 bytes):
//     Bytes 0-3:   Magic (0x53334442 = "S3DB")
//     Bytes 4-7:   Version
//     Bytes 8-11:  NumPages
//     Bytes 12-15: PageSize
//     Bytes 16-63: Reserved
//   Pages:
//     [PageSize * NumPages bytes]
//   Footer:
//     Bytes 0-3:   CRC32 of header + pages
type Chunk struct {
	ID       string
	Pages    []*page.Page
	NumPages int
}

// NewChunk creates a new empty chunk.
func NewChunk(id string) *Chunk {
	return &Chunk{
		ID:    id,
		Pages: make([]*page.Page, 0, PagesPerChunk),
	}
}

// AddPage adds a page to the chunk.
func (c *Chunk) AddPage(p *page.Page) {
	c.Pages = append(c.Pages, p)
	c.NumPages = len(c.Pages)
}

// Encode serializes the chunk to bytes for storage.
func (c *Chunk) Encode() []byte {
	dataSize := ChunkHeaderSize + page.PageSize*c.NumPages + ChunkFooterSize
	buf := make([]byte, dataSize)

	// Write header
	binary.LittleEndian.PutUint32(buf[0:4], ChunkMagic)
	binary.LittleEndian.PutUint32(buf[4:8], ChunkVersion)
	binary.LittleEndian.PutUint32(buf[8:12], uint32(c.NumPages))
	binary.LittleEndian.PutUint32(buf[12:16], page.PageSize)

	// Write pages
	for i, p := range c.Pages {
		offset := ChunkHeaderSize + i*page.PageSize
		copy(buf[offset:offset+page.PageSize], p.Data[:])
	}

	// Write CRC32 footer
	crc := crc32.ChecksumIEEE(buf[:dataSize-ChunkFooterSize])
	binary.LittleEndian.PutUint32(buf[dataSize-ChunkFooterSize:], crc)

	return buf
}

// DecodeChunk deserializes a chunk from bytes.
func DecodeChunk(id string, data []byte) (*Chunk, error) {
	if len(data) < ChunkHeaderSize+ChunkFooterSize {
		return nil, fmt.Errorf("%w: data too small", ErrChunkCorrupt)
	}

	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != ChunkMagic {
		return nil, fmt.Errorf("%w: got 0x%x", ErrChunkMagic, magic)
	}

	numPages := int(binary.LittleEndian.Uint32(data[8:12]))
	pageSize := int(binary.LittleEndian.Uint32(data[12:16]))

	expectedSize := ChunkHeaderSize + pageSize*numPages + ChunkFooterSize
	if len(data) != expectedSize {
		return nil, fmt.Errorf("%w: expected %d bytes, got %d", ErrChunkCorrupt, expectedSize, len(data))
	}

	// Verify CRC32
	storedCRC := binary.LittleEndian.Uint32(data[expectedSize-ChunkFooterSize:])
	computedCRC := crc32.ChecksumIEEE(data[:expectedSize-ChunkFooterSize])
	if storedCRC != computedCRC {
		return nil, fmt.Errorf("%w: CRC mismatch (stored=%d, computed=%d)", ErrChunkCorrupt, storedCRC, computedCRC)
	}

	chunk := &Chunk{
		ID:       id,
		NumPages: numPages,
		Pages:    make([]*page.Page, numPages),
	}

	for i := 0; i < numPages; i++ {
		offset := ChunkHeaderSize + i*pageSize
		p, err := page.FromBytes(data[offset : offset+pageSize])
		if err != nil {
			return nil, fmt.Errorf("decoding page %d in chunk %s: %w", i, id, err)
		}
		chunk.Pages[i] = p
	}

	return chunk, nil
}

// PageFromChunkBytes extracts a single page from raw chunk bytes without
// decoding the entire chunk. Used for efficient range reads.
func PageFromChunkBytes(data []byte, pageIndex int) (*page.Page, error) {
	if len(data) < ChunkHeaderSize {
		return nil, ErrChunkCorrupt
	}
	numPages := int(binary.LittleEndian.Uint32(data[8:12]))
	if pageIndex < 0 || pageIndex >= numPages {
		return nil, fmt.Errorf("page index %d out of range [0, %d)", pageIndex, numPages)
	}
	offset := ChunkHeaderSize + pageIndex*page.PageSize
	end := offset + page.PageSize
	if end > len(data) {
		return nil, ErrChunkCorrupt
	}
	return page.FromBytes(data[offset:end])
}
