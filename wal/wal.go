// Package wal implements a write-ahead log for crash recovery.
// WAL entries buffer page-level changes locally before they're flushed
// to chunks in the storage backend. On crash, the WAL is replayed to
// reconstruct the latest state.
//
// WAL segments are periodically flushed to storage and referenced
// in the manifest. After a successful checkpoint (all dirty pages
// written to chunks), WAL segments can be garbage collected.
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"

	"github.com/sjcotto/s3lite/page"
)

const (
	WALMagic   = 0x57414C53 // "WALS"
	RecInsert  = 1
	RecDelete  = 2
	RecUpdate  = 3
	RecCommit  = 4
	RecPageImg = 5 // full page image

	recordHeaderSize = 4 + 8 + 1 + 4 // length + LSN + type + pageID
)

// WAL is a write-ahead log.
type WAL struct {
	mu       sync.Mutex
	lsn      uint64
	records  []Record
	segments [][]byte // flushed segments
	segNum   uint32
}

// Record represents a single WAL record.
type Record struct {
	LSN      uint64
	Type     byte
	PageID   uint32
	PageData [page.PageSize]byte
}

// New creates a new WAL.
func New() *WAL {
	return &WAL{
		lsn: 1,
	}
}

// LogPageWrite logs a full page image to the WAL.
func (w *WAL) LogPageWrite(p *page.Page) uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := Record{
		LSN:    w.lsn,
		Type:   RecPageImg,
		PageID: p.ID,
	}
	copy(rec.PageData[:], p.Data[:])
	w.records = append(w.records, rec)
	w.lsn++
	return rec.LSN
}

// LogCommit logs a commit record.
func (w *WAL) LogCommit() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()

	rec := Record{
		LSN:  w.lsn,
		Type: RecCommit,
	}
	w.records = append(w.records, rec)
	w.lsn++
	return rec.LSN
}

// CurrentLSN returns the current log sequence number.
func (w *WAL) CurrentLSN() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.lsn
}

// Records returns all unflushed WAL records.
func (w *WAL) Records() []Record {
	w.mu.Lock()
	defer w.mu.Unlock()
	result := make([]Record, len(w.records))
	copy(result, w.records)
	return result
}

// DirtyPages returns page IDs that have been modified since the last checkpoint.
func (w *WAL) DirtyPages() map[uint32]bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	dirty := make(map[uint32]bool)
	for _, rec := range w.records {
		if rec.Type == RecPageImg {
			dirty[rec.PageID] = true
		}
	}
	return dirty
}

// LatestPageImage returns the most recent full page image for a page ID.
func (w *WAL) LatestPageImage(pageID uint32) (*page.Page, bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Walk backwards to find most recent
	for i := len(w.records) - 1; i >= 0; i-- {
		rec := w.records[i]
		if rec.Type == RecPageImg && rec.PageID == pageID {
			p := &page.Page{ID: pageID}
			copy(p.Data[:], rec.PageData[:])
			return p, true
		}
	}
	return nil, false
}

// Flush encodes the current WAL records into a segment.
func (w *WAL) Flush() ([]byte, string, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.records) == 0 {
		return nil, "", nil
	}

	w.segNum++
	segName := fmt.Sprintf("wal/seg-%06d", w.segNum)

	data := w.encodeSegment()
	w.segments = append(w.segments, data)
	w.records = w.records[:0] // clear after flush

	return data, segName, nil
}

// Checkpoint clears the WAL after pages have been written to chunks.
func (w *WAL) Checkpoint() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.records = w.records[:0]
	w.segments = w.segments[:0]
}

func (w *WAL) encodeSegment() []byte {
	// Calculate size
	size := 24 // header: magic(4) + segNum(4) + startLSN(8) + endLSN(8)
	for _, rec := range w.records {
		size += recordHeaderSize + 4 // +4 for CRC
		if rec.Type == RecPageImg {
			size += page.PageSize
		}
	}

	buf := make([]byte, size)
	off := 0

	// Header
	binary.LittleEndian.PutUint32(buf[off:], WALMagic)
	off += 4
	binary.LittleEndian.PutUint32(buf[off:], w.segNum)
	off += 4

	startLSN := w.records[0].LSN
	endLSN := w.records[len(w.records)-1].LSN
	binary.LittleEndian.PutUint64(buf[off:], startLSN)
	off += 8
	binary.LittleEndian.PutUint64(buf[off:], endLSN)
	off += 8

	// Records
	for _, rec := range w.records {
		recStart := off
		recLen := recordHeaderSize
		if rec.Type == RecPageImg {
			recLen += page.PageSize
		}

		binary.LittleEndian.PutUint32(buf[off:], uint32(recLen))
		off += 4
		binary.LittleEndian.PutUint64(buf[off:], rec.LSN)
		off += 8
		buf[off] = rec.Type
		off++
		binary.LittleEndian.PutUint32(buf[off:], rec.PageID)
		off += 4

		if rec.Type == RecPageImg {
			copy(buf[off:], rec.PageData[:])
			off += page.PageSize
		}

		// CRC of record
		crc := crc32.ChecksumIEEE(buf[recStart:off])
		binary.LittleEndian.PutUint32(buf[off:], crc)
		off += 4
	}

	return buf[:off]
}

// DecodeSegment decodes a WAL segment from bytes.
func DecodeSegment(data []byte) ([]Record, error) {
	if len(data) < 24 {
		return nil, fmt.Errorf("wal segment too small")
	}

	magic := binary.LittleEndian.Uint32(data[0:4])
	if magic != WALMagic {
		return nil, fmt.Errorf("invalid WAL magic: 0x%x", magic)
	}

	off := 24 // skip header
	var records []Record

	for off < len(data) {
		if off+recordHeaderSize+4 > len(data) {
			break
		}

		recLen := int(binary.LittleEndian.Uint32(data[off:]))
		recStart := off
		off += 4

		lsn := binary.LittleEndian.Uint64(data[off:])
		off += 8
		recType := data[off]
		off++
		pageID := binary.LittleEndian.Uint32(data[off:])
		off += 4

		rec := Record{
			LSN:    lsn,
			Type:   recType,
			PageID: pageID,
		}

		if recType == RecPageImg {
			if off+page.PageSize > len(data) {
				return nil, fmt.Errorf("truncated page image at LSN %d", lsn)
			}
			copy(rec.PageData[:], data[off:off+page.PageSize])
			off += page.PageSize
		}

		// Verify CRC
		expectedEnd := recStart + 4 + recLen
		storedCRC := binary.LittleEndian.Uint32(data[expectedEnd:])
		computedCRC := crc32.ChecksumIEEE(data[recStart:expectedEnd])
		if storedCRC != computedCRC {
			return nil, fmt.Errorf("CRC mismatch at LSN %d", lsn)
		}
		off = expectedEnd + 4

		records = append(records, rec)
	}

	return records, nil
}
