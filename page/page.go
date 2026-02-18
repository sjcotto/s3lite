// Package page implements the core page abstraction for s3lite.
// Pages are fixed-size (4KB) blocks that form the atomic unit of I/O.
// All data structures (B-tree nodes, overflow data) are stored in pages.
package page

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	PageSize       = 4096
	HeaderSize     = 16
	LeafExtraSize  = 8
	InternalExtra  = 4
	CellPtrSize    = 2
	MaxPagesPerDB  = 1 << 32

	PageTypeLeaf     = 1
	PageTypeInternal = 2
	PageTypeOverflow = 3

	FlagDirty = 1 << 0
	FlagRoot  = 1 << 1
)

var (
	ErrPageFull     = errors.New("page: not enough space")
	ErrKeyTooLarge  = errors.New("page: key too large")
	ErrNotFound     = errors.New("page: key not found")
	ErrInvalidPage  = errors.New("page: invalid page data")
)

// Page represents a single 4KB database page.
//
// Layout:
//   Bytes 0-3:   PageID (uint32)
//   Byte  4:     PageType (leaf=1, internal=2, overflow=3)
//   Byte  5:     Flags
//   Bytes 6-7:   NumCells (uint16)
//   Bytes 8-9:   CellContentStart (uint16) - where cell content area begins
//   Bytes 10-11: FreeSpace (uint16)
//   Bytes 12-15: Reserved
//
// For internal nodes, bytes 16-19: RightmostChild (uint32)
// For leaf nodes, bytes 16-19: NextLeaf (uint32), bytes 20-23: PrevLeaf (uint32)
//
// Cell pointer array starts after the extra header.
// Cell content area grows backward from the end of the page.
type Page struct {
	Data  [PageSize]byte
	ID    uint32
	Dirty bool
	Pins  int32
}

// NewPage creates a new page of the given type.
func NewPage(id uint32, pageType byte) *Page {
	p := &Page{ID: id}
	binary.LittleEndian.PutUint32(p.Data[0:4], id)
	p.Data[4] = pageType
	p.Data[5] = 0 // flags

	headerEnd := p.headerEnd()
	// Cell content area starts at end of page
	binary.LittleEndian.PutUint16(p.Data[8:10], PageSize)
	// Free space = page size - header
	binary.LittleEndian.PutUint16(p.Data[10:12], uint16(PageSize-headerEnd))

	return p
}

// FromBytes creates a page from raw bytes.
func FromBytes(data []byte) (*Page, error) {
	if len(data) != PageSize {
		return nil, fmt.Errorf("%w: expected %d bytes, got %d", ErrInvalidPage, PageSize, len(data))
	}
	p := &Page{}
	copy(p.Data[:], data)
	p.ID = binary.LittleEndian.Uint32(p.Data[0:4])
	return p, nil
}

// Type returns the page type.
func (p *Page) Type() byte {
	return p.Data[4]
}

// Flags returns the page flags.
func (p *Page) Flags() byte {
	return p.Data[5]
}

// SetFlag sets a flag on the page.
func (p *Page) SetFlag(flag byte) {
	p.Data[5] |= flag
}

// NumCells returns the number of cells in the page.
func (p *Page) NumCells() uint16 {
	return binary.LittleEndian.Uint16(p.Data[6:8])
}

func (p *Page) setNumCells(n uint16) {
	binary.LittleEndian.PutUint16(p.Data[6:8], n)
}

// CellContentStart returns offset where cell content begins.
func (p *Page) CellContentStart() uint16 {
	return binary.LittleEndian.Uint16(p.Data[8:10])
}

func (p *Page) setCellContentStart(off uint16) {
	binary.LittleEndian.PutUint16(p.Data[8:10], off)
}

// FreeSpace returns available free space in bytes.
func (p *Page) FreeSpace() uint16 {
	return binary.LittleEndian.Uint16(p.Data[10:12])
}

func (p *Page) setFreeSpace(n uint16) {
	binary.LittleEndian.PutUint16(p.Data[10:12], n)
}

// headerEnd returns the offset after all headers (common + type-specific).
func (p *Page) headerEnd() int {
	switch p.Type() {
	case PageTypeInternal:
		return HeaderSize + InternalExtra
	case PageTypeLeaf:
		return HeaderSize + LeafExtraSize
	default:
		return HeaderSize
	}
}

// cellPtrOffset returns the byte offset of the i-th cell pointer.
func (p *Page) cellPtrOffset(i int) int {
	return p.headerEnd() + i*CellPtrSize
}

// CellOffset returns the offset of the i-th cell's content.
func (p *Page) CellOffset(i int) uint16 {
	off := p.cellPtrOffset(i)
	return binary.LittleEndian.Uint16(p.Data[off : off+2])
}

func (p *Page) setCellPtr(i int, offset uint16) {
	off := p.cellPtrOffset(i)
	binary.LittleEndian.PutUint16(p.Data[off:off+2], offset)
}

// --- Leaf node operations ---

// NextLeaf returns the next leaf page ID (0 = none).
func (p *Page) NextLeaf() uint32 {
	return binary.LittleEndian.Uint32(p.Data[16:20])
}

// SetNextLeaf sets the next leaf page ID.
func (p *Page) SetNextLeaf(id uint32) {
	binary.LittleEndian.PutUint32(p.Data[16:20], id)
}

// PrevLeaf returns the previous leaf page ID (0 = none).
func (p *Page) PrevLeaf() uint32 {
	return binary.LittleEndian.Uint32(p.Data[20:24])
}

// SetPrevLeaf sets the previous leaf page ID.
func (p *Page) SetPrevLeaf(id uint32) {
	binary.LittleEndian.PutUint32(p.Data[20:24], id)
}

// LeafCell represents a key-value cell in a leaf page.
type LeafCell struct {
	Key   []byte
	Value []byte
}

// leafCellSize returns the total serialized size of a leaf cell.
func leafCellSize(keyLen, valLen int) int {
	return 2 + 2 + keyLen + valLen // key_size(2) + val_size(2) + key + value
}

// ReadLeafCell reads the i-th leaf cell.
func (p *Page) ReadLeafCell(i int) LeafCell {
	off := int(p.CellOffset(i))
	keySize := int(binary.LittleEndian.Uint16(p.Data[off : off+2]))
	valSize := int(binary.LittleEndian.Uint16(p.Data[off+2 : off+4]))
	key := make([]byte, keySize)
	copy(key, p.Data[off+4:off+4+keySize])
	val := make([]byte, valSize)
	copy(val, p.Data[off+4+keySize:off+4+keySize+valSize])
	return LeafCell{Key: key, Value: val}
}

// WriteLeafCell writes a key-value cell into the leaf page.
// Cells are inserted in sorted order.
func (p *Page) WriteLeafCell(key, value []byte) error {
	cellSize := leafCellSize(len(key), len(value))
	needed := cellSize + CellPtrSize // cell content + cell pointer
	if int(p.FreeSpace()) < needed {
		return ErrPageFull
	}

	// Find insertion position (binary search)
	pos := p.leafSearchPos(key)
	numCells := int(p.NumCells())

	// Make room for new cell pointer by shifting existing pointers right
	ptrStart := p.cellPtrOffset(pos)
	ptrEnd := p.cellPtrOffset(numCells)
	if pos < numCells {
		copy(p.Data[ptrStart+CellPtrSize:ptrEnd+CellPtrSize], p.Data[ptrStart:ptrEnd])
	}

	// Write cell content at the end of content area
	contentStart := int(p.CellContentStart()) - cellSize
	off := contentStart
	binary.LittleEndian.PutUint16(p.Data[off:off+2], uint16(len(key)))
	binary.LittleEndian.PutUint16(p.Data[off+2:off+4], uint16(len(value)))
	copy(p.Data[off+4:], key)
	copy(p.Data[off+4+len(key):], value)

	// Set cell pointer
	binary.LittleEndian.PutUint16(p.Data[ptrStart:ptrStart+2], uint16(contentStart))

	// Update header
	p.setNumCells(uint16(numCells + 1))
	p.setCellContentStart(uint16(contentStart))
	p.setFreeSpace(p.FreeSpace() - uint16(needed))
	p.Dirty = true

	return nil
}

// UpdateLeafCell updates the value of an existing cell at position i.
// If the new value fits in the old space, it's updated in-place.
// Otherwise returns ErrPageFull and the caller should delete+reinsert.
func (p *Page) UpdateLeafCell(i int, newValue []byte) error {
	off := int(p.CellOffset(i))
	keySize := int(binary.LittleEndian.Uint16(p.Data[off : off+2]))
	oldValSize := int(binary.LittleEndian.Uint16(p.Data[off+2 : off+4]))

	if len(newValue) <= oldValSize {
		// Fits in existing space
		binary.LittleEndian.PutUint16(p.Data[off+2:off+4], uint16(len(newValue)))
		copy(p.Data[off+4+keySize:], newValue)
		p.Dirty = true
		return nil
	}

	return ErrPageFull
}

// DeleteLeafCell removes the i-th leaf cell.
func (p *Page) DeleteLeafCell(i int) {
	numCells := int(p.NumCells())
	if i >= numCells {
		return
	}

	// Shift cell pointers left
	ptrStart := p.cellPtrOffset(i)
	ptrEnd := p.cellPtrOffset(numCells)
	copy(p.Data[ptrStart:], p.Data[ptrStart+CellPtrSize:ptrEnd])

	// Clear last pointer slot
	lastPtr := p.cellPtrOffset(numCells - 1)
	p.Data[lastPtr] = 0
	p.Data[lastPtr+1] = 0

	p.setNumCells(uint16(numCells - 1))
	// Note: we don't reclaim cell content space here (fragmentation).
	// A page compaction step could be added later.
	p.Dirty = true
}

// leafSearchPos returns the position where key should be inserted.
func (p *Page) leafSearchPos(key []byte) int {
	n := int(p.NumCells())
	lo, hi := 0, n
	for lo < hi {
		mid := (lo + hi) / 2
		cell := p.ReadLeafCell(mid)
		if compareBytes(cell.Key, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// LeafSearch searches for key in a leaf page. Returns (index, found).
func (p *Page) LeafSearch(key []byte) (int, bool) {
	pos := p.leafSearchPos(key)
	if pos < int(p.NumCells()) {
		cell := p.ReadLeafCell(pos)
		if compareBytes(cell.Key, key) == 0 {
			return pos, true
		}
	}
	return pos, false
}

// --- Internal node operations ---

// RightmostChild returns the rightmost child page ID.
func (p *Page) RightmostChild() uint32 {
	return binary.LittleEndian.Uint32(p.Data[16:20])
}

// SetRightmostChild sets the rightmost child page ID.
func (p *Page) SetRightmostChild(id uint32) {
	binary.LittleEndian.PutUint32(p.Data[16:20], id)
}

// InternalCell represents a cell in an internal page.
type InternalCell struct {
	LeftChild uint32
	Key       []byte
}

func internalCellSize(keyLen int) int {
	return 4 + 2 + keyLen // left_child(4) + key_size(2) + key
}

// ReadInternalCell reads the i-th internal cell.
func (p *Page) ReadInternalCell(i int) InternalCell {
	off := int(p.CellOffset(i))
	leftChild := binary.LittleEndian.Uint32(p.Data[off : off+4])
	keySize := int(binary.LittleEndian.Uint16(p.Data[off+4 : off+6]))
	key := make([]byte, keySize)
	copy(key, p.Data[off+6:off+6+keySize])
	return InternalCell{LeftChild: leftChild, Key: key}
}

// WriteInternalCell inserts an internal cell in sorted order.
func (p *Page) WriteInternalCell(leftChild uint32, key []byte) error {
	cellSize := internalCellSize(len(key))
	needed := cellSize + CellPtrSize
	if int(p.FreeSpace()) < needed {
		return ErrPageFull
	}

	pos := p.internalSearchPos(key)
	numCells := int(p.NumCells())

	// Shift pointers right
	ptrStart := p.cellPtrOffset(pos)
	ptrEnd := p.cellPtrOffset(numCells)
	if pos < numCells {
		copy(p.Data[ptrStart+CellPtrSize:ptrEnd+CellPtrSize], p.Data[ptrStart:ptrEnd])
	}

	// Write cell content
	contentStart := int(p.CellContentStart()) - cellSize
	off := contentStart
	binary.LittleEndian.PutUint32(p.Data[off:off+4], leftChild)
	binary.LittleEndian.PutUint16(p.Data[off+4:off+6], uint16(len(key)))
	copy(p.Data[off+6:], key)

	// Set pointer
	binary.LittleEndian.PutUint16(p.Data[ptrStart:ptrStart+2], uint16(contentStart))

	p.setNumCells(uint16(numCells + 1))
	p.setCellContentStart(uint16(contentStart))
	p.setFreeSpace(p.FreeSpace() - uint16(needed))
	p.Dirty = true

	return nil
}

// internalSearchPos finds where key falls in internal node.
// Returns the index of the first key >= given key.
func (p *Page) internalSearchPos(key []byte) int {
	n := int(p.NumCells())
	lo, hi := 0, n
	for lo < hi {
		mid := (lo + hi) / 2
		cell := p.ReadInternalCell(mid)
		if compareBytes(cell.Key, key) < 0 {
			lo = mid + 1
		} else {
			hi = mid
		}
	}
	return lo
}

// InternalSearch finds the child page to follow for a given key.
// Returns the child page ID.
func (p *Page) InternalSearch(key []byte) uint32 {
	pos := p.internalSearchPos(key)
	if pos < int(p.NumCells()) {
		cell := p.ReadInternalCell(pos)
		if compareBytes(key, cell.Key) < 0 {
			return cell.LeftChild
		}
	}
	// Key is >= all keys, follow rightmost child
	if pos == int(p.NumCells()) {
		return p.RightmostChild()
	}
	// Key matches or is between pos and pos+1
	if pos+1 < int(p.NumCells()) {
		return p.ReadInternalCell(pos + 1).LeftChild
	}
	return p.RightmostChild()
}

// InternalSearchIdx finds child page for a key, returns (child_page_id, index).
// The index indicates which separator was used (-1 for rightmost).
func (p *Page) InternalSearchIdx(key []byte) (uint32, int) {
	n := int(p.NumCells())
	for i := 0; i < n; i++ {
		cell := p.ReadInternalCell(i)
		if compareBytes(key, cell.Key) < 0 {
			return cell.LeftChild, i
		}
	}
	return p.RightmostChild(), -1
}

// Bytes returns the raw page data.
func (p *Page) Bytes() []byte {
	return p.Data[:]
}

// compareBytes compares two byte slices lexicographically.
func compareBytes(a, b []byte) int {
	minLen := len(a)
	if len(b) < minLen {
		minLen = len(b)
	}
	for i := 0; i < minLen; i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	if len(a) < len(b) {
		return -1
	}
	if len(a) > len(b) {
		return 1
	}
	return 0
}

// CompareBytes is the exported version of compareBytes.
func CompareBytes(a, b []byte) int {
	return compareBytes(a, b)
}

// AllLeafCells returns all leaf cells in order.
func (p *Page) AllLeafCells() []LeafCell {
	n := int(p.NumCells())
	cells := make([]LeafCell, n)
	for i := 0; i < n; i++ {
		cells[i] = p.ReadLeafCell(i)
	}
	return cells
}

// AllInternalCells returns all internal cells in order.
func (p *Page) AllInternalCells() []InternalCell {
	n := int(p.NumCells())
	cells := make([]InternalCell, n)
	for i := 0; i < n; i++ {
		cells[i] = p.ReadInternalCell(i)
	}
	return cells
}
