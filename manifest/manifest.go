// Package manifest manages the database manifest — the single source of truth
// that maps logical page IDs to physical chunk locations, and stores metadata
// like table schemas, bloom filters, and zone maps.
//
// The manifest is stored as a JSON object in the storage backend.
// On every commit, a new manifest version is written atomically.
// Old manifests are kept for time-travel queries and crash recovery.
package manifest

import (
	"encoding/json"
	"fmt"

	"github.com/sjcotto/s3lite/bloom"
)

// Manifest is the top-level database metadata.
type Manifest struct {
	Version   uint64                `json:"version"`
	PageSize  int                   `json:"page_size"`
	NumPages  uint32                `json:"num_pages"`
	NextPage  uint32                `json:"next_page"` // next free page ID
	Tables    map[string]*TableMeta `json:"tables"`
	Indexes   map[string]*IndexMeta `json:"indexes,omitempty"`
	Chunks    map[string]*ChunkMeta `json:"chunks"`
	WALSegs   []string              `json:"wal_segments,omitempty"`

	// PageIndex maps page ID -> chunk location.
	PageIndex map[uint32]PageLocation `json:"page_index"`
}

// TableMeta describes a table's schema and root page.
type TableMeta struct {
	RootPage uint32       `json:"root_page"`
	Columns  []ColumnMeta `json:"columns"`
	RowCount int64        `json:"row_count"`
}

// ColumnMeta describes a column.
type ColumnMeta struct {
	Name     string `json:"name"`
	Type     string `json:"type"` // "int", "text", "float", "blob"
	Nullable bool   `json:"nullable"`
	PK       bool   `json:"pk"` // primary key
}

// IndexMeta describes a secondary index.
type IndexMeta struct {
	Name     string   `json:"name"`
	Table    string   `json:"table"`
	Columns  []string `json:"columns"`
	RootPage uint32   `json:"root_page"`
	Unique   bool     `json:"unique"`
}

// ChunkMeta describes a chunk stored in the backend.
type ChunkMeta struct {
	Key         string   `json:"key"` // storage key
	Pages       []uint32 `json:"pages"`
	BloomFilter string   `json:"bloom_filter,omitempty"` // base64-encoded
	MinKey      string   `json:"min_key,omitempty"`
	MaxKey      string   `json:"max_key,omitempty"`
	SizeBytes   int      `json:"size_bytes"`
}

// PageLocation identifies where a page lives within a chunk.
type PageLocation struct {
	ChunkKey  string `json:"chunk"`
	PageIndex int    `json:"index"` // index within the chunk
}

// New creates a new empty manifest.
func New() *Manifest {
	return &Manifest{
		Version:   1,
		PageSize:  4096,
		NumPages:  0,
		NextPage:  0,
		Tables:    make(map[string]*TableMeta),
		Indexes:   make(map[string]*IndexMeta),
		Chunks:    make(map[string]*ChunkMeta),
		PageIndex: make(map[uint32]PageLocation),
	}
}

// AllocPage allocates a new page ID.
func (m *Manifest) AllocPage() uint32 {
	id := m.NextPage
	m.NextPage++
	m.NumPages++
	return id
}

// SetPageLocation records where a page is stored.
func (m *Manifest) SetPageLocation(pageID uint32, chunkKey string, index int) {
	m.PageIndex[pageID] = PageLocation{
		ChunkKey:  chunkKey,
		PageIndex: index,
	}
}

// GetPageLocation finds which chunk contains a page.
func (m *Manifest) GetPageLocation(pageID uint32) (PageLocation, bool) {
	loc, ok := m.PageIndex[pageID]
	return loc, ok
}

// AddChunk registers a new chunk in the manifest.
func (m *Manifest) AddChunk(meta *ChunkMeta) {
	m.Chunks[meta.Key] = meta
	for i, pageID := range meta.Pages {
		m.PageIndex[pageID] = PageLocation{
			ChunkKey:  meta.Key,
			PageIndex: i,
		}
	}
}

// ChunkBloomFilter returns the decoded bloom filter for a chunk.
func (m *Manifest) ChunkBloomFilter(chunkKey string) (*bloom.Filter, error) {
	meta, ok := m.Chunks[chunkKey]
	if !ok {
		return nil, fmt.Errorf("chunk %s not found", chunkKey)
	}
	if meta.BloomFilter == "" {
		return nil, nil
	}
	return bloom.Decode(meta.BloomFilter)
}

// Encode serializes the manifest to JSON.
func (m *Manifest) Encode() ([]byte, error) {
	return json.MarshalIndent(m, "", "  ")
}

// Decode deserializes a manifest from JSON.
func Decode(data []byte) (*Manifest, error) {
	m := &Manifest{}
	if err := json.Unmarshal(data, m); err != nil {
		return nil, fmt.Errorf("decoding manifest: %w", err)
	}
	if m.Tables == nil {
		m.Tables = make(map[string]*TableMeta)
	}
	if m.Indexes == nil {
		m.Indexes = make(map[string]*IndexMeta)
	}
	if m.Chunks == nil {
		m.Chunks = make(map[string]*ChunkMeta)
	}
	if m.PageIndex == nil {
		m.PageIndex = make(map[uint32]PageLocation)
	}
	return m, nil
}

// ManifestKey returns the storage key for a manifest version.
func ManifestKey(version uint64) string {
	return fmt.Sprintf("manifests/v%010d.json", version)
}

// LatestManifestKey is a pointer to the current manifest version.
const LatestManifestKey = "manifests/latest"

// Clone creates a deep copy of the manifest for copy-on-write semantics.
func (m *Manifest) Clone() *Manifest {
	data, _ := m.Encode()
	clone, _ := Decode(data)
	clone.Version = m.Version + 1
	return clone
}
