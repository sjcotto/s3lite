// Package engine is the main database engine that ties together
// all components: storage backend, page cache, B-tree, manifest, WAL,
// and bloom filters. It provides a high-level API for table operations.
package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sjcotto/s3lite/bloom"
	"github.com/sjcotto/s3lite/btree"
	"github.com/sjcotto/s3lite/manifest"
	"github.com/sjcotto/s3lite/page"
	"github.com/sjcotto/s3lite/storage"
	"github.com/sjcotto/s3lite/wal"
)

// Config holds engine configuration.
type Config struct {
	// CacheSize is the number of pages to keep in the buffer pool.
	CacheSize int
	// DataDir is the base directory for local storage backend.
	DataDir string
	// S3 config (if using S3 backend).
	S3 *storage.S3Config
	// ParallelFetch controls the number of concurrent chunk fetches.
	ParallelFetch int
}

// DefaultConfig returns a sensible default configuration.
func DefaultConfig(dataDir string) Config {
	return Config{
		CacheSize:     4096, // 4096 pages = 16MB cache
		DataDir:       dataDir,
		ParallelFetch: 4,
	}
}

// Engine is the main database engine.
type Engine struct {
	mu       sync.RWMutex
	backend  storage.Backend
	cache    *page.Cache
	manifest *manifest.Manifest
	wal      *wal.WAL
	trees    map[string]*btree.BTree // table name -> B-tree
	indexes  map[string]*btree.BTree // index name -> B-tree

	// ChunkFetch controls whether to fetch entire chunks on a page miss.
	ChunkFetch bool

	// ParallelFetch is the number of concurrent chunk fetches.
	ParallelFetch int

	// Transaction support
	txSavepoint *manifest.Manifest // snapshot for rollback
	txWALLen    int                // WAL length at BEGIN

	// Stats
	pagesRead       atomic.Int64
	pagesWritten    atomic.Int64
	cacheMisses     atomic.Int64
	cacheHits       atomic.Int64
	s3Gets          atomic.Int64
	s3Puts          atomic.Int64
	bloomSkips      atomic.Int64
	prefetchedPages atomic.Int64
}

// Open opens or creates a database.
func Open(cfg Config) (*Engine, error) {
	// Initialize storage backend
	var backend storage.Backend
	if cfg.S3 != nil {
		backend = storage.NewS3Backend(*cfg.S3)
	} else {
		var err error
		backend, err = storage.NewLocalBackend(cfg.DataDir)
		if err != nil {
			return nil, fmt.Errorf("creating local backend: %w", err)
		}
	}

	parallelFetch := cfg.ParallelFetch
	if parallelFetch <= 0 {
		parallelFetch = 4
	}

	e := &Engine{
		backend:       backend,
		cache:         page.NewCache(cfg.CacheSize),
		wal:           wal.New(),
		trees:         make(map[string]*btree.BTree),
		indexes:       make(map[string]*btree.BTree),
		ChunkFetch:    true,
		ParallelFetch: parallelFetch,
	}

	// Set up cache eviction callback
	e.cache.OnEvict = func(p *page.Page) error {
		return e.writePage(p)
	}

	// Try to load existing manifest
	ctx := context.Background()
	if err := e.loadManifest(ctx); err != nil {
		// No existing database — create fresh manifest
		e.manifest = manifest.New()
	}

	// Load B-trees for existing tables
	for name, table := range e.manifest.Tables {
		e.trees[name] = btree.New(table.RootPage, e)
	}

	// Load B-trees for existing indexes
	for name, idx := range e.manifest.Indexes {
		e.indexes[name] = btree.New(idx.RootPage, e)
	}

	return e, nil
}

// loadManifest loads the latest manifest from storage.
func (e *Engine) loadManifest(ctx context.Context) error {
	data, err := e.backend.Get(ctx, manifest.LatestManifestKey)
	if err != nil {
		return err
	}

	var version uint64
	if err := json.Unmarshal(data, &version); err != nil {
		return fmt.Errorf("parsing latest manifest pointer: %w", err)
	}

	mdata, err := e.backend.Get(ctx, manifest.ManifestKey(version))
	if err != nil {
		return err
	}

	m, err := manifest.Decode(mdata)
	if err != nil {
		return err
	}

	e.manifest = m
	return nil
}

// saveManifest writes the manifest to storage.
func (e *Engine) saveManifest(ctx context.Context) error {
	data, err := e.manifest.Encode()
	if err != nil {
		return err
	}

	key := manifest.ManifestKey(e.manifest.Version)
	if err := e.backend.Put(ctx, key, data); err != nil {
		return fmt.Errorf("writing manifest v%d: %w", e.manifest.Version, err)
	}

	versionData, _ := json.Marshal(e.manifest.Version)
	if err := e.backend.Put(ctx, manifest.LatestManifestKey, versionData); err != nil {
		return fmt.Errorf("updating latest pointer: %w", err)
	}

	return nil
}

// --- PageFetcher interface for btree ---

// FetchPage reads a page, checking cache, then WAL, then storage.
func (e *Engine) FetchPage(ctx context.Context, pageID uint32) (*page.Page, error) {
	// 1. Check cache
	if p := e.cache.Get(pageID); p != nil {
		e.cacheHits.Add(1)
		return p, nil
	}
	e.cacheMisses.Add(1)

	// 2. Check WAL for most recent version
	if p, ok := e.wal.LatestPageImage(pageID); ok {
		if err := e.cache.Put(p); err != nil {
			return nil, err
		}
		return p, nil
	}

	// 3. Fetch from storage
	p, err := e.fetchPageFromStorage(ctx, pageID)
	if err != nil {
		return nil, err
	}

	// Put in cache
	if err := e.cache.Put(p); err != nil {
		return nil, err
	}

	e.pagesRead.Add(1)
	return p, nil
}

// fetchPageFromStorage reads a page from a chunk in the storage backend.
func (e *Engine) fetchPageFromStorage(ctx context.Context, pageID uint32) (*page.Page, error) {
	loc, ok := e.manifest.GetPageLocation(pageID)
	if !ok {
		return nil, fmt.Errorf("page %d not found in manifest", pageID)
	}

	e.s3Gets.Add(1)

	if !e.ChunkFetch {
		offset := int64(storage.ChunkHeaderSize + loc.PageIndex*page.PageSize)
		data, err := e.backend.GetRange(ctx, loc.ChunkKey, offset, page.PageSize)
		if err != nil {
			return nil, fmt.Errorf("fetching page %d from chunk %s: %w", pageID, loc.ChunkKey, err)
		}
		return page.FromBytes(data)
	}

	// Chunk-aware fetch: get the entire chunk, cache all pages
	chunkData, err := e.backend.Get(ctx, loc.ChunkKey)
	if err != nil {
		return nil, fmt.Errorf("fetching chunk %s: %w", loc.ChunkKey, err)
	}

	chunk, err := storage.DecodeChunk(loc.ChunkKey, chunkData)
	if err != nil {
		return nil, fmt.Errorf("decoding chunk %s: %w", loc.ChunkKey, err)
	}

	// Cache all pages from this chunk
	var target *page.Page
	for _, p := range chunk.Pages {
		if p.ID == pageID {
			target = p
		} else {
			if existing := e.cache.Get(p.ID); existing == nil {
				_ = e.cache.Put(p)
				e.prefetchedPages.Add(1)
			}
		}
	}

	if target == nil {
		return nil, fmt.Errorf("page %d not found in chunk %s at index %d", pageID, loc.ChunkKey, loc.PageIndex)
	}
	return target, nil
}

// AllocPage creates a new page with the given type.
func (e *Engine) AllocPage(pageType byte) (*page.Page, error) {
	id := e.manifest.AllocPage()
	p := page.NewPage(id, pageType)
	if err := e.cache.Put(p); err != nil {
		return nil, err
	}
	return p, nil
}

// MarkDirty marks a page as needing to be flushed.
func (e *Engine) MarkDirty(p *page.Page) error {
	p.Dirty = true
	e.wal.LogPageWrite(p)
	return e.cache.Put(p)
}

func (e *Engine) writePage(p *page.Page) error {
	e.wal.LogPageWrite(p)
	return nil
}

// --- Table operations ---

// CreateTable creates a new table with the given schema.
func (e *Engine) CreateTable(ctx context.Context, name string, columns []manifest.ColumnMeta) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	name = strings.ToLower(name)
	if _, exists := e.manifest.Tables[name]; exists {
		return fmt.Errorf("table %s already exists", name)
	}

	tree, err := btree.NewEmpty(e)
	if err != nil {
		return err
	}

	e.manifest.Tables[name] = &manifest.TableMeta{
		RootPage: tree.RootPage,
		Columns:  columns,
		RowCount: 0,
	}
	e.trees[name] = tree

	return nil
}

// DropTable removes a table.
func (e *Engine) DropTable(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	name = strings.ToLower(name)
	if _, exists := e.manifest.Tables[name]; !exists {
		return fmt.Errorf("table %s does not exist", name)
	}

	delete(e.manifest.Tables, name)
	delete(e.trees, name)

	// Drop all indexes on this table
	for idxName, idx := range e.manifest.Indexes {
		if strings.EqualFold(idx.Table, name) {
			delete(e.manifest.Indexes, idxName)
			delete(e.indexes, idxName)
		}
	}

	return nil
}

// TableExists checks if a table exists.
func (e *Engine) TableExists(name string) bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	_, exists := e.manifest.Tables[strings.ToLower(name)]
	return exists
}

// TableMeta returns metadata for a table.
func (e *Engine) TableMeta(name string) (*manifest.TableMeta, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	meta, ok := e.manifest.Tables[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("table %s does not exist", name)
	}
	return meta, nil
}

// Insert inserts a row into a table.
func (e *Engine) Insert(ctx context.Context, table string, key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	table = strings.ToLower(table)
	tree, ok := e.trees[table]
	if !ok {
		return fmt.Errorf("table %s does not exist", table)
	}

	if err := tree.Put(ctx, key, value); err != nil {
		return err
	}

	e.manifest.Tables[table].RootPage = tree.RootPage
	e.manifest.Tables[table].RowCount++

	return nil
}

// Get retrieves a row by primary key.
func (e *Engine) Get(ctx context.Context, table string, key []byte) ([]byte, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	table = strings.ToLower(table)

	if e.canSkipWithBloom(table, key) {
		e.bloomSkips.Add(1)
		return nil, page.ErrNotFound
	}

	tree, ok := e.trees[table]
	if !ok {
		return nil, fmt.Errorf("table %s does not exist", table)
	}

	return tree.Get(ctx, key)
}

func (e *Engine) canSkipWithBloom(table string, key []byte) bool {
	meta, ok := e.manifest.Tables[table]
	if !ok {
		return false
	}
	_ = meta

	for _, chunkMeta := range e.manifest.Chunks {
		if chunkMeta.BloomFilter == "" {
			continue
		}
		bf, err := bloom.Decode(chunkMeta.BloomFilter)
		if err != nil {
			continue
		}
		if bf.MayContain(key) {
			return false
		}
	}

	return len(e.manifest.Chunks) > 0
}

// Delete removes a row by primary key.
func (e *Engine) Delete(ctx context.Context, table string, key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	table = strings.ToLower(table)
	tree, ok := e.trees[table]
	if !ok {
		return fmt.Errorf("table %s does not exist", table)
	}

	if err := tree.Delete(ctx, key); err != nil {
		return err
	}
	e.manifest.Tables[table].RowCount--
	return nil
}

// Scan returns all rows in a table.
func (e *Engine) Scan(ctx context.Context, table string) ([]page.LeafCell, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	table = strings.ToLower(table)
	tree, ok := e.trees[table]
	if !ok {
		return nil, fmt.Errorf("table %s does not exist", table)
	}

	return tree.Scan(ctx)
}

// ScanRange returns rows in a key range.
func (e *Engine) ScanRange(ctx context.Context, table string, startKey, endKey []byte) ([]page.LeafCell, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	table = strings.ToLower(table)
	tree, ok := e.trees[table]
	if !ok {
		return nil, fmt.Errorf("table %s does not exist", table)
	}

	return tree.ScanRange(ctx, startKey, endKey)
}

// --- Index operations ---

// CreateIndex creates a secondary index on a table.
func (e *Engine) CreateIndex(ctx context.Context, name, table string, columns []string, unique bool) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	name = strings.ToLower(name)
	table = strings.ToLower(table)

	if _, exists := e.manifest.Indexes[name]; exists {
		return fmt.Errorf("index %s already exists", name)
	}

	if _, exists := e.manifest.Tables[table]; !exists {
		return fmt.Errorf("table %s does not exist", table)
	}

	// Create B-tree for the index
	tree, err := btree.NewEmpty(e)
	if err != nil {
		return err
	}

	// Populate the index from existing data
	dataTree := e.trees[table]
	cells, err := dataTree.Scan(ctx)
	if err != nil {
		return err
	}

	for _, cell := range cells {
		var row map[string]interface{}
		if err := json.Unmarshal(cell.Value, &row); err != nil {
			continue
		}
		idxKey := buildIndexKeyWithPK(row, columns, cell.Key)
		// Index value = primary key (for index lookup -> table lookup)
		if err := tree.Put(ctx, idxKey, cell.Key); err != nil {
			return err
		}
	}

	e.manifest.Indexes[name] = &manifest.IndexMeta{
		Name:     name,
		Table:    table,
		Columns:  columns,
		RootPage: tree.RootPage,
		Unique:   unique,
	}
	e.indexes[name] = tree

	return nil
}

// DropIndex removes an index.
func (e *Engine) DropIndex(name string) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	name = strings.ToLower(name)
	if _, exists := e.manifest.Indexes[name]; !exists {
		return fmt.Errorf("index %s does not exist", name)
	}

	delete(e.manifest.Indexes, name)
	delete(e.indexes, name)
	return nil
}

// FindIndex finds an index on a table for the given column.
func (e *Engine) FindIndex(table, column string) string {
	e.mu.RLock()
	defer e.mu.RUnlock()

	table = strings.ToLower(table)
	column = strings.ToLower(column)

	for name, idx := range e.manifest.Indexes {
		if strings.EqualFold(idx.Table, table) && len(idx.Columns) == 1 && strings.EqualFold(idx.Columns[0], column) {
			return name
		}
	}
	return ""
}

// ScanIndex scans an index for keys matching the given value and returns the corresponding table rows.
// startKey and endKey are the indexed column values (without PK suffix).
func (e *Engine) ScanIndex(ctx context.Context, indexName string, startKey, endKey []byte) ([]page.LeafCell, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	indexName = strings.ToLower(indexName)
	idxTree, ok := e.indexes[indexName]
	if !ok {
		return nil, fmt.Errorf("index %s does not exist", indexName)
	}

	idxMeta, ok := e.manifest.Indexes[indexName]
	if !ok {
		return nil, fmt.Errorf("index %s not in manifest", indexName)
	}

	dataTree, ok := e.trees[strings.ToLower(idxMeta.Table)]
	if !ok {
		return nil, fmt.Errorf("table %s does not exist", idxMeta.Table)
	}

	// Range scan: startKey\x00\x01 to startKey\x00\x02
	// This captures all composite keys with this prefix
	rangeStart := make([]byte, len(startKey)+2)
	copy(rangeStart, startKey)
	rangeStart[len(startKey)] = 0x00
	rangeStart[len(startKey)+1] = 0x01

	rangeEnd := make([]byte, len(endKey)+2)
	copy(rangeEnd, endKey)
	rangeEnd[len(endKey)] = 0x00
	rangeEnd[len(endKey)+1] = 0x02 // exclusive end

	idxCells, err := idxTree.ScanRange(ctx, rangeStart, rangeEnd)
	if err != nil {
		return nil, err
	}

	// Look up actual rows by PK
	var result []page.LeafCell
	for _, idxCell := range idxCells {
		pk := idxCell.Value
		val, err := dataTree.Get(ctx, pk)
		if err != nil {
			continue
		}
		result = append(result, page.LeafCell{Key: pk, Value: val})
	}

	return result, nil
}

// UpdateIndexes updates all indexes for a table after an insert.
func (e *Engine) UpdateIndexes(ctx context.Context, table string, pk []byte, row map[string]interface{}) {
	e.mu.RLock()
	defer e.mu.RUnlock()

	table = strings.ToLower(table)
	for _, idx := range e.manifest.Indexes {
		if !strings.EqualFold(idx.Table, table) {
			continue
		}
		tree, ok := e.indexes[idx.Name]
		if !ok {
			continue
		}
		idxKey := buildIndexKeyWithPK(row, idx.Columns, pk)
		_ = tree.Put(ctx, idxKey, pk)
		// Update root page
		idx.RootPage = tree.RootPage
	}
}

func buildIndexKey(row map[string]interface{}, columns []string) []byte {
	var parts []string
	for _, col := range columns {
		for k, v := range row {
			if strings.EqualFold(k, col) {
				parts = append(parts, fmt.Sprintf("%v", v))
				break
			}
		}
	}
	return []byte(strings.Join(parts, "\x00"))
}

// buildIndexKeyWithPK builds an index key that includes the PK for uniqueness.
// Format: indexedValues + \x00\x01 + primaryKey
// This ensures non-unique index values still produce unique B-tree keys.
func buildIndexKeyWithPK(row map[string]interface{}, columns []string, pk []byte) []byte {
	base := buildIndexKey(row, columns)
	result := make([]byte, len(base)+2+len(pk))
	copy(result, base)
	result[len(base)] = 0x00
	result[len(base)+1] = 0x01 // separator that sorts after \x00
	copy(result[len(base)+2:], pk)
	return result
}

// IndexNames returns all index names.
func (e *Engine) IndexNames() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var names []string
	for name := range e.manifest.Indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// IndexMeta returns metadata for an index.
func (e *Engine) IndexMeta(name string) (*manifest.IndexMeta, error) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	meta, ok := e.manifest.Indexes[strings.ToLower(name)]
	if !ok {
		return nil, fmt.Errorf("index %s does not exist", name)
	}
	return meta, nil
}

// --- Transaction operations ---

// BeginTx starts an explicit transaction by saving a snapshot.
func (e *Engine) BeginTx() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.txSavepoint = e.manifest.Clone()
	e.txSavepoint.Version = e.manifest.Version // keep same version for savepoint
	e.txWALLen = len(e.wal.Records())
}

// CommitTx commits the current transaction (flush + clear savepoint).
func (e *Engine) CommitTx(ctx context.Context) error {
	e.txSavepoint = nil
	return e.Commit(ctx)
}

// RollbackTx rolls back to the savepoint.
func (e *Engine) RollbackTx() {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.txSavepoint == nil {
		return
	}

	// Restore manifest
	e.manifest = e.txSavepoint
	e.txSavepoint = nil

	// Evict dirty pages from cache (they contain uncommitted changes)
	e.cache.EvictDirty()

	// Rebuild tree references from the restored manifest
	e.trees = make(map[string]*btree.BTree)
	for name, table := range e.manifest.Tables {
		e.trees[name] = btree.New(table.RootPage, e)
	}

	e.indexes = make(map[string]*btree.BTree)
	for name, idx := range e.manifest.Indexes {
		e.indexes[name] = btree.New(idx.RootPage, e)
	}

	// Clear WAL entries added since BEGIN
	e.wal.Checkpoint()
}

// --- Commit & Lifecycle ---

// Commit flushes all dirty pages to storage as new chunks and updates the manifest.
func (e *Engine) Commit(ctx context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	dirtyPages := e.cache.DirtyPages()
	if len(dirtyPages) == 0 {
		return nil
	}

	var pages []*page.Page
	for _, id := range dirtyPages {
		p := e.cache.Get(id)
		if p != nil && p.Dirty {
			pages = append(pages, p)
		}
	}

	walDirty := e.wal.DirtyPages()
	for pid := range walDirty {
		found := false
		for _, p := range pages {
			if p.ID == pid {
				found = true
				break
			}
		}
		if !found {
			if p, ok := e.wal.LatestPageImage(pid); ok {
				pages = append(pages, p)
			}
		}
	}

	if len(pages) == 0 {
		return nil
	}

	chunks := e.groupIntoChunks(pages)

	newManifest := e.manifest.Clone()
	for _, chunk := range chunks {
		data := chunk.Encode()
		if err := e.backend.Put(ctx, chunk.ID, data); err != nil {
			return fmt.Errorf("writing chunk %s: %w", chunk.ID, err)
		}
		e.s3Puts.Add(1)
		e.pagesWritten.Add(int64(chunk.NumPages))

		bf := bloom.New(chunk.NumPages * 50)
		var pageIDs []uint32
		var minKey, maxKey []byte

		for _, p := range chunk.Pages {
			pageIDs = append(pageIDs, p.ID)
			if p.Type() == page.PageTypeLeaf {
				for _, cell := range p.AllLeafCells() {
					bf.Add(cell.Key)
					if minKey == nil || page.CompareBytes(cell.Key, minKey) < 0 {
						minKey = make([]byte, len(cell.Key))
						copy(minKey, cell.Key)
					}
					if maxKey == nil || page.CompareBytes(cell.Key, maxKey) > 0 {
						maxKey = make([]byte, len(cell.Key))
						copy(maxKey, cell.Key)
					}
				}
			}
		}

		chunkMeta := &manifest.ChunkMeta{
			Key:         chunk.ID,
			Pages:       pageIDs,
			BloomFilter: bf.Encode(),
			MinKey:      string(minKey),
			MaxKey:      string(maxKey),
			SizeBytes:   len(data),
		}
		newManifest.AddChunk(chunkMeta)
	}

	// Update table root pages
	for name, tree := range e.trees {
		if meta, ok := newManifest.Tables[name]; ok {
			meta.RootPage = tree.RootPage
		}
	}

	// Update index root pages
	for name, tree := range e.indexes {
		if meta, ok := newManifest.Indexes[name]; ok {
			meta.RootPage = tree.RootPage
		}
	}

	e.manifest = newManifest
	if err := e.saveManifest(ctx); err != nil {
		return err
	}

	for _, p := range pages {
		p.Dirty = false
	}

	e.wal.Checkpoint()

	return nil
}

func (e *Engine) groupIntoChunks(pages []*page.Page) []*storage.Chunk {
	var chunks []*storage.Chunk
	chunkIdx := 0

	for i := 0; i < len(pages); i += storage.PagesPerChunk {
		end := i + storage.PagesPerChunk
		if end > len(pages) {
			end = len(pages)
		}

		chunkID := fmt.Sprintf("chunks/c-%010d-%06d", e.manifest.Version, chunkIdx)
		chunk := storage.NewChunk(chunkID)
		for _, p := range pages[i:end] {
			chunk.AddPage(p)
		}
		chunks = append(chunks, chunk)
		chunkIdx++
	}

	return chunks
}

// Close flushes and closes the engine.
func (e *Engine) Close(ctx context.Context) error {
	return e.Commit(ctx)
}

// --- Parallel chunk prefetch ---

// PrefetchChunks fetches multiple chunks in parallel and caches all their pages.
func (e *Engine) PrefetchChunks(ctx context.Context, chunkKeys []string) int {
	if len(chunkKeys) == 0 {
		return 0
	}

	type result struct {
		chunk *storage.Chunk
		err   error
	}

	workers := e.ParallelFetch
	if workers > len(chunkKeys) {
		workers = len(chunkKeys)
	}

	jobs := make(chan string, len(chunkKeys))
	results := make(chan result, len(chunkKeys))

	// Start workers
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range jobs {
				data, err := e.backend.Get(ctx, key)
				if err != nil {
					results <- result{err: err}
					continue
				}
				c, err := storage.DecodeChunk(key, data)
				if err != nil {
					results <- result{err: err}
					continue
				}
				e.s3Gets.Add(1)
				results <- result{chunk: c}
			}
		}()
	}

	// Send jobs
	for _, key := range chunkKeys {
		jobs <- key
	}
	close(jobs)

	// Wait for all workers then close results
	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results
	prefetched := 0
	for r := range results {
		if r.err != nil || r.chunk == nil {
			continue
		}
		e.mu.Lock()
		for _, p := range r.chunk.Pages {
			if existing := e.cache.Get(p.ID); existing == nil {
				_ = e.cache.Put(p)
				e.prefetchedPages.Add(1)
				prefetched++
			}
		}
		e.mu.Unlock()
	}

	return prefetched
}

// --- Compaction ---

// Compact merges small chunks and removes old unreferenced chunks.
func (e *Engine) Compact(ctx context.Context) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	// Find small chunks (less than half full)
	threshold := storage.PagesPerChunk / 2
	var smallChunks []*manifest.ChunkMeta
	for _, cm := range e.manifest.Chunks {
		if len(cm.Pages) < threshold {
			smallChunks = append(smallChunks, cm)
		}
	}

	if len(smallChunks) < 2 {
		return "Nothing to compact", nil
	}

	// Collect all pages from small chunks
	var allPages []*page.Page
	for _, cm := range smallChunks {
		for _, pid := range cm.Pages {
			p := e.cache.Get(pid)
			if p != nil {
				allPages = append(allPages, p)
				continue
			}
			// Fetch from storage
			loc, ok := e.manifest.GetPageLocation(pid)
			if !ok {
				continue
			}
			data, err := e.backend.Get(ctx, loc.ChunkKey)
			if err != nil {
				continue
			}
			chunk, err := storage.DecodeChunk(loc.ChunkKey, data)
			if err != nil {
				continue
			}
			for _, cp := range chunk.Pages {
				if cp.ID == pid {
					allPages = append(allPages, cp)
					break
				}
			}
		}
	}

	if len(allPages) == 0 {
		return "Nothing to compact", nil
	}

	// Regroup into new chunks
	newChunks := e.groupIntoChunks(allPages)
	newManifest := e.manifest.Clone()

	// Remove old small chunks
	for _, cm := range smallChunks {
		delete(newManifest.Chunks, cm.Key)
	}

	// Write new chunks
	for _, chunk := range newChunks {
		data := chunk.Encode()
		if err := e.backend.Put(ctx, chunk.ID, data); err != nil {
			return "", fmt.Errorf("writing compacted chunk %s: %w", chunk.ID, err)
		}
		e.s3Puts.Add(1)

		bf := bloom.New(chunk.NumPages * 50)
		var pageIDs []uint32
		var minKey, maxKey []byte

		for _, p := range chunk.Pages {
			pageIDs = append(pageIDs, p.ID)
			if p.Type() == page.PageTypeLeaf {
				for _, cell := range p.AllLeafCells() {
					bf.Add(cell.Key)
					if minKey == nil || page.CompareBytes(cell.Key, minKey) < 0 {
						minKey = make([]byte, len(cell.Key))
						copy(minKey, cell.Key)
					}
					if maxKey == nil || page.CompareBytes(cell.Key, maxKey) > 0 {
						maxKey = make([]byte, len(cell.Key))
						copy(maxKey, cell.Key)
					}
				}
			}
		}

		chunkMeta := &manifest.ChunkMeta{
			Key:         chunk.ID,
			Pages:       pageIDs,
			BloomFilter: bf.Encode(),
			MinKey:      string(minKey),
			MaxKey:      string(maxKey),
			SizeBytes:   len(data),
		}
		newManifest.AddChunk(chunkMeta)
	}

	e.manifest = newManifest
	if err := e.saveManifest(ctx); err != nil {
		return "", err
	}

	// Delete old chunk objects
	for _, cm := range smallChunks {
		_ = e.backend.Delete(ctx, cm.Key)
	}

	return fmt.Sprintf("Compacted %d small chunks into %d chunks (%d pages)",
		len(smallChunks), len(newChunks), len(allPages)), nil
}

// GC removes old manifest versions and unreferenced chunks.
func (e *Engine) GC(ctx context.Context, keepVersions int) (string, error) {
	e.mu.RLock()
	currentVersion := e.manifest.Version
	referencedChunks := make(map[string]bool)
	for key := range e.manifest.Chunks {
		referencedChunks[key] = true
	}
	e.mu.RUnlock()

	// List all manifests
	manifests, err := e.backend.List(ctx, "manifests/v")
	if err != nil {
		return "", err
	}

	// Delete old manifests
	deletedManifests := 0
	for _, mkey := range manifests {
		if mkey == manifest.LatestManifestKey {
			continue
		}
		// Parse version from key
		var ver uint64
		fmt.Sscanf(mkey, "manifests/v%d.json", &ver)
		if ver > 0 && ver < currentVersion-uint64(keepVersions) {
			if err := e.backend.Delete(ctx, mkey); err == nil {
				deletedManifests++
			}
		}
	}

	// List all chunks and delete unreferenced
	allChunks, err := e.backend.List(ctx, "chunks/")
	if err != nil {
		return "", err
	}

	deletedChunks := 0
	for _, ckey := range allChunks {
		if !referencedChunks[ckey] {
			if err := e.backend.Delete(ctx, ckey); err == nil {
				deletedChunks++
			}
		}
	}

	return fmt.Sprintf("GC: removed %d old manifests, %d unreferenced chunks", deletedManifests, deletedChunks), nil
}

// --- Stats ---

// Stats returns engine statistics.
type Stats struct {
	PagesRead       int64           `json:"pages_read"`
	PagesWritten    int64           `json:"pages_written"`
	PrefetchedPages int64           `json:"prefetched_pages"`
	CacheHits       int64           `json:"cache_hits"`
	CacheMisses     int64           `json:"cache_misses"`
	CacheHitRate    float64         `json:"cache_hit_rate"`
	S3Gets          int64           `json:"s3_gets"`
	S3Puts          int64           `json:"s3_puts"`
	BloomSkips      int64           `json:"bloom_skips"`
	ManifestVer     uint64          `json:"manifest_version"`
	NumPages        uint32          `json:"num_pages"`
	NumChunks       int             `json:"num_chunks"`
	NumTables       int             `json:"num_tables"`
	NumIndexes      int             `json:"num_indexes"`
	CacheStats      page.CacheStats `json:"cache"`
}

func (e *Engine) Stats() Stats {
	e.mu.RLock()
	defer e.mu.RUnlock()

	cs := e.cache.Stats()
	hits := e.cacheHits.Load()
	misses := e.cacheMisses.Load()
	var hitRate float64
	if total := hits + misses; total > 0 {
		hitRate = float64(hits) / float64(total)
	}

	return Stats{
		PagesRead:       e.pagesRead.Load(),
		PagesWritten:    e.pagesWritten.Load(),
		PrefetchedPages: e.prefetchedPages.Load(),
		CacheHits:       hits,
		CacheMisses:     misses,
		CacheHitRate:    hitRate,
		S3Gets:          e.s3Gets.Load(),
		S3Puts:          e.s3Puts.Load(),
		BloomSkips:      e.bloomSkips.Load(),
		ManifestVer:     e.manifest.Version,
		NumPages:        e.manifest.NumPages,
		NumChunks:       len(e.manifest.Chunks),
		NumTables:       len(e.manifest.Tables),
		NumIndexes:      len(e.manifest.Indexes),
		CacheStats:      cs,
	}
}

// Tables returns the list of table names.
func (e *Engine) Tables() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	var names []string
	for name := range e.manifest.Tables {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Benchmark runs a simple read/write benchmark.
func (e *Engine) Benchmark(ctx context.Context, table string, numOps int) (string, error) {
	start := time.Now()
	for i := 0; i < numOps; i++ {
		key := []byte(fmt.Sprintf("bench-key-%08d", i))
		val := []byte(fmt.Sprintf(`{"key":"bench-key-%08d","value":"bench-value-%08d"}`, i, i))
		if err := e.Insert(ctx, table, key, val); err != nil {
			return "", err
		}
	}
	writeDur := time.Since(start)

	commitStart := time.Now()
	if err := e.Commit(ctx); err != nil {
		return "", err
	}
	commitDur := time.Since(commitStart)

	readStart := time.Now()
	for i := 0; i < numOps; i++ {
		key := []byte(fmt.Sprintf("bench-key-%08d", i))
		if _, err := e.Get(ctx, table, key); err != nil {
			return "", err
		}
	}
	readDur := time.Since(readStart)

	stats := e.Stats()
	result := fmt.Sprintf(
		"Benchmark Results (%d operations):\n"+
			"  Writes:  %v (%.0f ops/sec)\n"+
			"  Commit:  %v\n"+
			"  Reads:   %v (%.0f ops/sec)\n"+
			"  Cache hit rate: %.1f%%\n"+
			"  Pages: %d read, %d written\n"+
			"  S3 ops: %d gets, %d puts\n"+
			"  Bloom filter skips: %d\n",
		numOps,
		writeDur, float64(numOps)/writeDur.Seconds(),
		commitDur,
		readDur, float64(numOps)/readDur.Seconds(),
		stats.CacheHitRate*100,
		stats.PagesRead, stats.PagesWritten,
		stats.S3Gets, stats.S3Puts,
		stats.BloomSkips,
	)

	return result, nil
}
