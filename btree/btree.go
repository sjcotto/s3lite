// Package btree implements a B+ tree backed by the page cache.
// This is the core indexing structure, supporting point lookups,
// range scans, insertions, and deletions.
//
// The B+ tree stores key-value pairs in leaf nodes, with internal
// nodes containing separator keys and child pointers. Leaf nodes
// are linked for efficient range scans.
//
// Copy-on-write: when a node is modified, the modified page is
// written back to the cache (marked dirty), and eventually flushed
// to new chunks in storage. The original chunk is never modified.
package btree

import (
	"context"
	"fmt"

	"github.com/sjcotto/s3lite/page"
)

// PageFetcher is the interface the B-tree uses to read and allocate pages.
type PageFetcher interface {
	// FetchPage reads a page by ID, checking cache first then storage.
	FetchPage(ctx context.Context, pageID uint32) (*page.Page, error)
	// AllocPage creates a new empty page with the given type.
	AllocPage(pageType byte) (*page.Page, error)
	// MarkDirty marks a page as modified (needs to be flushed).
	MarkDirty(p *page.Page) error
}

// BTree is a B+ tree index.
type BTree struct {
	RootPage uint32
	fetcher  PageFetcher
}

// New creates a new B-tree backed by the given page fetcher.
func New(rootPage uint32, fetcher PageFetcher) *BTree {
	return &BTree{
		RootPage: rootPage,
		fetcher:  fetcher,
	}
}

// NewEmpty creates a new empty B-tree with a fresh root leaf page.
func NewEmpty(fetcher PageFetcher) (*BTree, error) {
	root, err := fetcher.AllocPage(page.PageTypeLeaf)
	if err != nil {
		return nil, fmt.Errorf("allocating root page: %w", err)
	}
	root.SetFlag(page.FlagRoot)
	if err := fetcher.MarkDirty(root); err != nil {
		return nil, err
	}
	return &BTree{
		RootPage: root.ID,
		fetcher:  fetcher,
	}, nil
}

// Get looks up a key and returns the value, or ErrNotFound.
func (t *BTree) Get(ctx context.Context, key []byte) ([]byte, error) {
	leafPage, _, err := t.findLeaf(ctx, key)
	if err != nil {
		return nil, err
	}

	idx, found := leafPage.LeafSearch(key)
	if !found {
		return nil, page.ErrNotFound
	}

	cell := leafPage.ReadLeafCell(idx)
	return cell.Value, nil
}

// Has checks if a key exists.
func (t *BTree) Has(ctx context.Context, key []byte) (bool, error) {
	_, err := t.Get(ctx, key)
	if err == page.ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// Put inserts or updates a key-value pair.
func (t *BTree) Put(ctx context.Context, key, value []byte) error {
	// Find the leaf page
	leafPage, parents, err := t.findLeafWithParents(ctx, key)
	if err != nil {
		return err
	}

	// Check if key already exists
	idx, found := leafPage.LeafSearch(key)
	if found {
		// Try in-place update
		err := leafPage.UpdateLeafCell(idx, value)
		if err == nil {
			return t.fetcher.MarkDirty(leafPage)
		}
		// Doesn't fit — delete and reinsert
		leafPage.DeleteLeafCell(idx)
	}

	// Try to insert
	err = leafPage.WriteLeafCell(key, value)
	if err == nil {
		return t.fetcher.MarkDirty(leafPage)
	}

	// Page is full — need to split
	if err == page.ErrPageFull {
		return t.splitAndInsert(ctx, leafPage, parents, key, value)
	}
	return err
}

// Delete removes a key from the tree.
func (t *BTree) Delete(ctx context.Context, key []byte) error {
	leafPage, _, err := t.findLeaf(ctx, key)
	if err != nil {
		return err
	}

	idx, found := leafPage.LeafSearch(key)
	if !found {
		return page.ErrNotFound
	}

	leafPage.DeleteLeafCell(idx)
	return t.fetcher.MarkDirty(leafPage)
}

// findLeaf traverses from root to the leaf page containing the key.
func (t *BTree) findLeaf(ctx context.Context, key []byte) (*page.Page, []parentInfo, error) {
	return t.findLeafWithParents(ctx, key)
}

// parentInfo tracks the path from root to leaf for splits.
type parentInfo struct {
	pageID uint32
	// which child index was followed
	childIdx int
}

// findLeafWithParents traverses from root to leaf, recording the path.
func (t *BTree) findLeafWithParents(ctx context.Context, key []byte) (*page.Page, []parentInfo, error) {
	var parents []parentInfo

	p, err := t.fetcher.FetchPage(ctx, t.RootPage)
	if err != nil {
		return nil, nil, fmt.Errorf("fetching root page %d: %w", t.RootPage, err)
	}

	for p.Type() == page.PageTypeInternal {
		childID, idx := p.InternalSearchIdx(key)
		parents = append(parents, parentInfo{pageID: p.ID, childIdx: idx})

		p, err = t.fetcher.FetchPage(ctx, childID)
		if err != nil {
			return nil, nil, fmt.Errorf("fetching page %d: %w", childID, err)
		}
	}

	return p, parents, nil
}

// splitAndInsert handles the case where a leaf is full and needs splitting.
func (t *BTree) splitAndInsert(ctx context.Context, leaf *page.Page, parents []parentInfo, key, value []byte) error {
	// Collect all existing cells plus the new one
	cells := leaf.AllLeafCells()
	newCell := page.LeafCell{Key: key, Value: value}

	// Find insertion position
	pos := 0
	for pos < len(cells) && page.CompareBytes(cells[pos].Key, key) < 0 {
		pos++
	}

	// Insert new cell into the sorted list
	allCells := make([]page.LeafCell, 0, len(cells)+1)
	allCells = append(allCells, cells[:pos]...)
	allCells = append(allCells, newCell)
	allCells = append(allCells, cells[pos:]...)

	// Split roughly in half
	mid := len(allCells) / 2

	// Create new right leaf
	rightLeaf, err := t.fetcher.AllocPage(page.PageTypeLeaf)
	if err != nil {
		return err
	}

	// Rewrite left leaf (reuse existing page)
	leftLeaf := page.NewPage(leaf.ID, page.PageTypeLeaf)

	// Fill left leaf
	for _, cell := range allCells[:mid] {
		if err := leftLeaf.WriteLeafCell(cell.Key, cell.Value); err != nil {
			return fmt.Errorf("writing to left leaf: %w", err)
		}
	}

	// Fill right leaf
	for _, cell := range allCells[mid:] {
		if err := rightLeaf.WriteLeafCell(cell.Key, cell.Value); err != nil {
			return fmt.Errorf("writing to right leaf: %w", err)
		}
	}

	// Link leaves
	rightLeaf.SetNextLeaf(leaf.NextLeaf())
	rightLeaf.SetPrevLeaf(leaf.ID)
	leftLeaf.SetNextLeaf(rightLeaf.ID)
	leftLeaf.SetPrevLeaf(leaf.PrevLeaf())

	// Update next leaf's prev pointer if it exists
	if leaf.NextLeaf() != 0 {
		nextPage, err := t.fetcher.FetchPage(ctx, leaf.NextLeaf())
		if err == nil {
			nextPage.SetPrevLeaf(rightLeaf.ID)
			_ = t.fetcher.MarkDirty(nextPage)
		}
	}

	// Copy left leaf data back to original page
	copy(leaf.Data[:], leftLeaf.Data[:])
	leaf.Dirty = true

	if err := t.fetcher.MarkDirty(leaf); err != nil {
		return err
	}
	if err := t.fetcher.MarkDirty(rightLeaf); err != nil {
		return err
	}

	// The separator key is the first key in the right leaf
	separatorKey := make([]byte, len(allCells[mid].Key))
	copy(separatorKey, allCells[mid].Key)

	// Insert separator into parent
	return t.insertIntoParent(ctx, leaf.ID, separatorKey, rightLeaf.ID, parents)
}

// insertIntoParent inserts a separator key into the parent node after a child split.
//
// When a child node splits:
//   - leftID: the original child page (contains lower keys)
//   - key: the separator key
//   - rightID: the new page (contains upper keys)
//
// We model an internal node as an alternating sequence:
//   ptr[0] key[0] ptr[1] key[1] ... ptr[N-1] key[N-1] ptr[N]
// where ptr[N] = rightmostChild.
//
// When ptr[i] splits, we insert the separator between i and i+1:
//   ... ptr[i] separator rightID key[i] ptr[i+1] ...
func (t *BTree) insertIntoParent(ctx context.Context, leftID uint32, key []byte, rightID uint32, parents []parentInfo) error {
	if len(parents) == 0 {
		return t.createNewRoot(leftID, key, rightID)
	}

	parentIdx := len(parents) - 1
	parent := parents[parentIdx]

	parentPage, err := t.fetcher.FetchPage(ctx, parent.pageID)
	if err != nil {
		return err
	}

	// Extract the full pointer/key structure from the parent
	ptrs, keys := extractNode(parentPage)

	// Find which pointer was the child that split.
	// parent.childIdx from InternalSearchIdx: if >= 0, we followed
	// cell[childIdx].leftChild = ptrs[childIdx]. If -1, we followed
	// rightmostChild = ptrs[len(keys)].
	var splitPos int
	if parent.childIdx >= 0 {
		splitPos = parent.childIdx
	} else {
		splitPos = len(keys) // rightmost pointer
	}

	// Insert separator + rightID after splitPos.
	// Before: ... ptrs[splitPos] keys[splitPos] ptrs[splitPos+1] ...
	// After:  ... ptrs[splitPos] separator rightID keys[splitPos] ptrs[splitPos+1] ...
	// (ptrs[splitPos] == leftID, which is already correct)
	newKeys := make([][]byte, 0, len(keys)+1)
	newPtrs := make([]uint32, 0, len(ptrs)+1)

	// Copy pointers and keys up to split point
	for i := 0; i <= splitPos; i++ {
		newPtrs = append(newPtrs, ptrs[i])
		if i < splitPos {
			newKeys = append(newKeys, keys[i])
		}
	}
	// Insert separator and rightID
	newKeys = append(newKeys, key)
	newPtrs = append(newPtrs, rightID)
	// Copy remaining
	for i := splitPos; i < len(keys); i++ {
		newKeys = append(newKeys, keys[i])
	}
	for i := splitPos + 1; i < len(ptrs); i++ {
		newPtrs = append(newPtrs, ptrs[i])
	}

	// Try to rebuild the page with the new entries
	if canFitInternal(newKeys) {
		rebuildInternalPage(parentPage, newPtrs, newKeys)
		return t.fetcher.MarkDirty(parentPage)
	}

	// Parent is full — need to split the internal node
	return t.splitInternal(ctx, parentPage, parents[:parentIdx], newPtrs, newKeys)
}

// extractNode extracts the alternating ptr/key structure from an internal page.
// Returns ptrs (len = N+1) and keys (len = N) where the last ptr is rightmostChild.
func extractNode(p *page.Page) ([]uint32, [][]byte) {
	n := int(p.NumCells())
	ptrs := make([]uint32, 0, n+1)
	keys := make([][]byte, 0, n)
	for i := 0; i < n; i++ {
		cell := p.ReadInternalCell(i)
		ptrs = append(ptrs, cell.LeftChild)
		keys = append(keys, cell.Key)
	}
	ptrs = append(ptrs, p.RightmostChild())
	return ptrs, keys
}

// canFitInternal checks if the given keys fit in one internal page.
func canFitInternal(keys [][]byte) bool {
	headerSize := page.HeaderSize + page.InternalExtra
	ptrArraySize := (len(keys)) * page.CellPtrSize
	contentSize := 0
	for _, k := range keys {
		contentSize += 4 + 2 + len(k) // leftChild(4) + keySize(2) + key
	}
	return headerSize+ptrArraySize+contentSize <= page.PageSize
}

// rebuildInternalPage clears and rewrites an internal page with new ptrs/keys.
func rebuildInternalPage(p *page.Page, ptrs []uint32, keys [][]byte) {
	newPage := page.NewPage(p.ID, page.PageTypeInternal)
	for i, k := range keys {
		_ = newPage.WriteInternalCell(ptrs[i], k)
	}
	newPage.SetRightmostChild(ptrs[len(keys)])
	copy(p.Data[:], newPage.Data[:])
	p.Dirty = true
}

// createNewRoot creates a new root node with two children.
func (t *BTree) createNewRoot(leftID uint32, key []byte, rightID uint32) error {
	newRoot, err := t.fetcher.AllocPage(page.PageTypeInternal)
	if err != nil {
		return err
	}
	newRoot.SetFlag(page.FlagRoot)
	_ = newRoot.WriteInternalCell(leftID, key)
	newRoot.SetRightmostChild(rightID)
	if err := t.fetcher.MarkDirty(newRoot); err != nil {
		return err
	}
	t.RootPage = newRoot.ID
	return nil
}

// splitInternal splits an internal node when it can't fit a new separator.
// ptrs and keys represent the desired new content (already including the new separator).
func (t *BTree) splitInternal(ctx context.Context, node *page.Page, parents []parentInfo, ptrs []uint32, keys [][]byte) error {
	// Split at midpoint: the middle key is promoted to the parent.
	mid := len(keys) / 2
	promotedKey := keys[mid]

	// Left node gets keys[:mid] and ptrs[:mid+1]
	leftKeys := keys[:mid]
	leftPtrs := ptrs[:mid+1]

	// Right node gets keys[mid+1:] and ptrs[mid+1:]
	rightKeys := keys[mid+1:]
	rightPtrs := ptrs[mid+1:]

	// Reuse existing page for left
	rebuildInternalPage(node, leftPtrs, leftKeys)

	// Create new page for right
	rightNode, err := t.fetcher.AllocPage(page.PageTypeInternal)
	if err != nil {
		return err
	}
	rebuildInternalPage(rightNode, rightPtrs, rightKeys)

	if err := t.fetcher.MarkDirty(node); err != nil {
		return err
	}
	if err := t.fetcher.MarkDirty(rightNode); err != nil {
		return err
	}

	// Propagate the promoted key up
	return t.insertIntoParent(ctx, node.ID, promotedKey, rightNode.ID, parents)
}

// Scan returns all key-value pairs in order.
func (t *BTree) Scan(ctx context.Context) ([]page.LeafCell, error) {
	// Find leftmost leaf
	p, err := t.fetcher.FetchPage(ctx, t.RootPage)
	if err != nil {
		return nil, err
	}

	for p.Type() == page.PageTypeInternal {
		if p.NumCells() > 0 {
			cell := p.ReadInternalCell(0)
			p, err = t.fetcher.FetchPage(ctx, cell.LeftChild)
		} else {
			p, err = t.fetcher.FetchPage(ctx, p.RightmostChild())
		}
		if err != nil {
			return nil, err
		}
	}

	// Traverse leaf chain
	var results []page.LeafCell
	for {
		cells := p.AllLeafCells()
		results = append(results, cells...)

		next := p.NextLeaf()
		if next == 0 {
			break
		}
		p, err = t.fetcher.FetchPage(ctx, next)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// ScanRange returns key-value pairs where startKey <= key < endKey.
// If startKey is nil, scan from beginning. If endKey is nil, scan to end.
func (t *BTree) ScanRange(ctx context.Context, startKey, endKey []byte) ([]page.LeafCell, error) {
	var p *page.Page
	var err error

	if startKey != nil {
		p, _, err = t.findLeaf(ctx, startKey)
	} else {
		// Find leftmost leaf
		p, err = t.fetcher.FetchPage(ctx, t.RootPage)
		if err != nil {
			return nil, err
		}
		for p.Type() == page.PageTypeInternal {
			if p.NumCells() > 0 {
				cell := p.ReadInternalCell(0)
				p, err = t.fetcher.FetchPage(ctx, cell.LeftChild)
			} else {
				p, err = t.fetcher.FetchPage(ctx, p.RightmostChild())
			}
			if err != nil {
				return nil, err
			}
		}
	}
	if err != nil {
		return nil, err
	}

	var results []page.LeafCell
	for {
		n := int(p.NumCells())
		for i := 0; i < n; i++ {
			cell := p.ReadLeafCell(i)
			if startKey != nil && page.CompareBytes(cell.Key, startKey) < 0 {
				continue
			}
			if endKey != nil && page.CompareBytes(cell.Key, endKey) >= 0 {
				return results, nil
			}
			results = append(results, cell)
		}

		next := p.NextLeaf()
		if next == 0 {
			break
		}
		p, err = t.fetcher.FetchPage(ctx, next)
		if err != nil {
			return nil, err
		}
	}

	return results, nil
}

// Count returns the total number of key-value pairs.
func (t *BTree) Count(ctx context.Context) (int, error) {
	cells, err := t.Scan(ctx)
	if err != nil {
		return 0, err
	}
	return len(cells), nil
}
