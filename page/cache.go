// Package page - cache.go implements an LRU page cache (buffer pool).
// This is the critical layer that minimizes S3 round trips by keeping
// frequently accessed pages in memory.
package page

import (
	"fmt"
	"sync"
)

// Cache is an LRU page cache / buffer pool.
// It maps page IDs to in-memory Page objects and evicts the least
// recently used pages when the cache is full.
type Cache struct {
	mu       sync.Mutex
	capacity int
	pages    map[uint32]*cacheEntry
	order    *lruList
	hits     int64
	misses   int64
	evicts   int64

	// OnEvict is called when a dirty page is evicted.
	// The caller must set this to flush dirty pages to storage.
	OnEvict func(p *Page) error
}

type cacheEntry struct {
	page *Page
	node *lruNode
}

// NewCache creates a new page cache with the given capacity (number of pages).
func NewCache(capacity int) *Cache {
	return &Cache{
		capacity: capacity,
		pages:    make(map[uint32]*cacheEntry, capacity),
		order:    newLRUList(),
	}
}

// Get retrieves a page from the cache. Returns nil if not found.
func (c *Cache) Get(pageID uint32) *Page {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.pages[pageID]
	if !ok {
		c.misses++
		return nil
	}
	c.hits++
	c.order.moveToFront(entry.node)
	return entry.page
}

// Put adds a page to the cache, evicting the LRU page if necessary.
func (c *Cache) Put(p *Page) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// If already in cache, update and move to front
	if entry, ok := c.pages[p.ID]; ok {
		entry.page = p
		c.order.moveToFront(entry.node)
		return nil
	}

	// Evict if at capacity
	if len(c.pages) >= c.capacity {
		if err := c.evictLocked(); err != nil {
			return fmt.Errorf("cache eviction failed: %w", err)
		}
	}

	node := c.order.pushFront(p.ID)
	c.pages[p.ID] = &cacheEntry{page: p, node: node}
	return nil
}

// Pin marks a page as pinned (cannot be evicted).
func (c *Cache) Pin(pageID uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.pages[pageID]; ok {
		entry.page.Pins++
	}
}

// Unpin decrements the pin count.
func (c *Cache) Unpin(pageID uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if entry, ok := c.pages[pageID]; ok {
		if entry.page.Pins > 0 {
			entry.page.Pins--
		}
	}
}

// FlushAll writes all dirty pages using the OnEvict callback.
func (c *Cache) FlushAll() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, entry := range c.pages {
		if entry.page.Dirty && c.OnEvict != nil {
			if err := c.OnEvict(entry.page); err != nil {
				return err
			}
			entry.page.Dirty = false
		}
	}
	return nil
}

// FlushPage writes a specific dirty page.
func (c *Cache) FlushPage(pageID uint32) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	entry, ok := c.pages[pageID]
	if !ok || !entry.page.Dirty || c.OnEvict == nil {
		return nil
	}
	if err := c.OnEvict(entry.page); err != nil {
		return err
	}
	entry.page.Dirty = false
	return nil
}

// DirtyPages returns all dirty page IDs.
func (c *Cache) DirtyPages() []uint32 {
	c.mu.Lock()
	defer c.mu.Unlock()

	var ids []uint32
	for id, entry := range c.pages {
		if entry.page.Dirty {
			ids = append(ids, id)
		}
	}
	return ids
}

// evictLocked evicts the least recently used unpinned page. Must hold mu.
func (c *Cache) evictLocked() error {
	// Walk from back (LRU end) to find an unpinned page
	node := c.order.back()
	for node != nil {
		entry := c.pages[node.pageID]
		if entry.page.Pins == 0 {
			// Flush if dirty
			if entry.page.Dirty && c.OnEvict != nil {
				if err := c.OnEvict(entry.page); err != nil {
					return err
				}
			}
			c.order.remove(node)
			delete(c.pages, node.pageID)
			c.evicts++
			return nil
		}
		node = node.prev
	}
	return fmt.Errorf("all pages are pinned, cannot evict")
}

// EvictDirty removes all dirty pages from the cache (used by rollback).
func (c *Cache) EvictDirty() {
	c.mu.Lock()
	defer c.mu.Unlock()

	var toRemove []uint32
	for id, entry := range c.pages {
		if entry.page.Dirty {
			toRemove = append(toRemove, id)
		}
	}
	for _, id := range toRemove {
		entry := c.pages[id]
		c.order.remove(entry.node)
		delete(c.pages, id)
	}
}

// Stats returns cache statistics.
func (c *Cache) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{
		Capacity: c.capacity,
		Size:     len(c.pages),
		Hits:     c.hits,
		Misses:   c.misses,
		Evicts:   c.evicts,
	}
}

// CacheStats holds cache performance statistics.
type CacheStats struct {
	Capacity int
	Size     int
	Hits     int64
	Misses   int64
	Evicts   int64
}

// HitRate returns the cache hit rate (0.0 to 1.0).
func (s CacheStats) HitRate() float64 {
	total := s.Hits + s.Misses
	if total == 0 {
		return 0
	}
	return float64(s.Hits) / float64(total)
}

// --- LRU doubly-linked list ---

type lruNode struct {
	pageID uint32
	prev   *lruNode
	next   *lruNode
}

type lruList struct {
	head *lruNode // most recently used
	tail *lruNode // least recently used
}

func newLRUList() *lruList {
	return &lruList{}
}

func (l *lruList) pushFront(pageID uint32) *lruNode {
	node := &lruNode{pageID: pageID}
	if l.head == nil {
		l.head = node
		l.tail = node
	} else {
		node.next = l.head
		l.head.prev = node
		l.head = node
	}
	return node
}

func (l *lruList) moveToFront(node *lruNode) {
	if node == l.head {
		return
	}
	l.remove(node)
	node.prev = nil
	node.next = l.head
	if l.head != nil {
		l.head.prev = node
	}
	l.head = node
	if l.tail == nil {
		l.tail = node
	}
}

func (l *lruList) remove(node *lruNode) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		l.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		l.tail = node.prev
	}
	node.prev = nil
	node.next = nil
}

func (l *lruList) back() *lruNode {
	return l.tail
}
