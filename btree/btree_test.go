package btree

import (
	"context"
	"fmt"
	"testing"

	"github.com/sjcotto/s3lite/page"
)

// memFetcher is a simple in-memory page fetcher for testing.
type memFetcher struct {
	pages  map[uint32]*page.Page
	nextID uint32
}

func newMemFetcher() *memFetcher {
	return &memFetcher{pages: make(map[uint32]*page.Page)}
}

func (m *memFetcher) FetchPage(_ context.Context, pageID uint32) (*page.Page, error) {
	p, ok := m.pages[pageID]
	if !ok {
		return nil, fmt.Errorf("page %d not found", pageID)
	}
	return p, nil
}

func (m *memFetcher) AllocPage(pageType byte) (*page.Page, error) {
	id := m.nextID
	m.nextID++
	p := page.NewPage(id, pageType)
	m.pages[id] = p
	return p, nil
}

func (m *memFetcher) MarkDirty(p *page.Page) error {
	p.Dirty = true
	m.pages[p.ID] = p
	return nil
}

func TestBTreeInsertAndGet(t *testing.T) {
	ctx := context.Background()
	f := newMemFetcher()

	tree, err := NewEmpty(f)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("val-%04d", i))
		if err := tree.Put(ctx, key, val); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val, err := tree.Get(ctx, key)
		if err != nil {
			t.Fatalf("get %d: %v", i, err)
		}
		expected := fmt.Sprintf("val-%04d", i)
		if string(val) != expected {
			t.Fatalf("get %d: expected %q, got %q", i, expected, string(val))
		}
	}
}

func TestBTreeLargeInsert(t *testing.T) {
	ctx := context.Background()
	f := newMemFetcher()

	tree, err := NewEmpty(f)
	if err != nil {
		t.Fatal(err)
	}

	n := 100000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("bench-key-%08d", i))
		val := []byte(fmt.Sprintf("bench-value-%08d", i))
		if err := tree.Put(ctx, key, val); err != nil {
			t.Fatalf("insert %d: %v (pages=%d)", i, err, f.nextID)
		}
	}

	missing := 0
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("bench-key-%08d", i))
		_, err := tree.Get(ctx, key)
		if err != nil {
			missing++
			if missing <= 5 {
				t.Logf("missing key %d: %v", i, err)
			}
		}
	}
	if missing > 0 {
		t.Fatalf("%d of %d keys missing", missing, n)
	}

	count, err := tree.Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if count != n {
		t.Fatalf("expected count %d, got %d", n, count)
	}
	t.Logf("OK: %d keys, %d pages", n, f.nextID)
}

func TestBTreeScanRange(t *testing.T) {
	ctx := context.Background()
	f := newMemFetcher()

	tree, err := NewEmpty(f)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("k%04d", i))
		val := []byte(fmt.Sprintf("v%04d", i))
		if err := tree.Put(ctx, key, val); err != nil {
			t.Fatalf("insert %d: %v", i, err)
		}
	}

	// Range scan: keys 100-199
	cells, err := tree.ScanRange(ctx, []byte("k0100"), []byte("k0200"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cells) != 100 {
		t.Fatalf("expected 100 range results, got %d", len(cells))
	}
	if string(cells[0].Key) != "k0100" {
		t.Fatalf("expected first key k0100, got %s", string(cells[0].Key))
	}
	if string(cells[99].Key) != "k0199" {
		t.Fatalf("expected last key k0199, got %s", string(cells[99].Key))
	}
}

func TestBTreeDelete(t *testing.T) {
	ctx := context.Background()
	f := newMemFetcher()

	tree, err := NewEmpty(f)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		val := []byte(fmt.Sprintf("val-%04d", i))
		_ = tree.Put(ctx, key, val)
	}

	// Delete every other key
	for i := 0; i < 100; i += 2 {
		key := []byte(fmt.Sprintf("key-%04d", i))
		if err := tree.Delete(ctx, key); err != nil {
			t.Fatalf("delete %d: %v", i, err)
		}
	}

	// Verify
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		_, err := tree.Get(ctx, key)
		if i%2 == 0 {
			if err == nil {
				t.Fatalf("key %d should be deleted", i)
			}
		} else {
			if err != nil {
				t.Fatalf("key %d should exist: %v", i, err)
			}
		}
	}
}

func TestBTreeUpdate(t *testing.T) {
	ctx := context.Background()
	f := newMemFetcher()

	tree, err := NewEmpty(f)
	if err != nil {
		t.Fatal(err)
	}

	key := []byte("test-key")
	_ = tree.Put(ctx, key, []byte("version-1"))

	val, _ := tree.Get(ctx, key)
	if string(val) != "version-1" {
		t.Fatalf("expected version-1, got %s", string(val))
	}

	_ = tree.Put(ctx, key, []byte("version-2"))
	val, _ = tree.Get(ctx, key)
	if string(val) != "version-2" {
		t.Fatalf("expected version-2, got %s", string(val))
	}
}
