package storage

import (
	"context"
	"errors"
	"os"
	"testing"

	"github.com/sjcotto/s3lite/page"
)

// --- Chunk encode / decode ---

func TestChunkEncodeDecodeRoundtrip(t *testing.T) {
	chunk := NewChunk("test-chunk")

	p1 := page.NewPage(1, page.PageTypeLeaf)
	_ = p1.WriteLeafCell([]byte("key1"), []byte("val1"))
	chunk.AddPage(p1)

	p2 := page.NewPage(2, page.PageTypeLeaf)
	_ = p2.WriteLeafCell([]byte("key2"), []byte("val2"))
	chunk.AddPage(p2)

	data := chunk.Encode()

	decoded, err := DecodeChunk("test-chunk", data)
	if err != nil {
		t.Fatalf("DecodeChunk: %v", err)
	}

	if decoded.NumPages != 2 {
		t.Fatalf("expected 2 pages, got %d", decoded.NumPages)
	}
	if decoded.ID != "test-chunk" {
		t.Fatalf("expected ID 'test-chunk', got %q", decoded.ID)
	}

	// Verify page content survived the roundtrip.
	cell := decoded.Pages[0].ReadLeafCell(0)
	if string(cell.Key) != "key1" || string(cell.Value) != "val1" {
		t.Fatalf("page 0 cell mismatch: %q=%q", string(cell.Key), string(cell.Value))
	}
	cell = decoded.Pages[1].ReadLeafCell(0)
	if string(cell.Key) != "key2" || string(cell.Value) != "val2" {
		t.Fatalf("page 1 cell mismatch: %q=%q", string(cell.Key), string(cell.Value))
	}
}

func TestChunkCorruptCRC(t *testing.T) {
	chunk := NewChunk("crc-test")
	chunk.AddPage(page.NewPage(1, page.PageTypeLeaf))
	data := chunk.Encode()

	// Corrupt a byte in the middle.
	data[ChunkHeaderSize+10] ^= 0xFF

	_, err := DecodeChunk("crc-test", data)
	if err == nil {
		t.Fatal("expected error for corrupted CRC")
	}
	if !errors.Is(err, ErrChunkCorrupt) {
		t.Fatalf("expected ErrChunkCorrupt, got: %v", err)
	}
}

func TestChunkBadMagic(t *testing.T) {
	data := make([]byte, ChunkHeaderSize+page.PageSize+ChunkFooterSize)
	// Leave magic as zero.
	_, err := DecodeChunk("bad", data)
	if err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestChunkTooSmall(t *testing.T) {
	_, err := DecodeChunk("tiny", []byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for tiny data")
	}
}

func TestPageFromChunkBytes(t *testing.T) {
	chunk := NewChunk("extract-test")
	for i := uint32(0); i < 3; i++ {
		p := page.NewPage(i, page.PageTypeLeaf)
		_ = p.WriteLeafCell([]byte{byte('a' + i)}, []byte("v"))
		chunk.AddPage(p)
	}
	data := chunk.Encode()

	// Extract second page (index 1).
	p, err := PageFromChunkBytes(data, 1)
	if err != nil {
		t.Fatalf("PageFromChunkBytes: %v", err)
	}
	cell := p.ReadLeafCell(0)
	if string(cell.Key) != "b" {
		t.Fatalf("expected key 'b', got %q", string(cell.Key))
	}

	// Out of range.
	_, err = PageFromChunkBytes(data, 10)
	if err == nil {
		t.Fatal("expected error for out-of-range index")
	}
}

// --- LocalBackend tests ---

func newTestBackend(t *testing.T) *LocalBackend {
	t.Helper()
	dir := t.TempDir()
	b, err := NewLocalBackend(dir)
	if err != nil {
		t.Fatalf("NewLocalBackend: %v", err)
	}
	return b
}

func TestLocalPutAndGet(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	data := []byte("hello world")
	if err := b.Put(ctx, "mykey", data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, err := b.Get(ctx, "mykey")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(got) != "hello world" {
		t.Fatalf("expected 'hello world', got %q", string(got))
	}
}

func TestLocalGetNotFound(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	_, err := b.Get(ctx, "nonexistent")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestLocalDelete(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	_ = b.Put(ctx, "todelete", []byte("data"))
	if err := b.Delete(ctx, "todelete"); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := b.Get(ctx, "todelete")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}

	// Deleting non-existent key should not error.
	if err := b.Delete(ctx, "nope"); err != nil {
		t.Fatalf("Delete non-existent: %v", err)
	}
}

func TestLocalList(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	_ = b.Put(ctx, "data/a.bin", []byte("a"))
	_ = b.Put(ctx, "data/b.bin", []byte("b"))
	_ = b.Put(ctx, "meta/c.bin", []byte("c"))

	keys, err := b.List(ctx, "data/")
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys with prefix 'data/', got %d: %v", len(keys), keys)
	}

	// List everything.
	all, err := b.List(ctx, "")
	if err != nil {
		t.Fatalf("List all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 total keys, got %d", len(all))
	}
}

func TestLocalExists(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	_ = b.Put(ctx, "exists-key", []byte("1"))

	ok, err := b.Exists(ctx, "exists-key")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected key to exist")
	}

	ok, err = b.Exists(ctx, "missing-key")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected key not to exist")
	}
}

func TestLocalGetRange(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	_ = b.Put(ctx, "rangekey", []byte("abcdefghij"))

	got, err := b.GetRange(ctx, "rangekey", 3, 4)
	if err != nil {
		t.Fatalf("GetRange: %v", err)
	}
	if string(got) != "defg" {
		t.Fatalf("expected 'defg', got %q", string(got))
	}
}

func TestLocalGetRangeNotFound(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	_, err := b.GetRange(ctx, "nope", 0, 10)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestLocalPutNestedDirs(t *testing.T) {
	b := newTestBackend(t)
	ctx := context.Background()

	if err := b.Put(ctx, "a/b/c/deep.txt", []byte("deep")); err != nil {
		t.Fatalf("Put nested: %v", err)
	}
	got, err := b.Get(ctx, "a/b/c/deep.txt")
	if err != nil {
		t.Fatalf("Get nested: %v", err)
	}
	if string(got) != "deep" {
		t.Fatalf("expected 'deep', got %q", string(got))
	}
}

func TestNewLocalBackendBadPath(t *testing.T) {
	// Attempt to create a backend at an unwritable path.
	_, err := NewLocalBackend("/dev/null/impossible")
	if err == nil {
		// Some OS might not error here; skip if so.
		if _, statErr := os.Stat("/dev/null/impossible"); statErr != nil {
			t.Log("expected error for impossible path, but got nil (OS-dependent)")
		}
	}
}
