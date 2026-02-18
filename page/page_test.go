package page

import (
	"fmt"
	"testing"
)

// --- Leaf page tests ---

func TestLeafWriteAndRead(t *testing.T) {
	p := NewPage(1, PageTypeLeaf)

	key := []byte("hello")
	val := []byte("world")
	if err := p.WriteLeafCell(key, val); err != nil {
		t.Fatalf("WriteLeafCell: %v", err)
	}

	if p.NumCells() != 1 {
		t.Fatalf("expected 1 cell, got %d", p.NumCells())
	}

	cell := p.ReadLeafCell(0)
	if string(cell.Key) != "hello" {
		t.Fatalf("expected key 'hello', got %q", string(cell.Key))
	}
	if string(cell.Value) != "world" {
		t.Fatalf("expected value 'world', got %q", string(cell.Value))
	}
}

func TestLeafSortedInsertion(t *testing.T) {
	p := NewPage(1, PageTypeLeaf)

	// Insert out of order; the page should keep them sorted.
	keys := []string{"cherry", "apple", "banana", "date"}
	for _, k := range keys {
		if err := p.WriteLeafCell([]byte(k), []byte("v")); err != nil {
			t.Fatalf("WriteLeafCell(%s): %v", k, err)
		}
	}

	expected := []string{"apple", "banana", "cherry", "date"}
	cells := p.AllLeafCells()
	if len(cells) != len(expected) {
		t.Fatalf("expected %d cells, got %d", len(expected), len(cells))
	}
	for i, e := range expected {
		if string(cells[i].Key) != e {
			t.Fatalf("cell[%d]: expected key %q, got %q", i, e, string(cells[i].Key))
		}
	}
}

func TestLeafSearch(t *testing.T) {
	p := NewPage(1, PageTypeLeaf)

	for i := 0; i < 20; i++ {
		k := fmt.Sprintf("k%04d", i)
		_ = p.WriteLeafCell([]byte(k), []byte("v"))
	}

	// Existing key.
	idx, found := p.LeafSearch([]byte("k0010"))
	if !found {
		t.Fatal("expected to find k0010")
	}
	cell := p.ReadLeafCell(idx)
	if string(cell.Key) != "k0010" {
		t.Fatalf("expected k0010 at idx, got %q", string(cell.Key))
	}

	// Missing key.
	_, found = p.LeafSearch([]byte("k9999"))
	if found {
		t.Fatal("should not find k9999")
	}
}

func TestLeafDelete(t *testing.T) {
	p := NewPage(1, PageTypeLeaf)

	for i := 0; i < 5; i++ {
		_ = p.WriteLeafCell([]byte(fmt.Sprintf("k%d", i)), []byte("v"))
	}

	// Delete k2 (the middle one in sorted order).
	idx, found := p.LeafSearch([]byte("k2"))
	if !found {
		t.Fatal("k2 should exist before delete")
	}
	p.DeleteLeafCell(idx)

	if p.NumCells() != 4 {
		t.Fatalf("expected 4 cells after delete, got %d", p.NumCells())
	}

	_, found = p.LeafSearch([]byte("k2"))
	if found {
		t.Fatal("k2 should not exist after delete")
	}

	// Remaining keys should still be present.
	for _, k := range []string{"k0", "k1", "k3", "k4"} {
		if _, ok := p.LeafSearch([]byte(k)); !ok {
			t.Fatalf("key %s should still exist", k)
		}
	}
}

func TestLeafUpdateInPlace(t *testing.T) {
	p := NewPage(1, PageTypeLeaf)
	_ = p.WriteLeafCell([]byte("key"), []byte("longvalue1234"))

	// Shorter value should fit in place.
	idx, _ := p.LeafSearch([]byte("key"))
	err := p.UpdateLeafCell(idx, []byte("short"))
	if err != nil {
		t.Fatalf("UpdateLeafCell should succeed for shorter value: %v", err)
	}
	cell := p.ReadLeafCell(idx)
	if string(cell.Value) != "short" {
		t.Fatalf("expected 'short', got %q", string(cell.Value))
	}

	// Longer value should fail.
	err = p.UpdateLeafCell(idx, []byte("this-is-a-much-longer-value-that-should-not-fit"))
	if err != ErrPageFull {
		t.Fatalf("expected ErrPageFull, got %v", err)
	}
}

func TestLeafPageFull(t *testing.T) {
	p := NewPage(1, PageTypeLeaf)

	// Fill the page until it reports full.
	bigVal := make([]byte, 200)
	var count int
	for i := 0; i < 1000; i++ {
		key := []byte(fmt.Sprintf("key-%04d", i))
		err := p.WriteLeafCell(key, bigVal)
		if err == ErrPageFull {
			count = i
			break
		}
		if err != nil {
			t.Fatalf("unexpected error at i=%d: %v", i, err)
		}
	}
	if count == 0 {
		t.Fatal("page should have become full eventually")
	}

	// All inserted cells should be readable.
	if int(p.NumCells()) != count {
		t.Fatalf("expected %d cells, got %d", count, p.NumCells())
	}
}

// --- Internal page tests ---

func TestInternalWriteAndRead(t *testing.T) {
	p := NewPage(10, PageTypeInternal)
	p.SetRightmostChild(99)

	if err := p.WriteInternalCell(1, []byte("alpha")); err != nil {
		t.Fatal(err)
	}
	if err := p.WriteInternalCell(2, []byte("gamma")); err != nil {
		t.Fatal(err)
	}
	if err := p.WriteInternalCell(3, []byte("beta")); err != nil {
		t.Fatal(err)
	}

	if p.NumCells() != 3 {
		t.Fatalf("expected 3 cells, got %d", p.NumCells())
	}

	// Should be sorted: alpha, beta, gamma.
	cells := p.AllInternalCells()
	expectedKeys := []string{"alpha", "beta", "gamma"}
	for i, ek := range expectedKeys {
		if string(cells[i].Key) != ek {
			t.Fatalf("cell[%d]: expected key %q, got %q", i, ek, string(cells[i].Key))
		}
	}

	if p.RightmostChild() != 99 {
		t.Fatalf("expected rightmost child 99, got %d", p.RightmostChild())
	}
}

func TestInternalSearch(t *testing.T) {
	p := NewPage(10, PageTypeInternal)
	_ = p.WriteInternalCell(10, []byte("b"))
	_ = p.WriteInternalCell(20, []byte("d"))
	_ = p.WriteInternalCell(30, []byte("f"))
	p.SetRightmostChild(40)

	// Key < "b" should go to left child of "b" = 10.
	child := p.InternalSearch([]byte("a"))
	if child != 10 {
		t.Fatalf("expected child 10 for key 'a', got %d", child)
	}

	// Key >= all separators should go to rightmost child.
	child = p.InternalSearch([]byte("z"))
	if child != 40 {
		t.Fatalf("expected child 40 for key 'z', got %d", child)
	}
}

func TestInternalSearchIdx(t *testing.T) {
	p := NewPage(10, PageTypeInternal)
	_ = p.WriteInternalCell(10, []byte("c"))
	_ = p.WriteInternalCell(20, []byte("f"))
	p.SetRightmostChild(30)

	// Key before first separator.
	child, idx := p.InternalSearchIdx([]byte("a"))
	if child != 10 || idx != 0 {
		t.Fatalf("expected child=10, idx=0; got child=%d, idx=%d", child, idx)
	}

	// Key between separators.
	child, idx = p.InternalSearchIdx([]byte("d"))
	if child != 20 || idx != 1 {
		t.Fatalf("expected child=20, idx=1; got child=%d, idx=%d", child, idx)
	}

	// Key after all separators -> rightmost.
	child, idx = p.InternalSearchIdx([]byte("z"))
	if child != 30 || idx != -1 {
		t.Fatalf("expected child=30, idx=-1; got child=%d, idx=%d", child, idx)
	}
}

// --- Page serialization roundtrip ---

func TestPageSerializationRoundtrip(t *testing.T) {
	p := NewPage(42, PageTypeLeaf)
	p.SetFlag(FlagRoot)
	p.SetNextLeaf(7)
	p.SetPrevLeaf(3)
	_ = p.WriteLeafCell([]byte("key1"), []byte("val1"))
	_ = p.WriteLeafCell([]byte("key2"), []byte("val2"))

	raw := p.Bytes()
	if len(raw) != PageSize {
		t.Fatalf("expected %d bytes, got %d", PageSize, len(raw))
	}

	p2, err := FromBytes(raw)
	if err != nil {
		t.Fatalf("FromBytes: %v", err)
	}

	if p2.ID != 42 {
		t.Fatalf("expected ID 42, got %d", p2.ID)
	}
	if p2.Type() != PageTypeLeaf {
		t.Fatalf("expected leaf type, got %d", p2.Type())
	}
	if p2.Flags()&FlagRoot == 0 {
		t.Fatal("expected FlagRoot to be set")
	}
	if p2.NextLeaf() != 7 {
		t.Fatalf("expected NextLeaf 7, got %d", p2.NextLeaf())
	}
	if p2.PrevLeaf() != 3 {
		t.Fatalf("expected PrevLeaf 3, got %d", p2.PrevLeaf())
	}
	if p2.NumCells() != 2 {
		t.Fatalf("expected 2 cells, got %d", p2.NumCells())
	}

	cell := p2.ReadLeafCell(0)
	if string(cell.Key) != "key1" || string(cell.Value) != "val1" {
		t.Fatalf("cell 0 mismatch: %q=%q", string(cell.Key), string(cell.Value))
	}
	cell = p2.ReadLeafCell(1)
	if string(cell.Key) != "key2" || string(cell.Value) != "val2" {
		t.Fatalf("cell 1 mismatch: %q=%q", string(cell.Key), string(cell.Value))
	}
}

func TestFromBytesInvalidSize(t *testing.T) {
	_, err := FromBytes(make([]byte, 100))
	if err == nil {
		t.Fatal("expected error for invalid page size")
	}
}

func TestCompareBytes(t *testing.T) {
	tests := []struct {
		a, b []byte
		want int
	}{
		{[]byte("a"), []byte("b"), -1},
		{[]byte("b"), []byte("a"), 1},
		{[]byte("abc"), []byte("abc"), 0},
		{[]byte("ab"), []byte("abc"), -1},
		{[]byte("abc"), []byte("ab"), 1},
		{nil, nil, 0},
		{nil, []byte("a"), -1},
		{[]byte("a"), nil, 1},
	}
	for _, tt := range tests {
		got := CompareBytes(tt.a, tt.b)
		if got != tt.want {
			t.Fatalf("CompareBytes(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
		}
	}
}

func TestDeleteOutOfRange(t *testing.T) {
	p := NewPage(1, PageTypeLeaf)
	_ = p.WriteLeafCell([]byte("a"), []byte("b"))
	// Deleting out of range should be a no-op, not panic.
	p.DeleteLeafCell(5)
	if p.NumCells() != 1 {
		t.Fatalf("expected 1 cell, got %d", p.NumCells())
	}
}
