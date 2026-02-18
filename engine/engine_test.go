package engine

import (
	"context"
	"os"
	"testing"

	"github.com/sjcotto/s3lite/manifest"
	"github.com/sjcotto/s3lite/page"
)

func newTestEngine(t *testing.T) (*Engine, string) {
	t.Helper()
	dir, err := os.MkdirTemp("", "s3lite-engine-test-*")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })

	cfg := DefaultConfig(dir)
	cfg.CacheSize = 256
	e, err := Open(cfg)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	return e, dir
}

func TestOpenCreatesDB(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	tables := e.Tables()
	if len(tables) != 0 {
		t.Fatalf("expected 0 tables, got %d", len(tables))
	}
}

func TestCreateTable(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	cols := []manifest.ColumnMeta{
		{Name: "id", Type: "int", PK: true},
		{Name: "name", Type: "text"},
	}
	if err := e.CreateTable(ctx, "users", cols); err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	if !e.TableExists("users") {
		t.Fatal("table 'users' should exist")
	}

	// Duplicate table should fail.
	err := e.CreateTable(ctx, "users", cols)
	if err == nil {
		t.Fatal("expected error creating duplicate table")
	}
}

func TestInsertAndGet(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	cols := []manifest.ColumnMeta{
		{Name: "id", Type: "int", PK: true},
		{Name: "data", Type: "text"},
	}
	_ = e.CreateTable(ctx, "t1", cols)

	if err := e.Insert(ctx, "t1", []byte("key1"), []byte("val1")); err != nil {
		t.Fatalf("Insert: %v", err)
	}

	val, err := e.Get(ctx, "t1", []byte("key1"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "val1" {
		t.Fatalf("expected 'val1', got %q", string(val))
	}
}

func TestGetNotFound(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	cols := []manifest.ColumnMeta{{Name: "id", Type: "text", PK: true}}
	_ = e.CreateTable(ctx, "t1", cols)

	_, err := e.Get(ctx, "t1", []byte("missing"))
	if err != page.ErrNotFound {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestScan(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	cols := []manifest.ColumnMeta{{Name: "k", Type: "text", PK: true}}
	_ = e.CreateTable(ctx, "t1", cols)

	for i := 0; i < 10; i++ {
		key := []byte{byte('a' + i)}
		_ = e.Insert(ctx, "t1", key, []byte("v"))
	}

	cells, err := e.Scan(ctx, "t1")
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(cells) != 10 {
		t.Fatalf("expected 10 cells, got %d", len(cells))
	}

	// Keys should be sorted.
	for i := 1; i < len(cells); i++ {
		if page.CompareBytes(cells[i-1].Key, cells[i].Key) >= 0 {
			t.Fatalf("cells not sorted: %q >= %q", cells[i-1].Key, cells[i].Key)
		}
	}
}

func TestDelete(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	cols := []manifest.ColumnMeta{{Name: "k", Type: "text", PK: true}}
	_ = e.CreateTable(ctx, "t1", cols)

	_ = e.Insert(ctx, "t1", []byte("a"), []byte("1"))
	_ = e.Insert(ctx, "t1", []byte("b"), []byte("2"))

	if err := e.Delete(ctx, "t1", []byte("a")); err != nil {
		t.Fatalf("Delete: %v", err)
	}

	_, err := e.Get(ctx, "t1", []byte("a"))
	if err != page.ErrNotFound {
		t.Fatalf("expected ErrNotFound after delete, got %v", err)
	}

	val, err := e.Get(ctx, "t1", []byte("b"))
	if err != nil || string(val) != "2" {
		t.Fatalf("key 'b' should still exist with value '2'")
	}
}

func TestCommit(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	cols := []manifest.ColumnMeta{{Name: "k", Type: "text", PK: true}}
	_ = e.CreateTable(ctx, "t1", cols)
	_ = e.Insert(ctx, "t1", []byte("x"), []byte("y"))

	if err := e.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Data should still be accessible after commit.
	val, err := e.Get(ctx, "t1", []byte("x"))
	if err != nil {
		t.Fatalf("Get after commit: %v", err)
	}
	if string(val) != "y" {
		t.Fatalf("expected 'y', got %q", string(val))
	}
}

func TestPersistenceAcrossReopen(t *testing.T) {
	dir, err := os.MkdirTemp("", "s3lite-persist-test-*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	ctx := context.Background()

	// First session: create table, insert, commit.
	{
		cfg := DefaultConfig(dir)
		cfg.CacheSize = 128
		e, err := Open(cfg)
		if err != nil {
			t.Fatalf("Open (1): %v", err)
		}

		cols := []manifest.ColumnMeta{
			{Name: "id", Type: "text", PK: true},
			{Name: "val", Type: "text"},
		}
		if err := e.CreateTable(ctx, "persist", cols); err != nil {
			t.Fatalf("CreateTable: %v", err)
		}

		for i := 0; i < 50; i++ {
			key := []byte{byte('A' + (i % 26)), byte('0' + (i / 26))}
			val := []byte("value")
			if err := e.Insert(ctx, "persist", key, val); err != nil {
				t.Fatalf("Insert %d: %v", i, err)
			}
		}

		if err := e.Close(ctx); err != nil {
			t.Fatalf("Close (1): %v", err)
		}
	}

	// Second session: reopen and verify.
	{
		cfg := DefaultConfig(dir)
		cfg.CacheSize = 128
		e, err := Open(cfg)
		if err != nil {
			t.Fatalf("Open (2): %v", err)
		}
		defer e.Close(ctx)

		if !e.TableExists("persist") {
			t.Fatal("table 'persist' should exist after reopen")
		}

		cells, err := e.Scan(ctx, "persist")
		if err != nil {
			t.Fatalf("Scan after reopen: %v", err)
		}
		if len(cells) != 50 {
			t.Fatalf("expected 50 cells after reopen, got %d", len(cells))
		}
	}
}

func TestInsertNonExistentTable(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	err := e.Insert(ctx, "nosuch", []byte("k"), []byte("v"))
	if err == nil {
		t.Fatal("expected error inserting into non-existent table")
	}
}

func TestDropTable(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	cols := []manifest.ColumnMeta{{Name: "k", Type: "text", PK: true}}
	_ = e.CreateTable(ctx, "droptarget", cols)

	if err := e.DropTable("droptarget"); err != nil {
		t.Fatalf("DropTable: %v", err)
	}
	if e.TableExists("droptarget") {
		t.Fatal("table should not exist after drop")
	}

	err := e.DropTable("nonexistent")
	if err == nil {
		t.Fatal("expected error dropping non-existent table")
	}
}

func TestStats(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	cols := []manifest.ColumnMeta{{Name: "k", Type: "text", PK: true}}
	_ = e.CreateTable(ctx, "stats_t", cols)
	_ = e.Insert(ctx, "stats_t", []byte("a"), []byte("b"))

	s := e.Stats()
	if s.NumTables != 1 {
		t.Fatalf("expected 1 table in stats, got %d", s.NumTables)
	}
}

func TestCreateAndDropIndex(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	cols := []manifest.ColumnMeta{
		{Name: "id", Type: "int", PK: true},
		{Name: "name", Type: "text"},
	}
	_ = e.CreateTable(ctx, "indexed", cols)
	_ = e.Insert(ctx, "indexed", []byte("1"), []byte(`{"id":1,"name":"Alice"}`))
	_ = e.Insert(ctx, "indexed", []byte("2"), []byte(`{"id":2,"name":"Bob"}`))

	// Create index
	if err := e.CreateIndex(ctx, "idx_name", "indexed", []string{"name"}, false); err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	// FindIndex should find it
	found := e.FindIndex("indexed", "name")
	if found != "idx_name" {
		t.Fatalf("expected to find idx_name, got %q", found)
	}

	// Index names
	names := e.IndexNames()
	if len(names) != 1 || names[0] != "idx_name" {
		t.Fatalf("expected [idx_name], got %v", names)
	}

	// Drop index
	if err := e.DropIndex("idx_name"); err != nil {
		t.Fatalf("DropIndex: %v", err)
	}

	found = e.FindIndex("indexed", "name")
	if found != "" {
		t.Fatalf("expected empty after drop, got %q", found)
	}
}

func TestIndexLookup(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	cols := []manifest.ColumnMeta{
		{Name: "id", Type: "text", PK: true},
		{Name: "email", Type: "text"},
	}
	_ = e.CreateTable(ctx, "users", cols)
	_ = e.Insert(ctx, "users", []byte("u1"), []byte(`{"id":"u1","email":"alice@test.com"}`))
	_ = e.Insert(ctx, "users", []byte("u2"), []byte(`{"id":"u2","email":"bob@test.com"}`))

	_ = e.CreateIndex(ctx, "idx_email", "users", []string{"email"}, false)

	// Use index to look up
	cells, err := e.ScanIndex(ctx, "idx_email", []byte("bob@test.com"), []byte("bob@test.com"))
	if err != nil {
		t.Fatalf("ScanIndex: %v", err)
	}
	if len(cells) != 1 {
		t.Fatalf("expected 1 result from index, got %d", len(cells))
	}
	if string(cells[0].Key) != "u2" {
		t.Fatalf("expected key 'u2', got %q", string(cells[0].Key))
	}
}

func TestBeginAndRollback(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	cols := []manifest.ColumnMeta{{Name: "k", Type: "text", PK: true}}
	_ = e.CreateTable(ctx, "txtest", cols)
	_ = e.Insert(ctx, "txtest", []byte("a"), []byte("1"))

	// Commit before BEGIN so data is persisted to storage
	_ = e.Commit(ctx)

	// Begin transaction
	e.BeginTx()

	// Insert during transaction
	_ = e.Insert(ctx, "txtest", []byte("b"), []byte("2"))

	// Rollback
	e.RollbackTx()

	// Only 'a' should exist
	cells, err := e.Scan(ctx, "txtest")
	if err != nil {
		t.Fatalf("Scan after rollback: %v", err)
	}
	if len(cells) != 1 {
		t.Fatalf("expected 1 cell after rollback, got %d", len(cells))
	}
	if string(cells[0].Key) != "a" {
		t.Fatalf("expected key 'a', got %q", string(cells[0].Key))
	}
}

func TestBeginAndCommitTx(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	cols := []manifest.ColumnMeta{{Name: "k", Type: "text", PK: true}}
	_ = e.CreateTable(ctx, "txtest2", cols)

	e.BeginTx()
	_ = e.Insert(ctx, "txtest2", []byte("x"), []byte("y"))
	if err := e.CommitTx(ctx); err != nil {
		t.Fatalf("CommitTx: %v", err)
	}

	val, err := e.Get(ctx, "txtest2", []byte("x"))
	if err != nil {
		t.Fatalf("Get after CommitTx: %v", err)
	}
	if string(val) != "y" {
		t.Fatalf("expected 'y', got %q", string(val))
	}
}

func TestCompact(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	cols := []manifest.ColumnMeta{{Name: "k", Type: "text", PK: true}}
	_ = e.CreateTable(ctx, "compact_t", cols)

	// Insert a small batch and commit (creates a small chunk)
	for i := 0; i < 5; i++ {
		_ = e.Insert(ctx, "compact_t", []byte{byte('a' + i)}, []byte("v"))
	}
	_ = e.Commit(ctx)

	// Insert another small batch
	for i := 5; i < 10; i++ {
		_ = e.Insert(ctx, "compact_t", []byte{byte('a' + i)}, []byte("v"))
	}
	_ = e.Commit(ctx)

	// Run compaction
	result, err := e.Compact(ctx)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	// Result should be informative (may or may not actually compact depending on chunk sizes)
	if result == "" {
		t.Fatal("expected non-empty result from Compact")
	}

	// Data should still be intact
	cells, err := e.Scan(ctx, "compact_t")
	if err != nil {
		t.Fatalf("Scan after compact: %v", err)
	}
	if len(cells) != 10 {
		t.Fatalf("expected 10 cells after compact, got %d", len(cells))
	}
}

func TestGC(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	cols := []manifest.ColumnMeta{{Name: "k", Type: "text", PK: true}}
	_ = e.CreateTable(ctx, "gc_t", cols)
	_ = e.Insert(ctx, "gc_t", []byte("a"), []byte("1"))
	_ = e.Commit(ctx)
	_ = e.Insert(ctx, "gc_t", []byte("b"), []byte("2"))
	_ = e.Commit(ctx)

	result, err := e.GC(ctx, 1)
	if err != nil {
		t.Fatalf("GC: %v", err)
	}
	if result == "" {
		t.Fatal("expected non-empty GC result")
	}
}

func TestDropTableDropsIndexes(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	cols := []manifest.ColumnMeta{
		{Name: "id", Type: "text", PK: true},
		{Name: "name", Type: "text"},
	}
	_ = e.CreateTable(ctx, "withidx", cols)
	_ = e.CreateIndex(ctx, "idx_n", "withidx", []string{"name"}, false)

	if len(e.IndexNames()) != 1 {
		t.Fatal("expected 1 index before drop")
	}

	_ = e.DropTable("withidx")

	if len(e.IndexNames()) != 0 {
		t.Fatalf("expected 0 indexes after table drop, got %d", len(e.IndexNames()))
	}
}

func TestStatsIncludesIndexes(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	cols := []manifest.ColumnMeta{
		{Name: "id", Type: "text", PK: true},
		{Name: "v", Type: "text"},
	}
	_ = e.CreateTable(ctx, "st", cols)
	_ = e.CreateIndex(ctx, "idx_v", "st", []string{"v"}, false)

	s := e.Stats()
	if s.NumIndexes != 1 {
		t.Fatalf("expected 1 index in stats, got %d", s.NumIndexes)
	}
}

func TestManyInsertsWithSplits(t *testing.T) {
	e, _ := newTestEngine(t)
	ctx := context.Background()
	defer e.Close(ctx)

	cols := []manifest.ColumnMeta{{Name: "k", Type: "text", PK: true}}
	_ = e.CreateTable(ctx, "big", cols)

	n := 500
	for i := 0; i < n; i++ {
		key := []byte{byte(i / 256), byte(i % 256), 'k'}
		val := []byte("some-value-data")
		if err := e.Insert(ctx, "big", key, val); err != nil {
			t.Fatalf("Insert %d: %v", i, err)
		}
	}

	cells, err := e.Scan(ctx, "big")
	if err != nil {
		t.Fatalf("Scan: %v", err)
	}
	if len(cells) != n {
		t.Fatalf("expected %d cells, got %d", n, len(cells))
	}
}
