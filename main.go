// s3lite - A SQLite-like database with S3-compatible object storage backend.
//
// s3lite stores data in pages organized as a B+ tree, grouped into chunks
// and stored as objects in S3 (or a local filesystem for testing).
// It features:
//   - Page-level storage with copy-on-write semantics
//   - LRU page cache (buffer pool) to minimize S3 round trips
//   - Bloom filters per chunk for smart page skipping
//   - Zone maps (min/max keys) per chunk for range pruning
//   - Write-ahead log (WAL) for crash recovery
//   - Manifest-based page directory with atomic version updates
//   - SQL: CREATE TABLE, INSERT, SELECT (WHERE/ORDER BY/LIMIT/GROUP BY/HAVING), DELETE, UPDATE
//   - JOINs: INNER JOIN, LEFT JOIN, RIGHT JOIN
//   - Aggregates: COUNT, SUM, AVG, MIN, MAX
//   - Secondary indexes: CREATE INDEX, automatic index use
//   - Transactions: BEGIN, COMMIT, ROLLBACK
//   - Compaction and garbage collection
//
// Usage:
//
//	s3lite                      # Start REPL with local storage in ./s3lite-data
//	s3lite --data /path/to/dir  # Custom data directory
//	s3lite --s3-bucket mybucket # Use S3 backend
//	s3lite -e "SQL statement"   # Execute and exit
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/sjcotto/s3lite/engine"
	"github.com/sjcotto/s3lite/sql"
	"github.com/sjcotto/s3lite/storage"
)

func main() {
	// Flags
	dataDir := flag.String("data", "s3lite-data", "Local storage directory")
	cacheSize := flag.Int("cache", 4096, "Page cache size (number of pages)")
	execute := flag.String("e", "", "Execute SQL and exit")

	// HTTP server flags
	port := flag.Int("port", 0, "HTTP server port (0 = REPL mode)")
	apiKey := flag.String("api-key", "", "API key for HTTP server authentication")

	// S3 flags
	s3Bucket := flag.String("s3-bucket", "", "S3 bucket name")
	s3Endpoint := flag.String("s3-endpoint", "", "S3 endpoint URL")
	s3Region := flag.String("s3-region", "us-east-1", "S3 region")
	s3Prefix := flag.String("s3-prefix", "", "S3 key prefix")

	flag.Parse()

	// Allow API key from environment variable
	if *apiKey == "" {
		*apiKey = os.Getenv("S3LITE_API_KEY")
	}

	// Build config
	cfg := engine.Config{
		CacheSize:     *cacheSize,
		DataDir:       *dataDir,
		ParallelFetch: 4,
	}

	if *s3Bucket != "" {
		cfg.S3 = &storage.S3Config{
			Bucket:    *s3Bucket,
			Prefix:    *s3Prefix,
			Endpoint:  *s3Endpoint,
			Region:    *s3Region,
			AccessKey: os.Getenv("AWS_ACCESS_KEY_ID"),
			SecretKey: os.Getenv("AWS_SECRET_ACCESS_KEY"),
		}
	}

	// Open database
	db, err := engine.Open(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening database: %v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	defer func() {
		if err := db.Close(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Error closing database: %v\n", err)
		}
	}()

	exec := sql.NewExecutor(db)

	// HTTP server mode
	if *port > 0 {
		srv := NewServer(db, *apiKey)
		log.Fatal(srv.ListenAndServe(*port))
		return
	}

	// Single command mode
	if *execute != "" {
		result, err := exec.Execute(ctx, *execute)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: %v\n", err)
			os.Exit(1)
		}
		fmt.Print(sql.FormatResult(result))
		return
	}

	// Interactive REPL
	fmt.Println("s3lite - SQLite-like database with S3 storage")
	fmt.Println("=============================================")
	if cfg.S3 != nil {
		fmt.Printf("Backend: S3 (%s/%s)\n", *s3Bucket, *s3Prefix)
	} else {
		fmt.Printf("Backend: local filesystem (%s)\n", *dataDir)
	}
	fmt.Printf("Cache: %d pages (%d MB)\n", *cacheSize, *cacheSize*4096/1024/1024)
	fmt.Println()
	fmt.Println("Type .help for available commands")
	fmt.Println()

	scanner := bufio.NewScanner(os.Stdin)
	var multiLine strings.Builder

	for {
		if multiLine.Len() == 0 {
			fmt.Print("s3lite> ")
		} else {
			fmt.Print("  ... ")
		}

		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		// Dot commands
		if multiLine.Len() == 0 && strings.HasPrefix(line, ".") {
			handleDotCommand(ctx, db, exec, line)
			continue
		}

		multiLine.WriteString(line)
		multiLine.WriteString(" ")

		// Check if statement is complete (ends with ;)
		input := strings.TrimSpace(multiLine.String())
		if !strings.HasSuffix(input, ";") {
			continue
		}

		input = strings.TrimSuffix(input, ";")
		multiLine.Reset()

		start := time.Now()
		result, err := exec.Execute(ctx, input)
		elapsed := time.Since(start)

		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		fmt.Print(sql.FormatResult(result))
		fmt.Printf("Time: %v\n\n", elapsed)
	}
}

func handleDotCommand(ctx context.Context, db *engine.Engine, exec *sql.Executor, line string) {
	parts := strings.Fields(line)
	cmd := parts[0]

	switch cmd {
	case ".quit", ".exit", ".q":
		if err := db.Close(ctx); err != nil {
			fmt.Printf("Error closing: %v\n", err)
		}
		os.Exit(0)

	case ".tables":
		tables := db.Tables()
		if len(tables) == 0 {
			fmt.Println("No tables")
		} else {
			for _, t := range tables {
				fmt.Printf("  %s\n", t)
			}
		}
		fmt.Println()

	case ".indexes":
		indexes := db.IndexNames()
		if len(indexes) == 0 {
			fmt.Println("No indexes")
		} else {
			for _, idx := range indexes {
				meta, err := db.IndexMeta(idx)
				if err != nil {
					continue
				}
				unique := ""
				if meta.Unique {
					unique = "UNIQUE "
				}
				fmt.Printf("  %s%s ON %s(%s)\n", unique, idx, meta.Table, strings.Join(meta.Columns, ", "))
			}
		}
		fmt.Println()

	case ".schema":
		if len(parts) < 2 {
			fmt.Println("Usage: .schema <table_name>")
			return
		}
		meta, err := db.TableMeta(parts[1])
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Printf("Table: %s\n", parts[1])
		fmt.Printf("Root page: %d\n", meta.RootPage)
		fmt.Printf("Row count: %d\n", meta.RowCount)
		fmt.Println("Columns:")
		for _, col := range meta.Columns {
			pk := ""
			if col.PK {
				pk = " PRIMARY KEY"
			}
			null := ""
			if !col.Nullable {
				null = " NOT NULL"
			}
			fmt.Printf("  %-20s %-10s%s%s\n", col.Name, col.Type, pk, null)
		}
		fmt.Println()

	case ".stats":
		stats := db.Stats()
		data, _ := json.MarshalIndent(stats, "", "  ")
		fmt.Println(string(data))
		fmt.Println()

	case ".commit":
		start := time.Now()
		if err := db.Commit(ctx); err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Printf("Committed in %v\n\n", time.Since(start))

	case ".compact":
		start := time.Now()
		result, err := db.Compact(ctx)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Printf("%s (took %v)\n\n", result, time.Since(start))

	case ".gc":
		keep := 5
		if len(parts) > 1 {
			fmt.Sscanf(parts[1], "%d", &keep)
		}
		start := time.Now()
		result, err := db.GC(ctx, keep)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Printf("%s (took %v)\n\n", result, time.Since(start))

	case ".bench":
		n := 1000
		if len(parts) > 1 {
			fmt.Sscanf(parts[1], "%d", &n)
		}
		table := "_benchmark"
		if len(parts) > 2 {
			table = parts[2]
		}

		if !db.TableExists(table) {
			result, err := exec.Execute(ctx, fmt.Sprintf(
				"CREATE TABLE %s (key TEXT PRIMARY KEY, value TEXT)", table))
			if err != nil {
				fmt.Printf("Error creating bench table: %v\n", err)
				return
			}
			_ = result
		}

		result, err := db.Benchmark(ctx, table, n)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
		fmt.Println(result)

	case ".manifest":
		stats := db.Stats()
		fmt.Printf("Manifest version: %d\n", stats.ManifestVer)
		fmt.Printf("Total pages: %d\n", stats.NumPages)
		fmt.Printf("Chunks: %d\n", stats.NumChunks)
		fmt.Printf("Tables: %d\n", stats.NumTables)
		fmt.Printf("Indexes: %d\n", stats.NumIndexes)
		fmt.Println()

	case ".help":
		fmt.Println("Commands:")
		fmt.Println("  .tables        - List all tables")
		fmt.Println("  .indexes       - List all indexes")
		fmt.Println("  .schema <tbl>  - Show table schema")
		fmt.Println("  .stats         - Show engine statistics")
		fmt.Println("  .commit        - Flush all changes to storage")
		fmt.Println("  .compact       - Merge small chunks")
		fmt.Println("  .gc [n]        - Remove old manifests/chunks (keep n versions)")
		fmt.Println("  .bench <n>     - Run benchmark with n operations")
		fmt.Println("  .manifest      - Show current manifest")
		fmt.Println("  .quit          - Exit")
		fmt.Println()
		fmt.Println("SQL:")
		fmt.Println("  CREATE TABLE, DROP TABLE, INSERT, SELECT, UPDATE, DELETE")
		fmt.Println("  CREATE INDEX, DROP INDEX")
		fmt.Println("  BEGIN, COMMIT, ROLLBACK")
		fmt.Println("  JOINs: INNER JOIN, LEFT JOIN, RIGHT JOIN")
		fmt.Println("  Aggregates: COUNT, SUM, AVG, MIN, MAX")
		fmt.Println("  GROUP BY, HAVING, DISTINCT, IN, IS NULL")
		fmt.Println()

	default:
		fmt.Printf("Unknown command: %s (try .help)\n", cmd)
	}
}
