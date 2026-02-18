# s3lite

A SQLite-like database engine that uses S3-compatible object storage as its backend. Data is stored in 4KB pages organized as a B+ tree, grouped into ~1MB chunks, and stored as objects in S3 (or a local filesystem for development).

## Features

- **Page-level storage** with copy-on-write semantics
- **LRU page cache** (buffer pool) to minimize S3 round trips
- **Bloom filters** per chunk for smart page skipping
- **Zone maps** (min/max keys) per chunk for range pruning
- **Write-ahead log (WAL)** for crash recovery
- **Manifest-based page directory** with atomic version updates

### SQL Support

- `CREATE TABLE`, `DROP TABLE`, `INSERT`, `SELECT`, `UPDATE`, `DELETE`
- `CREATE INDEX`, `DROP INDEX` — secondary indexes with automatic query optimization
- `BEGIN`, `COMMIT`, `ROLLBACK` — snapshot isolation transactions
- `INNER JOIN`, `LEFT JOIN`, `RIGHT JOIN`
- `COUNT`, `SUM`, `AVG`, `MIN`, `MAX` aggregates
- `GROUP BY`, `HAVING`, `DISTINCT`, `ORDER BY`, `LIMIT`, `OFFSET`
- `WHERE` with `AND`, `OR`, `NOT`, `LIKE`, `IN`, `NOT IN`, `IS NULL`, `IS NOT NULL`
- Expression support: `+`, `-`, `*`, `/`, comparisons

### Performance

- **Chunk-aware fetching**: one S3 GET fetches an entire chunk (~256 pages), caching all pages
- **Parallel chunk prefetching** with configurable worker pool
- **Compaction**: merges small chunks into larger ones
- **Garbage collection**: removes old manifests and unreferenced chunks
- **Zero SDK dependency**: AWS Signature V4 signing implemented from scratch

## Quick Start

```bash
# Build
go build -o s3lite .

# Start REPL with local storage
./s3lite

# Custom data directory
./s3lite --data /path/to/dir

# Use S3 backend
export AWS_ACCESS_KEY_ID=your-key
export AWS_SECRET_ACCESS_KEY=your-secret
./s3lite --s3-bucket mybucket --s3-region us-east-1

# Execute SQL and exit
./s3lite -e "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)"
```

## REPL Commands

```
s3lite> CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT);
s3lite> INSERT INTO users (id, name, age) VALUES (1, 'Alice', 30), (2, 'Bob', 25);
s3lite> SELECT * FROM users WHERE age > 20 ORDER BY name;
s3lite> CREATE INDEX idx_age ON users (age);
s3lite> SELECT name, COUNT(*) FROM users GROUP BY name HAVING COUNT(*) > 1;
```

### Dot Commands

| Command | Description |
|---------|-------------|
| `.tables` | List all tables |
| `.indexes` | List all indexes |
| `.schema <table>` | Show table schema |
| `.stats` | Show engine statistics |
| `.commit` | Flush all changes to storage |
| `.compact` | Merge small chunks |
| `.gc [n]` | Remove old manifests/chunks (keep n versions) |
| `.bench <n>` | Run benchmark with n operations |
| `.manifest` | Show current manifest info |
| `.quit` | Exit |

## Architecture

```
┌─────────────┐
│   SQL Layer  │  Parser → Executor
├─────────────┤
│   Engine     │  Table/Index ops, Transactions
├─────────────┤
│   B+ Tree    │  Copy-on-write indexing
├─────────────┤
│  Page Cache  │  LRU buffer pool (configurable size)
├─────────────┤
│     WAL      │  Write-ahead log for crash recovery
├─────────────┤
│   Storage    │  S3 or local filesystem backend
└─────────────┘
```

### Storage Model

- **Pages**: Fixed 4KB blocks — the atomic unit of I/O
- **Chunks**: ~1MB objects (256 pages) stored in S3 — the unit of transfer
- **Manifest**: JSON metadata mapping page IDs to chunk locations, versioned atomically
- **Bloom filters**: Per-chunk probabilistic key existence checks
- **Zone maps**: Per-chunk min/max keys for range pruning

## Running Tests

```bash
go test ./...
```

## Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--data` | `s3lite-data` | Local storage directory |
| `--cache` | `4096` | Page cache size (number of pages) |
| `--s3-bucket` | | S3 bucket name (enables S3 backend) |
| `--s3-endpoint` | | S3 endpoint URL |
| `--s3-region` | `us-east-1` | S3 region |
| `--s3-prefix` | | S3 key prefix |
| `-e` | | Execute SQL statement and exit |

## License

MIT
