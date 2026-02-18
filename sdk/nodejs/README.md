# @s3lite/client

Node.js client for the s3lite HTTP API. Zero runtime dependencies, TypeScript-first.

## Requirements

- Node.js 18+ (uses built-in `fetch`)
- s3lite server running with `--port` flag

## Installation

```bash
npm install @s3lite/client
```

## Quick Start

```typescript
import { S3LiteClient } from '@s3lite/client';

const client = new S3LiteClient({
  url: 'http://localhost:8080',
  apiKey: 'your-api-key', // optional
});

// Execute SQL
await client.execute('CREATE TABLE users (id INT PRIMARY KEY, name TEXT)');
await client.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");

// Query with typed results
const users = await client.query<{ id: number; name: string }>('SELECT * FROM users');
console.log(users); // [{ id: 1, name: 'Alice' }]

// Transactions
await client.transaction(async (tx) => {
  await tx.execute("INSERT INTO users (id, name) VALUES (2, 'Bob')");
  await tx.execute("INSERT INTO users (id, name) VALUES (3, 'Charlie')");
});

// Utility methods
const tables = await client.tables();
const schema = await client.schema('users');
const indexes = await client.indexes();
const stats = await client.stats();

// Maintenance
await client.flush();
await client.compact();
await client.gc({ keep: 3 });

// Health check
const ok = await client.health();

await client.close();
```

## API

### `new S3LiteClient(options)`

| Option   | Type     | Description                        |
|----------|----------|------------------------------------|
| `url`    | `string` | Server URL (required)              |
| `apiKey` | `string` | Bearer token for authentication    |
| `fetch`  | `fetch`  | Custom fetch implementation        |

### Methods

| Method                          | Returns              | Description                    |
|---------------------------------|----------------------|--------------------------------|
| `execute(sql)`                  | `ExecuteResult`      | Execute raw SQL                |
| `query<T>(sql)`                 | `T[]`                | Query and map rows to objects  |
| `transaction(fn)`               | `void`               | Run function in a transaction  |
| `tables()`                      | `string[]`           | List table names               |
| `schema(table)`                 | `TableSchema`        | Get table schema               |
| `indexes()`                     | `IndexInfo[]`        | List indexes                   |
| `stats()`                       | `Stats`              | Engine statistics              |
| `flush()`                       | `void`               | Flush dirty pages              |
| `compact()`                     | `string`             | Compact chunks                 |
| `gc(options?)`                  | `string`             | Garbage collect                |
| `health()`                      | `boolean`            | Health check                   |
| `close()`                       | `void`               | Close client                   |

### Error Types

- `S3LiteError` — base error
- `S3LiteQueryError` — SQL execution error (includes `.sql` property)
- `S3LiteAuthError` — authentication failure (401)
- `S3LiteConnectionError` — network error
