import { S3LiteClient } from '../src/client';
import {
  S3LiteQueryError,
  S3LiteAuthError,
  S3LiteConnectionError,
} from '../src/errors';

function mockFetch(handler: (url: string, init: RequestInit) => any): typeof globalThis.fetch {
  return (async (input: any, init?: any) => {
    const url = typeof input === 'string' ? input : input.url;
    const result = handler(url, init ?? {});
    return {
      status: result.status ?? 200,
      json: async () => result.body,
    } as Response;
  }) as typeof globalThis.fetch;
}

describe('S3LiteClient', () => {
  describe('health', () => {
    it('returns true when server is healthy', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: mockFetch(() => ({ body: { ok: true } })),
      });
      expect(await client.health()).toBe(true);
    });

    it('returns false when server is unreachable', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: (() => { throw new Error('ECONNREFUSED'); }) as any,
      });
      expect(await client.health()).toBe(false);
    });
  });

  describe('execute', () => {
    it('sends SQL and returns result', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: mockFetch((url, init) => {
          expect(url).toBe('http://localhost:8080/api/v1/execute');
          expect(init.method).toBe('POST');
          const body = JSON.parse(init.body as string);
          expect(body.sql).toBe('SELECT * FROM users');
          return {
            body: {
              ok: true,
              columns: ['id', 'name'],
              rows: [[1, 'Alice'], [2, 'Bob']],
              row_count: 2,
              time_ms: 1.5,
            },
          };
        }),
      });

      const result = await client.execute('SELECT * FROM users');
      expect(result.columns).toEqual(['id', 'name']);
      expect(result.rows).toEqual([[1, 'Alice'], [2, 'Bob']]);
      expect(result.rowCount).toBe(2);
      expect(result.timeMs).toBe(1.5);
    });

    it('throws S3LiteQueryError on SQL error', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: mockFetch(() => ({
          body: { ok: false, error: 'table does not exist' },
        })),
      });

      await expect(client.execute('SELECT * FROM nope')).rejects.toThrow(
        S3LiteQueryError
      );
      try {
        await client.execute('SELECT * FROM nope');
      } catch (err: any) {
        expect(err.sql).toBe('SELECT * FROM nope');
        expect(err.message).toBe('table does not exist');
      }
    });

    it('sends insert and returns row count', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: mockFetch(() => ({
          body: {
            ok: true,
            message: 'Inserted 1 row(s)',
            row_count: 1,
            time_ms: 0.8,
          },
        })),
      });

      const result = await client.execute("INSERT INTO users VALUES (1, 'Alice')");
      expect(result.rowCount).toBe(1);
      expect(result.message).toBe('Inserted 1 row(s)');
    });
  });

  describe('query', () => {
    it('returns typed objects', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: mockFetch(() => ({
          body: {
            ok: true,
            columns: ['id', 'name', 'age'],
            rows: [[1, 'Alice', 30], [2, 'Bob', 25]],
            row_count: 2,
            time_ms: 1.0,
          },
        })),
      });

      const users = await client.query<{ id: number; name: string; age: number }>(
        'SELECT * FROM users'
      );
      expect(users).toEqual([
        { id: 1, name: 'Alice', age: 30 },
        { id: 2, name: 'Bob', age: 25 },
      ]);
    });
  });

  describe('authentication', () => {
    it('sends Bearer token', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        apiKey: 'my-secret',
        fetch: mockFetch((_url, init) => {
          expect((init.headers as Record<string, string>)?.['Authorization']).toBe('Bearer my-secret');
          return { body: { ok: true, tables: [] } };
        }),
      });

      await client.tables();
    });

    it('throws S3LiteAuthError on 401', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        apiKey: 'wrong',
        fetch: mockFetch(() => ({
          status: 401,
          body: { ok: false, error: 'unauthorized' },
        })),
      });

      await expect(client.tables()).rejects.toThrow(S3LiteAuthError);
    });
  });

  describe('connection errors', () => {
    it('throws S3LiteConnectionError on network failure', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: (() => { throw new Error('ECONNREFUSED'); }) as any,
      });

      await expect(client.execute('SELECT 1')).rejects.toThrow(
        S3LiteConnectionError
      );
    });
  });

  describe('tables', () => {
    it('returns table names', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: mockFetch(() => ({
          body: { ok: true, tables: ['users', 'products'] },
        })),
      });

      expect(await client.tables()).toEqual(['users', 'products']);
    });
  });

  describe('schema', () => {
    it('returns table schema', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: mockFetch((url) => {
          expect(url).toContain('/api/v1/tables/users/schema');
          return {
            body: {
              ok: true,
              schema: {
                table: 'users',
                root_page: 0,
                row_count: 10,
                columns: [
                  { name: 'id', type: 'int', pk: true },
                  { name: 'name', type: 'text' },
                ],
              },
            },
          };
        }),
      });

      const schema = await client.schema('users');
      expect(schema.table).toBe('users');
      expect(schema.rowCount).toBe(10);
      expect(schema.columns).toHaveLength(2);
      expect(schema.columns[0].pk).toBe(true);
    });
  });

  describe('indexes', () => {
    it('returns index list', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: mockFetch(() => ({
          body: {
            ok: true,
            indexes: [
              { name: 'idx_email', table: 'users', columns: ['email'], unique: true },
            ],
          },
        })),
      });

      const indexes = await client.indexes();
      expect(indexes).toHaveLength(1);
      expect(indexes[0].name).toBe('idx_email');
      expect(indexes[0].unique).toBe(true);
    });
  });

  describe('stats', () => {
    it('returns engine stats', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: mockFetch(() => ({
          body: {
            ok: true,
            stats: {
              pages_read: 100,
              pages_written: 50,
              cache_hit_rate: 0.95,
              num_tables: 3,
            },
          },
        })),
      });

      const stats = await client.stats();
      expect(stats.pages_read).toBe(100);
      expect(stats.cache_hit_rate).toBe(0.95);
    });
  });

  describe('flush', () => {
    it('calls commit endpoint', async () => {
      let called = false;
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: mockFetch((url, init) => {
          if (url.includes('/api/v1/commit')) {
            called = true;
            expect(init.method).toBe('POST');
          }
          return { body: { ok: true, message: 'committed' } };
        }),
      });

      await client.flush();
      expect(called).toBe(true);
    });
  });

  describe('compact', () => {
    it('returns message', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: mockFetch(() => ({
          body: { ok: true, message: 'Compacted 2 chunks' },
        })),
      });

      expect(await client.compact()).toBe('Compacted 2 chunks');
    });
  });

  describe('gc', () => {
    it('sends keep option', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: mockFetch((_url, init) => {
          const body = JSON.parse(init.body as string);
          expect(body.keep).toBe(3);
          return { body: { ok: true, message: 'GC done' } };
        }),
      });

      expect(await client.gc({ keep: 3 })).toBe('GC done');
    });

    it('defaults keep to 5', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: mockFetch((_url, init) => {
          const body = JSON.parse(init.body as string);
          expect(body.keep).toBe(5);
          return { body: { ok: true, message: 'GC done' } };
        }),
      });

      await client.gc();
    });
  });

  describe('transaction', () => {
    it('executes BEGIN, operations, COMMIT in order', async () => {
      const calls: string[] = [];
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: mockFetch((url, init) => {
          if (url.includes('/api/v1/session') && init.method === 'POST' && !url.includes('execute')) {
            return { body: { ok: true, session_id: 'sess-1' } };
          }
          if (url.includes('/api/v1/session/sess-1') && init.method === 'DELETE') {
            calls.push('DELETE_SESSION');
            return { body: { ok: true } };
          }
          if (url.includes('/api/v1/execute')) {
            const body = JSON.parse(init.body as string);
            calls.push(body.sql);
            expect(body.session).toBe('sess-1');
            return {
              body: {
                ok: true,
                message: body.sql,
                row_count: body.sql.startsWith('INSERT') ? 1 : 0,
                time_ms: 0.5,
              },
            };
          }
          return { body: { ok: true } };
        }),
      });

      await client.transaction(async (tx) => {
        await tx.execute("INSERT INTO users VALUES (1, 'Alice')");
        await tx.execute("INSERT INTO users VALUES (2, 'Bob')");
      });

      expect(calls).toEqual([
        'BEGIN',
        "INSERT INTO users VALUES (1, 'Alice')",
        "INSERT INTO users VALUES (2, 'Bob')",
        'COMMIT',
        'DELETE_SESSION',
      ]);
    });

    it('rolls back on error', async () => {
      const calls: string[] = [];
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: mockFetch((url, init) => {
          if (url.includes('/api/v1/session') && init.method === 'POST' && !url.includes('execute')) {
            return { body: { ok: true, session_id: 'sess-2' } };
          }
          if (url.includes('/api/v1/session/sess-2') && init.method === 'DELETE') {
            calls.push('DELETE_SESSION');
            return { body: { ok: true } };
          }
          if (url.includes('/api/v1/execute')) {
            const body = JSON.parse(init.body as string);
            calls.push(body.sql);
            return { body: { ok: true, row_count: 0, time_ms: 0.1 } };
          }
          return { body: { ok: true } };
        }),
      });

      await expect(
        client.transaction(async (tx) => {
          await tx.execute("INSERT INTO users VALUES (1, 'Alice')");
          throw new Error('oops');
        })
      ).rejects.toThrow('oops');

      expect(calls).toEqual([
        'BEGIN',
        "INSERT INTO users VALUES (1, 'Alice')",
        'ROLLBACK',
        'DELETE_SESSION',
      ]);
    });

    it('supports query within transaction', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: mockFetch((url, init) => {
          if (url.includes('/api/v1/session') && init.method === 'POST' && !url.includes('execute')) {
            return { body: { ok: true, session_id: 'sess-3' } };
          }
          if (url.includes('/api/v1/session/sess-3') && init.method === 'DELETE') {
            return { body: { ok: true } };
          }
          if (url.includes('/api/v1/execute')) {
            const body = JSON.parse(init.body as string);
            if (body.sql.startsWith('SELECT')) {
              return {
                body: {
                  ok: true,
                  columns: ['id', 'name'],
                  rows: [[1, 'Alice']],
                  row_count: 1,
                  time_ms: 0.5,
                },
              };
            }
            return { body: { ok: true, row_count: 0, time_ms: 0.1 } };
          }
          return { body: { ok: true } };
        }),
      });

      let result: any[] = [];
      await client.transaction(async (tx) => {
        result = await tx.query('SELECT * FROM users');
      });

      expect(result).toEqual([{ id: 1, name: 'Alice' }]);
    });
  });

  describe('close', () => {
    it('resolves without error', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080',
        fetch: mockFetch(() => ({ body: { ok: true } })),
      });
      await expect(client.close()).resolves.toBeUndefined();
    });
  });

  describe('url normalization', () => {
    it('strips trailing slash', async () => {
      const client = new S3LiteClient({
        url: 'http://localhost:8080/',
        fetch: mockFetch((url) => {
          expect(url).toBe('http://localhost:8080/api/v1/health');
          return { body: { ok: true } };
        }),
      });
      await client.health();
    });
  });
});
