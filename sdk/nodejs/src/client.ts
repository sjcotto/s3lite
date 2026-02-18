import {
  S3LiteClientOptions,
  ExecuteResult,
  TableSchema,
  IndexInfo,
  Stats,
  GCOptions,
  ApiResponse,
  TransactionExecutor,
} from './types';
import {
  S3LiteError,
  S3LiteQueryError,
  S3LiteAuthError,
  S3LiteConnectionError,
} from './errors';

export class S3LiteClient {
  private readonly url: string;
  private readonly apiKey?: string;
  private readonly _fetch: typeof globalThis.fetch;

  constructor(options: S3LiteClientOptions) {
    this.url = options.url.replace(/\/+$/, '');
    this.apiKey = options.apiKey;
    this._fetch = options.fetch ?? globalThis.fetch;
  }

  private async request(
    method: string,
    path: string,
    body?: any
  ): Promise<ApiResponse> {
    const headers: Record<string, string> = {
      'Content-Type': 'application/json',
    };
    if (this.apiKey) {
      headers['Authorization'] = `Bearer ${this.apiKey}`;
    }

    let response: Response;
    try {
      response = await this._fetch(`${this.url}${path}`, {
        method,
        headers,
        body: body !== undefined ? JSON.stringify(body) : undefined,
      });
    } catch (err: any) {
      throw new S3LiteConnectionError(err.message ?? 'connection failed');
    }

    if (response.status === 401) {
      throw new S3LiteAuthError();
    }

    const data = (await response.json()) as ApiResponse;
    return data;
  }

  async execute(sql: string): Promise<ExecuteResult> {
    const data = await this.request('POST', '/api/v1/execute', { sql });
    if (!data.ok) {
      throw new S3LiteQueryError(data.error ?? 'query failed', sql);
    }
    return {
      columns: data.columns ?? [],
      rows: data.rows ?? [],
      rowCount: data.row_count ?? 0,
      timeMs: data.time_ms ?? 0,
      message: data.message ?? '',
    };
  }

  async query<T extends Record<string, any> = Record<string, any>>(
    sql: string
  ): Promise<T[]> {
    const result = await this.execute(sql);
    return result.rows.map((row) => {
      const obj: Record<string, any> = {};
      for (let i = 0; i < result.columns.length; i++) {
        obj[result.columns[i]] = row[i];
      }
      return obj as T;
    });
  }

  async transaction(fn: (tx: TransactionExecutor) => Promise<void>): Promise<void> {
    // Create a dedicated session
    const sessData = await this.request('POST', '/api/v1/session');
    if (!sessData.ok) {
      throw new S3LiteError(sessData.error ?? 'failed to create session');
    }
    const sessionId = sessData.session_id!;

    const txExecute = async (sql: string): Promise<ExecuteResult> => {
      const data = await this.request('POST', '/api/v1/execute', {
        sql,
        session: sessionId,
      });
      if (!data.ok) {
        throw new S3LiteQueryError(data.error ?? 'query failed', sql);
      }
      return {
        columns: data.columns ?? [],
        rows: data.rows ?? [],
        rowCount: data.row_count ?? 0,
        timeMs: data.time_ms ?? 0,
        message: data.message ?? '',
      };
    };

    const txQuery = async <T extends Record<string, any>>(sql: string): Promise<T[]> => {
      const result = await txExecute(sql);
      return result.rows.map((row) => {
        const obj: Record<string, any> = {};
        for (let i = 0; i < result.columns.length; i++) {
          obj[result.columns[i]] = row[i];
        }
        return obj as T;
      });
    };

    const tx: TransactionExecutor = { execute: txExecute, query: txQuery };

    try {
      await txExecute('BEGIN');
      await fn(tx);
      await txExecute('COMMIT');
    } catch (err) {
      try {
        await txExecute('ROLLBACK');
      } catch {
        // Rollback failed — session cleanup will handle it
      }
      throw err;
    } finally {
      try {
        await this.request('DELETE', `/api/v1/session/${sessionId}`);
      } catch {
        // Best-effort cleanup
      }
    }
  }

  async tables(): Promise<string[]> {
    const data = await this.request('GET', '/api/v1/tables');
    if (!data.ok) {
      throw new S3LiteError(data.error ?? 'failed to list tables');
    }
    return data.tables ?? [];
  }

  async schema(table: string): Promise<TableSchema> {
    const data = await this.request(
      'GET',
      `/api/v1/tables/${encodeURIComponent(table)}/schema`
    );
    if (!data.ok) {
      throw new S3LiteError(data.error ?? 'failed to get schema');
    }
    const s = data.schema;
    return {
      table: s.table,
      rootPage: s.root_page,
      rowCount: s.row_count,
      columns: s.columns,
    };
  }

  async indexes(): Promise<IndexInfo[]> {
    const data = await this.request('GET', '/api/v1/indexes');
    if (!data.ok) {
      throw new S3LiteError(data.error ?? 'failed to list indexes');
    }
    return data.indexes ?? [];
  }

  async stats(): Promise<Stats> {
    const data = await this.request('GET', '/api/v1/stats');
    if (!data.ok) {
      throw new S3LiteError(data.error ?? 'failed to get stats');
    }
    return data.stats as Stats;
  }

  async flush(): Promise<void> {
    const data = await this.request('POST', '/api/v1/commit');
    if (!data.ok) {
      throw new S3LiteError(data.error ?? 'flush failed');
    }
  }

  async compact(): Promise<string> {
    const data = await this.request('POST', '/api/v1/compact');
    if (!data.ok) {
      throw new S3LiteError(data.error ?? 'compact failed');
    }
    return data.message ?? '';
  }

  async gc(options?: GCOptions): Promise<string> {
    const data = await this.request('POST', '/api/v1/gc', {
      keep: options?.keep ?? 5,
    });
    if (!data.ok) {
      throw new S3LiteError(data.error ?? 'gc failed');
    }
    return data.message ?? '';
  }

  async health(): Promise<boolean> {
    try {
      const data = await this.request('GET', '/api/v1/health');
      return data.ok;
    } catch {
      return false;
    }
  }

  async close(): Promise<void> {
    // No persistent connections to clean up with fetch
  }
}
