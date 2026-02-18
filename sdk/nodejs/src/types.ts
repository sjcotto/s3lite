export interface S3LiteClientOptions {
  url: string;
  apiKey?: string;
  fetch?: typeof globalThis.fetch;
}

export interface ExecuteResult {
  columns: string[];
  rows: any[][];
  rowCount: number;
  timeMs: number;
  message: string;
}

export interface TableSchema {
  table: string;
  rootPage: number;
  rowCount: number;
  columns: ColumnInfo[];
}

export interface ColumnInfo {
  name: string;
  type: string;
  pk?: boolean;
  nullable?: boolean;
}

export interface IndexInfo {
  name: string;
  table: string;
  columns: string[];
  unique: boolean;
}

export interface Stats {
  pages_read: number;
  pages_written: number;
  prefetched_pages: number;
  cache_hits: number;
  cache_misses: number;
  cache_hit_rate: number;
  s3_gets: number;
  s3_puts: number;
  bloom_skips: number;
  manifest_version: number;
  num_pages: number;
  num_chunks: number;
  num_tables: number;
  num_indexes: number;
}

export interface GCOptions {
  keep?: number;
}

export interface ApiResponse {
  ok: boolean;
  columns?: string[];
  rows?: any[][];
  row_count?: number;
  time_ms?: number;
  message?: string;
  error?: string;
  session_id?: string;
  tables?: string[];
  schema?: any;
  indexes?: any;
  stats?: any;
}

export interface TransactionExecutor {
  execute(sql: string): Promise<ExecuteResult>;
  query<T extends Record<string, any> = Record<string, any>>(sql: string): Promise<T[]>;
}
