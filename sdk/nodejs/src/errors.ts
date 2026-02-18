export class S3LiteError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'S3LiteError';
  }
}

export class S3LiteQueryError extends S3LiteError {
  public readonly sql: string;

  constructor(message: string, sql: string) {
    super(message);
    this.name = 'S3LiteQueryError';
    this.sql = sql;
  }
}

export class S3LiteAuthError extends S3LiteError {
  constructor(message: string = 'unauthorized') {
    super(message);
    this.name = 'S3LiteAuthError';
  }
}

export class S3LiteConnectionError extends S3LiteError {
  constructor(message: string) {
    super(message);
    this.name = 'S3LiteConnectionError';
  }
}
