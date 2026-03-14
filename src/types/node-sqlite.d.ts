/**
 * Minimal type declarations for node:sqlite (Node 22.5+).
 * Only the subset used by sqlite-buffer.ts is declared here.
 */
declare module 'node:sqlite' {
  interface StatementResultingChanges {
    changes: number;
    lastInsertRowid: number | bigint;
  }

  class StatementSync {
    run(...params: unknown[]): StatementResultingChanges;
    get(...params: unknown[]): unknown;
    all(...params: unknown[]): unknown[];
  }

  class DatabaseSync {
    constructor(path: string, options?: { readOnly?: boolean });
    exec(sql: string): void;
    prepare(sql: string): StatementSync;
    close(): void;
  }
}
