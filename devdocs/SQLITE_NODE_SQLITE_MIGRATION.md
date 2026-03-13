# Migration: better-sqlite3 to node:sqlite

## Date
2026-03-13

## Problem

`better-sqlite3` requires a postinstall script (`prebuild-install`) to download a platform-specific native binary. SignalK 2 blocks postinstall scripts during plugin installation via `npm install --ignore-scripts`, so the binary is never downloaded. The plugin falls back to in-memory LRU buffers silently — no crash, but no crash-safe persistence either.

This was discovered after publishing 0.7.7-beta.1 (now unpublished).

## Solution

Replace `better-sqlite3` with `node:sqlite`, which ships built into Node.js 22.5+. Zero install dependencies. Synchronous API like `better-sqlite3`. Eliminates the native binary problem entirely.

## Node.js Requirement

`node:sqlite` is available from Node 22.5.0 onward. `engines.node` in `package.json` was bumped from `>=18.0.0` to `>=22.5.0`. Both localhost and mizzen run Node 22.

The module is still marked `ExperimentalWarning` by Node.js. It prints a one-time warning on first use. This is cosmetic and does not affect functionality.

## API Differences

| Feature | `better-sqlite3` | `node:sqlite` |
|---------|-----------------|---------------|
| Import | `require('better-sqlite3')` | `require('node:sqlite').DatabaseSync` |
| Constructor | `new Database(path)` | `new DatabaseSync(path)` |
| Pragmas | `db.pragma('journal_mode = WAL')` | `db.exec('PRAGMA journal_mode = WAL')` |
| `pragma('table_info(x)')` | Returns rows directly | `db.prepare('PRAGMA table_info(x)').all()` |
| `pragma('wal_checkpoint(TRUNCATE)')` | Direct call | `db.exec('PRAGMA wal_checkpoint(TRUNCATE)')` |
| `.transaction(fn)` | Returns wrapped callable | **Not available** — use `exec('BEGIN')`/`exec('COMMIT')`/`exec('ROLLBACK')` |
| `.open` property | `db.open` returns boolean | **Not available** — `.open()` is a method to reopen. Track state manually via `this._open` flag |
| `.prepare()/.run()/.get()/.all()` | Same | Same |
| `.exec()` | Same | Same |
| `.close()` | Same | Same |
| `.run()` return | `{ changes, lastInsertRowid }` | `{ changes, lastInsertRowid }` — same |
| Results | Plain objects | Null-prototype objects (cast with `as` works fine in TS) |
| Read-only | `new Database(path, { readonly: true })` | `new DatabaseSync(path, { readOnly: true })` (camelCase) |

## Files Changed

### `src/utils/sqlite-buffer.ts`

The main refactor. Every change maps to an API difference above:

- **Import**: `import Database = require('better-sqlite3')` replaced with `import { DatabaseSync, StatementSync } from 'node:sqlite'`
- **Types**: `Database.Database` replaced with `DatabaseSync`, `Database.Statement` replaced with `StatementSync`
- **Constructor**: `new Database(this.dbPath)` replaced with `new DatabaseSync(this.dbPath)`
- **Open tracking**: Added `private _open = true` flag, set to `false` in `close()`. All `this.db.open` checks replaced with `this._open`
- **Pragmas** (5 calls in constructor): `this.db.pragma('x = y')` replaced with `this.db.exec('PRAGMA x = y')`
- **`pragma('table_info(x)')` calls** (3 locations): replaced with `this.db.prepare('PRAGMA table_info(x)').all()`
- **Checkpoint**: `this.db.pragma('wal_checkpoint(TRUNCATE)')` replaced with `this.db.exec('PRAGMA wal_checkpoint(TRUNCATE)')`
- **Transactions** (2 locations — legacy migration and batch insert): `this.db.transaction(() => { ... })()` replaced with manual `BEGIN`/`COMMIT`/`ROLLBACK`:

```typescript
// Before:
const txn = this.db.transaction(() => { ... });
txn();

// After:
this.db.exec('BEGIN');
try {
  ...
  this.db.exec('COMMIT');
} catch (e) {
  this.db.exec('ROLLBACK');
  throw e;
}
```

### `src/types/node-sqlite.d.ts`

New minimal type declaration file for `node:sqlite`. Only declares the subset of the API used by `sqlite-buffer.ts` (`DatabaseSync`, `StatementSync`, `StatementResultingChanges`). This avoids needing to bump `@types/node` from `^20` to `^22`, which would be a broader change.

### `tests/test-sqlite-buffer.js`

- `require('better-sqlite3')` replaced with `require('node:sqlite').DatabaseSync`
- `{ readonly: true }` replaced with `{ readOnly: true }` (7 occurrences)
- `db.pragma('table_info(...)')` replaced with `db.prepare('PRAGMA table_info(...)').all()`
- `db.pragma('journal_mode = WAL')` replaced with `db.exec('PRAGMA journal_mode = WAL')`

### `tests/test-data-pipeline.js`

- `require('better-sqlite3')` replaced with `require('node:sqlite').DatabaseSync`
- `{ readonly: true }` replaced with `{ readOnly: true }`
- Error message updated from "better-sqlite3 not available" to "node:sqlite not available"

### `package.json`

- Removed `"better-sqlite3": "^11.8.1"` from `dependencies`
- Removed `"@types/better-sqlite3": "^7.6.12"` from `devDependencies`
- Updated `engines.node` from `>=18.0.0` to `>=22.5.0`

### `package-lock.json`

Regenerated via `npm install` — `better-sqlite3` and its transitive dependencies (`prebuild-install`, `node-addon-api`, etc.) are no longer present.

## Graceful Fallback

The existing architecture already handles `state.sqliteBuffer` being `null` — the plugin falls back to in-memory LRU buffers. On Node < 22.5 where `node:sqlite` is not available, the buffer initialization will fail and this fallback will activate. No additional try-catch wrapper was needed at the import site because the buffer construction is already wrapped in a try-catch in the plugin startup code.

## Verification Checklist

1. `npm run build` — TypeScript compiles cleanly
2. `npm run ci` — lint + format pass
3. `better-sqlite3` removed from `package-lock.json`
4. `node tests/test-sqlite-buffer.js` — unit tests pass
5. Test on localhost SignalK — buffer status shows "enabled", records accumulate
6. Test on mizzen after deploy — same check
