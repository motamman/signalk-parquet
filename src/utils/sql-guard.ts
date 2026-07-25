/**
 * Validation for user- and model-supplied SQL executed on the shared DuckDB
 * pool (the raw /api/query endpoint and Claude-generated analysis queries).
 *
 * DuckDB must never open the live SQLite buffer (see buffer-staging.ts): its
 * bundled SQLite cannot see node:sqlite's in-process POSIX locks, so it treats
 * the database as unused, runs WAL recovery, and truncates the -shm file under
 * the writer's active mmap, killing the server with SIGBUS.
 *
 * Two rules, because the dangerous constructs arrive in two shapes:
 *
 * 1. Every statement must be a read-only query. ATTACH, INSTALL and LOAD reach
 *    the SQLite code path directly; COPY and EXPORT DATABASE write files; SET
 *    and PRAGMA can undo the engine lockdown in duckdb-pool. A whitelist
 *    rejects all of them, plus the next spelling of the same capability, and
 *    plus anything chained after a benign leading SELECT. Blocklists were tried
 *    first and lost to CALL sqlite_attach(...), FORCE INSTALL and EXPORT
 *    DATABASE.
 *
 * 2. Some table functions open databases or files from inside an otherwise
 *    valid SELECT, so they are rejected by name: the sqlite_* family, query()
 *    and query_table() (which execute a nested SQL string that rule 1 cannot
 *    see), and the raw file readers read_text/read_blob/glob.
 *
 * The name check deliberately runs over SQL whose quoted identifiers are still
 * visible. DuckDB resolves "sqlite_scan"('db','t') exactly like the bare form,
 * so masking identifiers before this check let the quoted spelling through.
 *
 * Scope note: this is not a filesystem sandbox. read_parquet and read_csv can
 * still read any path the process can reach — that is inherent to an endpoint
 * whose purpose is running SQL over local files, and it predates this guard.
 * What is guaranteed here is only that DuckDB never opens the write buffer as
 * a database and never writes through the query engine.
 */

/**
 * Statement kinds that only read. DuckDB's FROM-first form (`FROM t SELECT *`),
 * DESCRIBE/SUMMARIZE/EXPLAIN and a parenthesised leading subquery are all
 * idiomatic read-only SQL that the raw query console is expected to accept.
 */
const READ_ONLY_STATEMENT_START =
  /^(SELECT|WITH|FROM|DESCRIBE|SUMMARIZE|EXPLAIN|VALUES|PIVOT|UNPIVOT|TABLE|SHOW|\()/i;

/**
 * Statement kinds worth naming explicitly in the rejection message, because
 * they are the ones a caller might reasonably expect to work.
 */
const FILE_OR_STATE_STATEMENTS = [
  'ATTACH',
  'DETACH',
  'INSTALL',
  'FORCE',
  'LOAD',
  'COPY',
  'EXPORT',
  'IMPORT',
  'SET',
  'RESET',
  'PRAGMA',
  'CALL',
];

/**
 * Table functions that open a database or a raw file, or execute a nested SQL
 * string the statement whitelist cannot inspect. `sqlite_\w+` covers scan,
 * attach and query without needing to track additions to the extension.
 */
const FORBIDDEN_FUNCTION_PATTERN =
  /\b(sqlite_\w+|query|query_table|read_text|read_blob|glob|duckdb_secrets)\s*\(/i;

/**
 * Statement keywords that can never appear in a read-only query. The statement
 * whitelist rejects them at statement start; scanning anywhere as well catches
 * the WITH-prefixed form, e.g.
 * `WITH x AS (SELECT 3 AS a) INSERT INTO t SELECT a FROM x`, which DuckDB
 * parses and runs and which starts with an allowed keyword.
 */
const DATA_MODIFYING_KEYWORDS = [
  'DROP',
  'DELETE',
  'UPDATE',
  'INSERT',
  'CREATE',
  'ALTER',
  'TRUNCATE',
];

/** Blank every character of a match except newlines, preserving offsets. */
function blank(match: string): string {
  return match.replace(/[^\r\n]/g, ' ');
}

const STRINGS_AND_COMMENTS =
  /'(?:[^']|'')*'?|\$(\w*)\$[\s\S]*?(?:\$\1\$|$)|--[^\r\n]*|\/\*[\s\S]*?(?:\*\/|$)/g;
const QUOTED_IDENTIFIER = /"(?:[^"]|"")*"?/g;

/**
 * Blank SQL string literals ('...' with '' escapes), dollar-quoted strings
 * ($$...$$, $tag$...$tag$), quoted identifiers ("..."), and comments (-- line,
 * block) so statement splitting and keyword scanning never see content DuckDB
 * treats as data. Replacement preserves length and newlines so offsets and
 * line numbers still line up.
 *
 * Input:  SELECT 'a;b' AS x -- attach note
 * Output: SELECT       AS x
 *
 * An unterminated literal or comment is blanked to the end of the input.
 * DuckDB rejects every such form at parse time ("unterminated quoted string",
 * "unterminated /* comment", and the same for quoted identifiers and
 * dollar-quoted strings), so nothing executable can hide behind one.
 */
export function maskSqlLiteralsAndComments(sql: string): string {
  return sql
    .replace(STRINGS_AND_COMMENTS, blank)
    .replace(QUOTED_IDENTIFIER, blank);
}

/**
 * Mask for the function-name check: blanks strings and comments but keeps
 * quoted identifiers, then removes the quote characters so that
 * `"sqlite_scan"(` collapses to `sqlite_scan(` and matches.
 *
 * A quoted alias that literally contains a forbidden name followed by `(`
 * becomes a false positive. That trade is deliberate — the alternative let the
 * quoted spelling reach the live buffer.
 */
function maskForFunctionScan(sql: string): string {
  return sql.replace(STRINGS_AND_COMMENTS, blank).replace(/"/g, '');
}

/**
 * Find the first whole-word occurrence of any keyword outside literals and
 * comments. Keywords are module-private constants consisting of word
 * characters, so no escaping is needed.
 */
function findSqlKeyword(
  maskedSql: string,
  keywords: readonly string[]
): string | null {
  for (const keyword of keywords) {
    if (new RegExp(`\\b${keyword}\\b`, 'i').test(maskedSql)) {
      return keyword;
    }
  }
  return null;
}

/**
 * Split masked SQL into statements on semicolons. Semicolons inside literals
 * and comments are already blanked, so a plain split is safe.
 */
function splitStatements(maskedSql: string): string[] {
  return maskedSql
    .split(';')
    .map(statement => statement.trim())
    .filter(statement => statement.length > 0);
}

/**
 * Check user- or model-supplied SQL for anything that is not a plain read-only
 * query. Returns a human-readable rejection reason (surfaced to the API caller
 * and fed back to the model so it can retry), or null when the SQL is safe to
 * execute.
 */
export function findUnsafeSqlReason(sql: string): string | null {
  const masked = maskSqlLiteralsAndComments(sql);
  const statements = splitStatements(masked);

  if (statements.length === 0) {
    return 'Query is empty.';
  }

  for (const statement of statements) {
    if (!READ_ONLY_STATEMENT_START.test(statement)) {
      const firstWord = statement.split(/\s+/)[0].toUpperCase();
      if (FILE_OR_STATE_STATEMENTS.includes(firstWord)) {
        return (
          `'${firstWord}' is not allowed: it can open a database file, write ` +
          `to the filesystem, or change engine settings. Only read-only ` +
          `queries are permitted.`
        );
      }
      return (
        `Only read-only queries are allowed (SELECT, WITH, FROM, DESCRIBE, ` +
        `SUMMARIZE, EXPLAIN, VALUES, PIVOT, TABLE, SHOW); got '${firstWord}'.`
      );
    }
  }

  const forbiddenFunction = FORBIDDEN_FUNCTION_PATTERN.exec(
    maskForFunctionScan(sql)
  );
  if (forbiddenFunction) {
    return (
      `Table function '${forbiddenFunction[1]}' is not allowed: it opens a ` +
      `database or file directly, or runs nested SQL that cannot be checked. ` +
      `Use read_parquet for data files.`
    );
  }

  const modifying = findSqlKeyword(masked, DATA_MODIFYING_KEYWORDS);
  if (modifying) {
    return `Data-modifying keyword '${modifying}' is not allowed.`;
  }

  return null;
}
