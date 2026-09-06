/**
 * Validation for user- and model-supplied SQL: the raw `/api/query` endpoint
 * and the SQL the analysis model writes for itself.
 *
 * Both paths execute on the hardened sandbox instance (see
 * `DuckDBPool.getSandboxConnection`), which confines *where* the engine may
 * touch the filesystem — `allowed_directories` plus `enable_external_access
 * = false` — and blocks extension loading, so external databases, the network
 * and the S3 secret are all out of reach.
 *
 * What the sandbox does not do is make the SQL read-only. Inside the allowed
 * directory the engine still writes: `COPY (...) TO 'navigation_position.
 * parquet'` overwrites a recorded parquet file in place, and `EXPORT DATABASE`
 * drops files alongside it. The plugin's data directory is exactly that
 * allowed directory, so untrusted SQL can destroy recorded data without ever
 * leaving the sandbox. The analysis path is not gated behind `enableRawSql`,
 * which makes this reachable on a default install.
 *
 * This guard supplies the missing half: one statement, and that statement must
 * be a read-only query. Three rules, because the dangerous constructs arrive
 * in three shapes.
 *
 * 1. Statement whitelist. ATTACH, INSTALL and LOAD open databases or load
 *    extensions; COPY and EXPORT DATABASE write files; SET and PRAGMA change
 *    engine settings. A whitelist rejects all of them, plus the next spelling
 *    of the same capability, plus anything chained after a benign leading
 *    SELECT. A blocklist was tried first and lost to `CALL sqlite_attach(...)`,
 *    `FORCE INSTALL` and `EXPORT DATABASE`.
 *
 * 2. The same keywords rejected anywhere in a statement, not only at its
 *    start, because a statement can carry another one inside it. `EXPLAIN
 *    ANALYZE COPY (...) TO 'navigation_position.parquet'` starts with a
 *    whitelisted keyword and overwrites the file, and `EXPLAIN ANALYZE SET
 *    memory_limit='4GB'` raised the sandbox's own 512MB cap to 3.7GiB — a DoS
 *    on a Pi. EXPLAIN's inner statement is whitelisted on its own too, so the
 *    two rules cover the bare form twice over; the anywhere-scan is what
 *    covers the spellings the prefix strip does not recognise, `EXPLAIN
 *    (ANALYZE) COPY ...` among them.
 *
 * 3. Function-name rejection, for the table functions that reach outside an
 *    otherwise valid SELECT: `query()` and `query_table()` execute a nested SQL
 *    string rule 1 cannot see into, `read_text`/`read_blob`/`glob` read raw
 *    bytes and directory listings, and the `sqlite_*` family opens a SQLite
 *    database directly.
 *
 * All three read masked SQL, and the mask is a single lexer pass by design
 * rather than by accident — see `SQL_TOKEN`.
 *
 * The `sqlite_*` rejection is defence in depth rather than the load-bearing
 * control: the sandbox already refuses to load `sqlite_scanner`. It matters
 * because DuckDB must never open the live SQLite buffer under any
 * configuration — its bundled SQLite cannot see node:sqlite's in-process POSIX
 * locks, so it treats the database as unused, runs WAL recovery, and truncates
 * the -shm file under the writer's active mmap, killing the server with SIGBUS
 * (see buffer-staging.ts). Buffer rows reach DuckDB only via staged TEMP
 * tables.
 *
 * The name check deliberately runs over SQL whose quoted identifiers are still
 * visible. DuckDB resolves `"sqlite_scan"('db','t')` exactly like the bare
 * form, so masking identifiers away before this check lets the quoted spelling
 * through.
 *
 * Known false positives, accepted as the cheaper side of the trade: an
 * unquoted column or alias named after a rejected keyword — `SELECT drop FROM
 * t`, `SELECT set FROM t` — is rejected, while the quoted spelling `SELECT
 * "drop" FROM t` works, because quoted identifiers are masked before the
 * keyword scans. `LOAD` is kept out of the anywhere-scan for exactly this
 * reason, since `AS load` is the natural alias for `propulsion.main.load`; it
 * stays rejected at statement start, and the sandbox refuses extension loading
 * on its own account anyway.
 *
 * Scope note: this is not a filesystem sandbox and does not try to be one.
 * `read_parquet` and `read_csv` can still read any path inside the allowed
 * directory — that is the point of an endpoint whose purpose is running SQL
 * over the recorded data.
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
 * The subset of the above rejected anywhere in a statement, for statements
 * that wrap another statement.
 *
 * `LOAD` and `FORCE` are deliberately absent: both are common enough as column
 * names or aliases that scanning for them anywhere would reject real queries,
 * and neither loses much by being caught only at statement start — `FORCE` is
 * only ever a prefix of `INSTALL`, which is scanned, and the sandbox refuses
 * extension loading regardless of what this guard says.
 */
const NESTED_FILE_OR_STATE_KEYWORDS = [
  'ATTACH',
  'DETACH',
  'INSTALL',
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

/**
 * `EXPLAIN` and `EXPLAIN ANALYZE` prefix another statement. ANALYZE runs it,
 * so what follows has to satisfy the whitelist on its own.
 */
const EXPLAIN_PREFIX = /^EXPLAIN(?:\s+ANALYZE)?\s+/i;

/**
 * Every construct whose body DuckDB reads as data rather than as SQL, lexed in
 * one alternation: comments, string literals in all three spellings, and
 * quoted identifiers.
 *
 * In `SELECT 'a;b' AS "x--y" -- attach note` it matches three tokens:
 * `'a;b'`, `"x--y"` and `-- attach note`.
 *
 * One pass rather than several is the whole point. Separate passes cannot lex
 * mutually exclusive tokens: whichever runs first consumes the other's opening
 * token from inside a body where it is only text. `SELECT 1 AS "a--b"; COPY
 * (...) TO 'navigation_position.parquet'` is the case that mattered — a
 * comment pass reads `--b"; COPY ...` as a line comment and blanks the
 * semicolon along with the entire second statement, leaving the guard to
 * validate one harmless SELECT while DuckDB runs both.
 *
 * An unterminated literal or comment is blanked to the end of the input.
 * DuckDB rejects every such form at parse time ("unterminated quoted string",
 * "unterminated block comment", and the same for quoted identifiers and
 * dollar-quoted strings), so nothing executable can hide behind one.
 */
const SQL_TOKEN = new RegExp(
  [
    // Line comment: -- to end of line.
    '--[^\\r\\n]*',
    // Block comment: /* to the first */, or to end of input.
    '/\\*[\\s\\S]*?(?:\\*/|$)',
    // Escape string, where a backslash escapes the next character:
    // E'a\'b' is one literal ending at the third quote, not two. The
    // lookbehind keeps DATE'2024-01-01' out, where the E ends a keyword.
    "(?<![\\w\"])[eE]'(?:\\\\[\\s\\S]|''|[^'\\\\])*'?",
    // String literal, where '' is the only escape: 'it''s' is one literal.
    "'(?:''|[^'])*'?",
    // Dollar-quoted string: $$a$$ or $tag$a$tag$, closed by its own tag.
    '\\$(?<dollarTag>\\w*)\\$[\\s\\S]*?(?:\\$\\k<dollarTag>\\$|$)',
    // Quoted identifier, where "" is the only escape.
    '"(?:""|[^"])*"?',
  ].join('|'),
  'g'
);

/** How the mask renders quoted identifiers. */
type IdentifierHandling =
  /** Blank them, so statement splitting and keyword scans cannot see them. */
  | 'blank'
  /** Blank only the quotes, so `"sqlite_scan"(` still reads as a call. */
  | 'unquote';

/** Blank every character of a match except newlines, preserving offsets. */
function blank(match: string): string {
  return match.replace(/[^\r\n]/g, ' ');
}

/**
 * Blank literals and comments, and render quoted identifiers as asked, so the
 * checks below never see content DuckDB treats as data. Replacement preserves
 * length and newlines, so offsets and line numbers still line up with the
 * original SQL.
 */
function maskSql(sql: string, identifiers: IdentifierHandling): string {
  return sql.replace(SQL_TOKEN, token => {
    if (identifiers === 'unquote' && token.startsWith('"')) {
      return token.replace(/"/g, ' ');
    }
    return blank(token);
  });
}

/**
 * Mask used for statement splitting and keyword scanning: literals, comments
 * and quoted identifiers all become blanks.
 *
 * Input:  SELECT 'a;b' AS x -- attach note
 * Output: SELECT       AS x
 */
export function maskSqlLiteralsAndComments(sql: string): string {
  return maskSql(sql, 'blank');
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
 * Strip leading EXPLAIN / EXPLAIN ANALYZE so the statement being explained is
 * validated in its own right.
 *
 * Input:  EXPLAIN ANALYZE COPY (SELECT 1) TO 'recorded.parquet'
 * Output: COPY (SELECT 1) TO 'recorded.parquet'
 */
function stripExplainPrefix(statement: string): string {
  let remaining = statement;
  while (EXPLAIN_PREFIX.test(remaining)) {
    remaining = remaining.replace(EXPLAIN_PREFIX, '');
  }
  return remaining;
}

/** Rejection message for a keyword that opens files or changes engine state. */
function fileOrStateRejection(keyword: string): string {
  return (
    `'${keyword}' is not allowed: it can open a database file, write ` +
    `to the filesystem, or change engine settings. Only read-only ` +
    `queries are permitted.`
  );
}

/** Check one masked statement. Returns a rejection reason, or null if safe. */
function findUnsafeStatementReason(statement: string): string | null {
  const explained = stripExplainPrefix(statement);

  if (!READ_ONLY_STATEMENT_START.test(explained)) {
    const firstWord = explained.split(/\s+/)[0].toUpperCase();
    if (FILE_OR_STATE_STATEMENTS.includes(firstWord)) {
      return fileOrStateRejection(firstWord);
    }
    return (
      `Only read-only queries are allowed (SELECT, WITH, FROM, DESCRIBE, ` +
      `SUMMARIZE, EXPLAIN, VALUES, PIVOT, TABLE, SHOW); got '${firstWord}'.`
    );
  }

  const nested = findSqlKeyword(explained, NESTED_FILE_OR_STATE_KEYWORDS);
  if (nested) {
    return fileOrStateRejection(nested);
  }

  const modifying = findSqlKeyword(explained, DATA_MODIFYING_KEYWORDS);
  if (modifying) {
    return `Data-modifying keyword '${modifying}' is not allowed.`;
  }

  return null;
}

/**
 * Check user- or model-supplied SQL for anything that is not a single plain
 * read-only query. Returns a human-readable rejection reason (surfaced to the
 * API caller and fed back to the model so it can retry), or null when the SQL
 * is safe to execute.
 */
export function findUnsafeSqlReason(sql: string): string | null {
  const masked = maskSqlLiteralsAndComments(sql);
  const statements = splitStatements(masked);

  if (statements.length === 0) {
    return 'Query is empty.';
  }

  for (const statement of statements) {
    const reason = findUnsafeStatementReason(statement);
    if (reason) {
      return reason;
    }
  }

  // Reached only when every statement is read-only on its own. Both callers
  // keep the result of the last statement and discard the rest, so a batch was
  // never useful here, and holding the guard to one statement means a future
  // masking slip has to survive this check as well as the ones above.
  if (statements.length > 1) {
    return (
      `Only one statement is allowed; got ${statements.length}. Run the ` +
      `queries one at a time.`
    );
  }

  const forbiddenFunction = FORBIDDEN_FUNCTION_PATTERN.exec(
    maskSql(sql, 'unquote')
  );
  if (forbiddenFunction) {
    return (
      `Table function '${forbiddenFunction[1]}' is not allowed: it opens a ` +
      `database or file directly, or runs nested SQL that cannot be checked. ` +
      `Use read_parquet for data files.`
    );
  }

  return null;
}
