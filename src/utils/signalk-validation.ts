/**
 * Boundary validation for SignalK contexts and paths.
 *
 * The History API and several routes accept a `context` (e.g. vessels.<id>) and
 * a `path` (e.g. navigation.position) from untrusted HTTP input and splice them
 * into DuckDB `read_parquet('...')` glob literals and into filesystem directory
 * names. The SQL builders escape quotes (see sql-escape.ts) as the guaranteed
 * defence, but these allow-list validators reject the values up-front so glob
 * wildcards, path separators, and traversal sequences never reach a glob or the
 * filesystem at all. Validation is deliberately strict: anything that is not a
 * well-formed SignalK identifier is rejected rather than sanitised.
 */
import { Context, Path } from '@signalk/server-api';
import * as fs from 'fs';
import * as path from 'path';

// A SignalK context is `vessels.<id>` / `aircraft.<id>` / etc. The id segment is
// typically a urn (urn:mrn:imo:mmsi:..., urn:mrn:signalk:uuid:...) so colons,
// dots, hyphens and underscores are allowed; quotes, slashes, glob metacharacters
// (* ? [ ]) and whitespace are not.
// The id segment must start and end with an alphanumeric, so dot-only ids like
// "vessels.." or "vessels...etc" are rejected.
// Input:  "vessels.urn:mrn:imo:mmsi:230099999"  -> returned unchanged
// Input:  "vessels.x')+(SELECT..."              -> throws
// Input:  "vessels.."                            -> throws
const CONTEXT_PATTERN =
  /^[A-Za-z][A-Za-z0-9]*\.[A-Za-z0-9](?:[A-Za-z0-9:_.-]*[A-Za-z0-9])?$/;

// A SignalK path is dot-delimited alphanumeric segments, e.g.
// "environment.wind.speedApparent" or "electrical.batteries.1.voltage".
// Reject anything containing a quote, slash, glob metacharacter, or '..'.
// Input:  "navigation.position" -> returned unchanged
// Input:  "../../etc"           -> throws
const PATH_PATTERN = /^[A-Za-z][A-Za-z0-9]*(\.[A-Za-z0-9_-]+)*$/;

/**
 * Validate a SignalK context string from untrusted input. Returns it unchanged
 * when valid; throws otherwise. Callers should already have resolved the
 * `self`/`vessels.self` aliases to a concrete context before calling this.
 */
export function validateContext(context: string): Context {
  if (
    typeof context !== 'string' ||
    context.includes('..') ||
    !CONTEXT_PATTERN.test(context)
  ) {
    throw new Error(`Invalid SignalK context: ${JSON.stringify(context)}`);
  }
  return context as Context;
}

/**
 * Validate a single SignalK path segment from untrusted input. Returns it
 * unchanged when valid; throws otherwise.
 */
export function validateSignalKPath(p: string): Path {
  if (typeof p !== 'string' || p.length > 256 || !PATH_PATTERN.test(p)) {
    throw new Error(`Invalid SignalK path: ${JSON.stringify(p)}`);
  }
  return p as Path;
}

/**
 * Assert that a candidate filesystem path resolves to a location inside
 * `dataDir`. Returns the resolved absolute path when safe; throws otherwise.
 * Used to stop `../` traversal in request-supplied keys/paths from reaching
 * files outside the plugin's data directory.
 */
export function assertWithinDataDir(
  dataDir: string,
  candidate: string
): string {
  const root = path.resolve(dataDir);
  const resolved = path.resolve(candidate);
  if (resolved !== root && !resolved.startsWith(root + path.sep)) {
    throw new Error(
      `Path escapes the data directory: ${JSON.stringify(candidate)}`
    );
  }
  // path.resolve is lexical only: a symlink inside dataDir could still point
  // outside it. When both paths exist, compare their real (symlink-resolved)
  // locations so a symlinked entry can't escape. A non-existent candidate has
  // no symlink to follow, so the lexical check above already covers it.
  let realRoot: string;
  let realResolved: string;
  try {
    realRoot = fs.realpathSync(root);
    realResolved = fs.realpathSync(resolved);
  } catch (err) {
    if ((err as NodeJS.ErrnoException).code === 'ENOENT') {
      return resolved;
    }
    throw err;
  }
  if (
    realResolved !== realRoot &&
    !realResolved.startsWith(realRoot + path.sep)
  ) {
    throw new Error(
      `Path escapes the data directory via symlink: ${JSON.stringify(candidate)}`
    );
  }
  return resolved;
}
