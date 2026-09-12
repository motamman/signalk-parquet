/**
 * Directory-anchored glob. File discovery under the data directory (or a
 * user-supplied import directory) goes through here instead of calling glob()
 * on a pattern built with path.join().
 */
import { glob } from 'glob';

export interface GlobInOptions {
  /** Case-insensitive matching, e.g. `*.gpx` also matching `TRACK.GPX`. */
  nocase?: boolean;
  /** Patterns, relative to the directory, whose matches are dropped. */
  ignore?: string[];
}

/**
 * Matches `pattern` against the tree under `dir` and returns absolute paths
 * with native separators. `pattern` uses forward slashes and is relative to
 * `dir`; the directory itself never becomes part of a pattern, because glob()
 * reads a backslash as an escape character (so a Windows path built with
 * path.join() matches nothing) and glob syntax in a directory name, such as
 * `[1]` or `+(x)`, would be matched as a pattern instead of literally.
 *
 * Example: globIn('C:\\data', 'tier=raw/context=*')
 *   -> ['C:\\data\\tier=raw\\context=vessels__self']
 *
 * A directory that does not exist yields no matches.
 */
export function globIn(
  dir: string,
  pattern: string,
  options: GlobInOptions = {}
): Promise<string[]> {
  return glob(pattern, { ...options, cwd: dir, absolute: true });
}
