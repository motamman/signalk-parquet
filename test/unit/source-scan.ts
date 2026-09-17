/**
 * Shared machinery for static source-scan invariants (tests that read src/
 * and fail when a forbidden pattern is written back into it).
 *
 * Comments are blanked first, using the TypeScript scanner rather than a
 * hand-rolled lexer (a regex literal containing a quote desynchronises a naive
 * one), so a hazard can still be documented in prose while SQL and globs
 * inside string literals stay visible to the scan.
 */
import * as fs from 'fs';
import * as path from 'path';
import * as ts from 'typescript';

export const SRC_DIR = path.join(__dirname, '..', '..', 'src');

/**
 * Blank every comment in TypeScript source, preserving offsets and newlines
 * so match positions still map to line numbers. String and template literals
 * are left intact because the text these tests look for lives inside them.
 *
 * Input:  const sql = 'ATTACH x'; // ATTACH note
 * Output: const sql = 'ATTACH x';
 */
export function blankComments(source: string): string {
  const chars = source.split('');
  const sourceFile = ts.createSourceFile(
    'scan.ts',
    source,
    ts.ScriptTarget.Latest,
    true
  );

  const blankRange = (pos: number, end: number): void => {
    for (let i = pos; i < end && i < chars.length; i += 1) {
      if (chars[i] !== '\n' && chars[i] !== '\r') {
        chars[i] = ' ';
      }
    }
  };

  const visit = (node: ts.Node): void => {
    for (const range of ts.getLeadingCommentRanges(
      source,
      node.getFullStart()
    ) ?? []) {
      blankRange(range.pos, range.end);
    }
    for (const range of ts.getTrailingCommentRanges(source, node.getEnd()) ??
      []) {
      blankRange(range.pos, range.end);
    }
    node.getChildren(sourceFile).forEach(visit);
  };
  visit(sourceFile);

  return chars.join('');
}

/** Relative path with forward slashes, so exemption lists read the same on Windows. */
export function relativeSourcePath(file: string): string {
  return path.relative(SRC_DIR, file).split(path.sep).join('/');
}

/** Every .ts file under src/, minus the named exemptions (relative, forward-slash paths). */
export function listSourceFiles(exempt: string[] = []): string[] {
  return fs
    .readdirSync(SRC_DIR, { recursive: true, encoding: 'utf8' })
    .filter(entry => entry.endsWith('.ts'))
    .map(entry => path.join(SRC_DIR, entry))
    .filter(file => !exempt.includes(relativeSourcePath(file)));
}

export function lineNumberAt(text: string, index: number): number {
  return text.slice(0, index).split('\n').length;
}

/**
 * Every `file:line` at which `pattern` matches the comment-blanked source of
 * `files`. All occurrences are reported, not just the first per file, so a
 * failing run doubles as the migration worklist.
 */
export function findAll(
  files: string[],
  pattern: RegExp,
  /** A match whose whole source line matches this is not a violation. */
  except?: RegExp
): string[] {
  const global = new RegExp(
    pattern.source,
    pattern.flags.includes('g') ? pattern.flags : pattern.flags + 'g'
  );
  const hits: string[] = [];
  for (const file of files) {
    const scanned = blankComments(fs.readFileSync(file, 'utf8'));
    global.lastIndex = 0;
    let match = global.exec(scanned);
    while (match) {
      const lineStart = scanned.lastIndexOf('\n', match.index) + 1;
      const lineEnd = scanned.indexOf('\n', match.index);
      const line = scanned.slice(
        lineStart,
        lineEnd === -1 ? undefined : lineEnd
      );
      if (!except || !except.test(line)) {
        hits.push(
          `${relativeSourcePath(file)}:${lineNumberAt(scanned, match.index)}`
        );
      }
      if (match[0].length === 0) global.lastIndex += 1;
      match = global.exec(scanned);
    }
  }
  return hits;
}
