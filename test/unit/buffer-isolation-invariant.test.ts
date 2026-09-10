/**
 * Static invariant: no source code may let DuckDB open the live SQLite
 * buffer. DuckDB's bundled SQLite cannot see node:sqlite's in-process POSIX
 * locks; opening buffer.db truncates its -shm file under the writer's active
 * mmap and kills the server with SIGBUS (see src/utils/buffer-staging.ts).
 * Buffer rows must reach DuckDB only via staged TEMP tables.
 *
 * Every file under src/ is scanned for the patterns that reintroduce the
 * crash. Comments are blanked first, using the TypeScript scanner rather than
 * a hand-rolled lexer (a regex literal containing a quote desynchronises a
 * naive one), so the hazard can still be documented in prose while SQL inside
 * string literals stays visible to the scan.
 */
import { expect } from 'chai';
import * as fs from 'fs';
import * as path from 'path';
import * as ts from 'typescript';

const SRC_DIR = path.join(__dirname, '..', '..', 'src');

/**
 * The guard names the forbidden functions in its own rejection messages and
 * match patterns, so it would trip a scan meant for query code. It is exempt
 * by name rather than by luck — today it passes only because none of those
 * mentions happens to be followed by `(`, which a harmless reword would break.
 */
const EXEMPT_FILES = ['utils/sql-guard.ts'];

const FORBIDDEN_PATTERNS: { name: string; pattern: RegExp }[] = [
  { name: 'a sqlite_* table function call', pattern: /\bsqlite_\w+\s*\(/i },
  { name: 'ATTACH ... TYPE SQLITE', pattern: /TYPE\s+SQLITE\b/i },
  { name: 'getConnectionWithBuffer', pattern: /getConnectionWithBuffer/ },
];

/**
 * Blank every comment in TypeScript source, preserving offsets and newlines
 * so match positions still map to line numbers. String and template literals
 * are left intact because the SQL this test looks for lives inside them.
 *
 * Input:  const sql = 'ATTACH x'; // ATTACH note
 * Output: const sql = 'ATTACH x';
 */
function blankComments(source: string): string {
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

function listSourceFiles(dir: string): string[] {
  const isExempt = (entry: string): boolean =>
    EXEMPT_FILES.some(exempt => entry.split(path.sep).join('/') === exempt);

  return fs
    .readdirSync(dir, { recursive: true, encoding: 'utf8' })
    .filter(entry => entry.endsWith('.ts') && !isExempt(entry))
    .map(entry => path.join(dir, entry));
}

function lineNumberAt(text: string, index: number): number {
  return text.slice(0, index).split('\n').length;
}

describe('blankComments', () => {
  it('removes line and block comments', () => {
    expect(blankComments('const a = 1; // ATTACH note')).to.not.include(
      'ATTACH'
    );
    expect(blankComments('/* ATTACH note */ const a = 1;')).to.not.include(
      'ATTACH'
    );
  });

  it('keeps string literal contents', () => {
    expect(blankComments("const sql = 'ATTACH x';")).to.include('ATTACH x');
  });

  it('keeps template literal contents', () => {
    expect(blankComments('const sql = `ATTACH ${p}`;')).to.include('ATTACH ');
  });

  it('does not desynchronise on a regex literal containing quotes', () => {
    // A hand-rolled lexer treats the quote inside the regex as a string start
    // and stops stripping comments from there on.
    const source = 'const re = /[\'"]/g;\nconst b = 2; // ATTACH note';
    expect(blankComments(source)).to.not.include('ATTACH');
  });

  it('preserves length and newlines', () => {
    const source = 'const a = 1; // note\nconst b = 2;';
    const blanked = blankComments(source);
    expect(blanked).to.have.lengthOf(source.length);
    expect(blanked.split('\n')).to.have.lengthOf(2);
  });
});

describe('buffer isolation invariant', () => {
  const files = listSourceFiles(SRC_DIR);

  it('finds the source tree', () => {
    expect(files.length).to.be.greaterThan(10);
  });

  for (const { name, pattern } of FORBIDDEN_PATTERNS) {
    it(`no source file contains ${name}`, () => {
      const violations: string[] = [];
      for (const file of files) {
        const scanned = blankComments(fs.readFileSync(file, 'utf8'));
        const match = pattern.exec(scanned);
        if (match) {
          violations.push(
            `${path.relative(SRC_DIR, file)}:${lineNumberAt(scanned, match.index)}`
          );
        }
      }
      expect(
        violations,
        `DuckDB must never open the live SQLite buffer (SIGBUS, see ` +
          `buffer-staging.ts). Found ${name} in: ${violations.join(', ')}`
      ).to.deep.equal([]);
    });
  }
});
