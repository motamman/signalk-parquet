/**
 * Layout-aware listing of the hive tree.
 *
 * Every data file lives five known levels down, tier, context, path, year
 * and day, so nothing that runs at plugin start needs a `**` glob: one
 * visits every directory in the store to find the few it wants, and on a
 * store with thousands of vessels that is hundreds of thousands of readdirs
 * on the server's main thread. This module walks the five known levels and
 * prunes at each one, so listing a week of day directories costs one readdir
 * per tier, context, path and year, never one per day in the store.
 *
 * The walk yields to the event loop every `yieldEvery` readdirs, so however
 * long it takes on a large store the server keeps answering.
 *
 * Example: listHiveDirs(dir, { level: 'day', days: new Set(['2026/258']) })
 *   -> [{ tier: 'raw', year: 2026, dayOfYear: 258, dayDir: '.../day=258', … }]
 */

import * as fs from 'fs-extra';
import * as path from 'path';
import type { Dirent } from 'fs';
import { SPECIAL_DIRECTORIES } from './path-helpers';

/** A year or day directory in the tree, with the ancestors it sits under. */
export interface HiveDir {
  tier: string;
  contextDir: string;
  pathDir: string;
  yearDir: string;
  year: number;
  /** Set at level 'day'. */
  dayDir?: string;
  dayOfYear?: number;
}

export interface ListHiveDirsOptions {
  /** Stop at year directories or descend to day directories. */
  level: 'year' | 'day';
  /** Tier names to include (without the `tier=` prefix); all when omitted. */
  tiers?: Iterable<string>;
  /** Years to include; all when omitted. */
  years?: Iterable<number>;
  /**
   * Days to include as `"<year>/<dayOfYear>"` keys (see dayKey); all when
   * omitted. Only consulted at level 'day'.
   */
  days?: Iterable<string>;
  /** Readdirs between yields to the event loop. */
  yieldEvery?: number;
}

/** The key `days` uses for one UTC day. */
export function dayKey(year: number, dayOfYear: number): string {
  return `${year}/${dayOfYear}`;
}

/** Let the event loop run once. */
export function yieldToEventLoop(): Promise<void> {
  return new Promise(resolve => setImmediate(resolve));
}

/** Directory entries, or none when the directory is missing or unreadable. */
async function readDir(dir: string): Promise<Dirent[]> {
  try {
    return await fs.readdir(dir, { withFileTypes: true });
  } catch {
    return [];
  }
}

function isSkipped(name: string): boolean {
  return SPECIAL_DIRECTORIES.has(name) || name.startsWith('.');
}

/**
 * Canonical partition values, as the writer spells them: a four-digit year
 * and a zero-padded day of year. Matching the whole value is what keeps a
 * sidecar like `year=2026-old` or `day=001-backup` out of the walk — parseInt
 * alone reads those as 2026 and 1, and the callers of this walk compact,
 * upload and delete what it returns.
 */
const YEAR_VALUE = /^\d{4}$/;
const DAY_VALUE = /^\d{3}$/;

/** The value after `prefix=` for a directory entry that has it, else null. */
function partitionValue(entry: Dirent, prefix: string): string | null {
  if (!entry.isDirectory() || isSkipped(entry.name)) return null;
  const marker = `${prefix}=`;
  return entry.name.startsWith(marker) ? entry.name.slice(marker.length) : null;
}

/**
 * List the year (or day) directories of the tree, pruned by tier, year and
 * day. Missing directories yield nothing.
 */
export async function listHiveDirs(
  dataDir: string,
  options: ListHiveDirsOptions
): Promise<HiveDir[]> {
  const tiers = options.tiers ? new Set(options.tiers) : null;
  const years = options.years ? new Set(options.years) : null;
  const days = options.days ? new Set(options.days) : null;
  const yieldEvery = Math.max(1, options.yieldEvery ?? 100);
  let reads = 0;
  const read = async (dir: string): Promise<Dirent[]> => {
    reads += 1;
    if (reads % yieldEvery === 0) await yieldToEventLoop();
    return readDir(dir);
  };

  const out: HiveDir[] = [];
  for (const tierEntry of await read(dataDir)) {
    const tier = partitionValue(tierEntry, 'tier');
    if (tier === null || (tiers && !tiers.has(tier))) continue;
    const tierDir = path.join(dataDir, tierEntry.name);
    for (const contextEntry of await read(tierDir)) {
      if (partitionValue(contextEntry, 'context') === null) continue;
      const contextDir = path.join(tierDir, contextEntry.name);
      for (const pathEntry of await read(contextDir)) {
        if (partitionValue(pathEntry, 'path') === null) continue;
        const pathDir = path.join(contextDir, pathEntry.name);
        for (const yearEntry of await read(pathDir)) {
          const yearText = partitionValue(yearEntry, 'year');
          if (yearText === null || !YEAR_VALUE.test(yearText)) continue;
          const year = parseInt(yearText, 10);
          if (years && !years.has(year)) continue;
          const yearDir = path.join(pathDir, yearEntry.name);
          const base = { tier, contextDir, pathDir, yearDir, year };
          if (options.level === 'year') {
            out.push(base);
            continue;
          }
          for (const dayEntry of await read(yearDir)) {
            const dayText = partitionValue(dayEntry, 'day');
            if (dayText === null || !DAY_VALUE.test(dayText)) continue;
            const dayOfYear = parseInt(dayText, 10);
            if (dayOfYear < 1 || dayOfYear > 366) continue;
            if (days && !days.has(dayKey(year, dayOfYear))) continue;
            out.push({
              ...base,
              dayDir: path.join(yearDir, dayEntry.name),
              dayOfYear,
            });
          }
        }
      }
    }
  }
  return out;
}

/**
 * The `.parquet` files directly inside one directory, as absolute paths.
 * Subdirectories (quarantine/, processed/ and the like) are not entered.
 */
export async function listParquetFiles(dir: string): Promise<string[]> {
  const out: string[] = [];
  for (const entry of await readDir(dir)) {
    if (entry.isFile() && entry.name.endsWith('.parquet')) {
      out.push(path.join(dir, entry.name));
    }
  }
  return out;
}

/**
 * The files matching `test` directly inside one directory, as absolute
 * paths. Used by the startup sweeps for temp-file and trash-directory names.
 */
export async function listEntries(
  dir: string,
  test: (entry: Dirent) => boolean
): Promise<string[]> {
  const out: string[] = [];
  for (const entry of await readDir(dir)) {
    if (test(entry)) out.push(path.join(dir, entry.name));
  }
  return out;
}
