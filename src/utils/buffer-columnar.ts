/**
 * Columnar encoding for buffer rows crossing a thread boundary.
 *
 * Rows are never sent as objects. Measured on a Raspberry Pi 5 (2026-09-26),
 * structured-cloning row objects costs the *receiving* thread about 4.3 µs a
 * row — 21.7 ms for a 5,000-row page, p99 30 ms — which is the same order as
 * reading them there directly, and would make a worker pointless. Packing each
 * column into one typed array and transferring the buffers costs the receiver
 * 0.2 ms flat at any page size, and moves the packing cost to the sender.
 *
 * One column becomes one typed array plus an explicit null mask, so a null is
 * a null rather than a NaN or an empty string: the buffer's columns are
 * nullable and the DuckDB appender needs to know which is which.
 *
 * Integers use BigInt64Array rather than Float64Array. Row ids are far below
 * 2^53 today, but an exact integer channel costs the same eight bytes and
 * removes the question.
 */

/** How a column's values travel. Fixed by the column's declared SQLite type. */
export type ColumnKind = 'i64' | 'f64' | 'str';

export interface ColumnarColumn {
  name: string;
  kind: ColumnKind;
  /** Uint8Array, one byte per row: 1 when the value is null. */
  nulls: ArrayBuffer;
  /** i64: BigInt64Array. f64: Float64Array. Absent for 'str'. */
  values?: ArrayBuffer;
  /** 'str' only: the rows' utf8 bytes, concatenated. */
  data?: ArrayBuffer;
  /** 'str' only: Uint32Array of rowCount + 1 byte offsets into `data`. */
  offsets?: ArrayBuffer;
}

export interface ColumnarPage {
  rowCount: number;
  columns: ColumnarColumn[];
}

/**
 * The channel a column travels on, from its declared SQLite type — the same
 * mapping `buffer-staging.ts` uses to pick a DuckDB column type, so a column
 * arrives as the type the appender is going to want.
 */
export function columnKindFor(sqliteType: string): ColumnKind {
  const t = sqliteType.toUpperCase();
  if (t.includes('INT')) return 'i64';
  if (t.includes('REAL') || t.includes('FLOA') || t.includes('DOUB')) {
    return 'f64';
  }
  return 'str';
}

/** A value SQLite hands back for a nullable, dynamically typed column. */
type CellValue = unknown;

function isNullish(v: CellValue): boolean {
  return v === null || v === undefined;
}

/**
 * Pack rows into a page, and list the buffers to transfer with it.
 *
 * SQLite columns are dynamically typed, so a value that will not convert on
 * its declared channel is encoded as null rather than as a guess — the same
 * choice the DuckDB appender makes today for unconvertible content.
 */
export function encodePage(
  rows: Array<Record<string, unknown>>,
  schema: Array<{ name: string; type: string }>
): { page: ColumnarPage; transfer: ArrayBuffer[] } {
  const rowCount = rows.length;
  const columns: ColumnarColumn[] = [];
  const transfer: ArrayBuffer[] = [];

  for (const col of schema) {
    const kind = columnKindFor(col.type);
    const nulls = new Uint8Array(rowCount);

    if (kind === 'i64' || kind === 'f64') {
      const values =
        kind === 'i64'
          ? new BigInt64Array(rowCount)
          : new Float64Array(rowCount);
      for (let i = 0; i < rowCount; i++) {
        const v = rows[i][col.name];
        if (isNullish(v)) {
          nulls[i] = 1;
          continue;
        }
        if (kind === 'i64') {
          const n = typeof v === 'bigint' ? v : Number(v);
          if (typeof n === 'bigint') {
            (values as BigInt64Array)[i] = n;
          } else if (Number.isFinite(n)) {
            (values as BigInt64Array)[i] = BigInt(Math.trunc(n));
          } else {
            nulls[i] = 1;
          }
        } else {
          const n = Number(v);
          if (Number.isFinite(n)) {
            (values as Float64Array)[i] = n;
          } else {
            nulls[i] = 1;
          }
        }
      }
      columns.push({
        name: col.name,
        kind,
        nulls: nulls.buffer,
        values: values.buffer,
      });
      transfer.push(nulls.buffer, values.buffer);
      continue;
    }

    // Text: one utf8 blob plus byte offsets, so the receiver decodes only the
    // rows it actually reads and never allocates a string per row up front.
    const encoder = new TextEncoder();
    const parts: Uint8Array[] = new Array(rowCount);
    const offsets = new Uint32Array(rowCount + 1);
    let total = 0;
    for (let i = 0; i < rowCount; i++) {
      const v = rows[i][col.name];
      if (isNullish(v)) {
        nulls[i] = 1;
        parts[i] = new Uint8Array(0);
      } else {
        parts[i] = encoder.encode(String(v));
      }
      offsets[i] = total;
      total += parts[i].length;
    }
    offsets[rowCount] = total;
    const data = new Uint8Array(total);
    for (let i = 0; i < rowCount; i++) {
      if (parts[i].length > 0) data.set(parts[i], offsets[i]);
    }
    columns.push({
      name: col.name,
      kind,
      nulls: nulls.buffer,
      data: data.buffer,
      offsets: offsets.buffer,
    });
    transfer.push(nulls.buffer, data.buffer, offsets.buffer);
  }

  return { page: { rowCount, columns }, transfer };
}

/** Reads one column of a received page by row index. */
export interface ColumnReader {
  name: string;
  kind: ColumnKind;
  /** null, a bigint for 'i64', a number for 'f64', a string for 'str'. */
  at(row: number): bigint | number | string | null;
}

/**
 * Readers for a received page, in the page's column order. Nothing is copied:
 * the typed arrays are views on the transferred buffers, and a text value is
 * decoded on the call that asks for it.
 */
export function readPage(page: ColumnarPage): ColumnReader[] {
  const decoder = new TextDecoder();
  return page.columns.map(col => {
    const nulls = new Uint8Array(col.nulls);
    if (col.kind === 'i64') {
      const values = new BigInt64Array(col.values as ArrayBuffer);
      return {
        name: col.name,
        kind: col.kind,
        at: (row: number) => (nulls[row] ? null : values[row]),
      };
    }
    if (col.kind === 'f64') {
      const values = new Float64Array(col.values as ArrayBuffer);
      return {
        name: col.name,
        kind: col.kind,
        at: (row: number) => (nulls[row] ? null : values[row]),
      };
    }
    const data = new Uint8Array(col.data as ArrayBuffer);
    const offsets = new Uint32Array(col.offsets as ArrayBuffer);
    return {
      name: col.name,
      kind: col.kind,
      at: (row: number) =>
        nulls[row]
          ? null
          : decoder.decode(data.subarray(offsets[row], offsets[row + 1])),
    };
  });
}
