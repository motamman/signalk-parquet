#!/usr/bin/env python3
"""
Recover data from a corrupted SQLite buffer.db by raw page scanning.

CRITICAL: Read-only. Never modifies the source file.

Scans all pages looking for leaf table B-tree pages (0x0d), parses cell
payloads, filters for buffer_records rows (16 columns), and writes
recovered data to a clean SQLite database.
"""

import struct
import sys
import os
import sqlite3
import time

CORRUPT_DB = "/tmp/buffer.db.corrupt"
OUTPUT_DB = "/tmp/buffer_recovered.db"

EXPECTED_COLUMNS = 16  # buffer_records has 16 columns (including rowid stored as id)


def read_varint(data, offset):
    """Read a SQLite varint (1-9 bytes, Huffman-coded)."""
    result = 0
    for i in range(9):
        if offset + i >= len(data):
            raise ValueError(f"Varint extends past data at offset {offset}")
        byte = data[offset + i]
        if i < 8:
            result = (result << 7) | (byte & 0x7F)
            if byte < 0x80:
                return result, offset + i + 1
        else:
            # 9th byte: all 8 bits are data
            result = (result << 8) | byte
            return result, offset + i + 1
    raise ValueError("Varint too long")


def decode_column(serial_type, data, offset):
    """Decode a column value based on its serial type. Returns (value, new_offset)."""
    if serial_type == 0:
        return None, offset
    elif serial_type == 1:
        val = struct.unpack_from(">b", data, offset)[0]
        return val, offset + 1
    elif serial_type == 2:
        val = struct.unpack_from(">h", data, offset)[0]
        return val, offset + 2
    elif serial_type == 3:
        b = data[offset:offset + 3]
        val = int.from_bytes(b, 'big', signed=True)
        return val, offset + 3
    elif serial_type == 4:
        val = struct.unpack_from(">i", data, offset)[0]
        return val, offset + 4
    elif serial_type == 5:
        b = data[offset:offset + 6]
        val = int.from_bytes(b, 'big', signed=True)
        return val, offset + 6
    elif serial_type == 6:
        val = struct.unpack_from(">q", data, offset)[0]
        return val, offset + 8
    elif serial_type == 7:
        val = struct.unpack_from(">d", data, offset)[0]
        return val, offset + 8
    elif serial_type == 8:
        return 0, offset
    elif serial_type == 9:
        return 1, offset
    elif serial_type >= 12 and serial_type % 2 == 0:
        # Blob
        length = (serial_type - 12) // 2
        val = data[offset:offset + length]
        return val, offset + length
    elif serial_type >= 13 and serial_type % 2 == 1:
        # Text
        length = (serial_type - 13) // 2
        try:
            val = data[offset:offset + length].decode("utf-8", errors="replace")
        except Exception:
            val = data[offset:offset + length].decode("latin-1")
        return val, offset + length
    else:
        raise ValueError(f"Unknown serial type {serial_type}")


def content_size(serial_type):
    """Return the byte size of a column value in the record body."""
    if serial_type == 0:
        return 0
    elif serial_type in (8, 9):
        return 0
    elif 1 <= serial_type <= 6:
        return serial_type
    elif serial_type == 7:
        return 8
    elif serial_type >= 12 and serial_type % 2 == 0:
        return (serial_type - 12) // 2
    elif serial_type >= 13 and serial_type % 2 == 1:
        return (serial_type - 13) // 2
    return 0


def read_overflow_chain(f, page_number, page_size, bytes_needed):
    """Follow overflow page chain to collect remaining payload bytes."""
    collected = bytearray()
    current_page = page_number
    while current_page != 0 and len(collected) < bytes_needed:
        offset = (current_page - 1) * page_size
        f.seek(offset)
        page_data = f.read(page_size)
        if len(page_data) < 8:
            break
        next_page = struct.unpack_from(">I", page_data, 0)[0]
        # Usable content starts at byte 4
        usable = page_data[4:]
        remaining = bytes_needed - len(collected)
        collected.extend(usable[:remaining])
        current_page = next_page
    return bytes(collected)


def is_valid_row(values):
    """Validate that decoded values look like a buffer_records row."""
    if len(values) != EXPECTED_COLUMNS:
        return False

    # values[0] = id (rowid) — should be a positive integer
    if not isinstance(values[0], int) or values[0] <= 0:
        return False

    # values[1] = context — should start with "vessels."
    if not isinstance(values[1], str) or not values[1].startswith("vessels."):
        return False

    # values[2] = path — should contain dots
    if not isinstance(values[2], str) or "." not in values[2]:
        return False

    # values[3] = received_timestamp — should look like ISO timestamp
    if isinstance(values[3], str):
        if not (values[3].startswith("202") and "T" in values[3]):
            return False
    else:
        return False

    return True


def main():
    if not os.path.exists(CORRUPT_DB):
        print(f"ERROR: {CORRUPT_DB} not found")
        sys.exit(1)

    file_size = os.path.getsize(CORRUPT_DB)
    print(f"Source: {CORRUPT_DB} ({file_size / (1024*1024):.1f} MB)")

    # Read SQLite header
    with open(CORRUPT_DB, "rb") as f:
        header = f.read(100)

    if header[:16] != b"SQLite format 3\x00":
        print("WARNING: SQLite header magic not found, proceeding anyway")

    page_size = struct.unpack_from(">H", header, 16)[0]
    if page_size == 1:
        page_size = 65536
    page_count = file_size // page_size

    print(f"Page size: {page_size}, Page count: {page_count}")

    # Prepare output database
    if os.path.exists(OUTPUT_DB):
        os.remove(OUTPUT_DB)

    out_conn = sqlite3.connect(OUTPUT_DB)
    out_cur = out_conn.cursor()
    out_cur.execute("""
        CREATE TABLE buffer_records (
            id INTEGER PRIMARY KEY,
            context TEXT,
            path TEXT,
            received_timestamp TEXT,
            signalk_timestamp TEXT,
            value REAL,
            value_json TEXT,
            source TEXT,
            source_label TEXT,
            source_type TEXT,
            source_pgn INTEGER,
            source_src TEXT,
            meta TEXT,
            exported INTEGER DEFAULT 0,
            export_batch_id TEXT,
            created_at TEXT
        )
    """)
    out_conn.execute("PRAGMA journal_mode=WAL")
    out_conn.execute("PRAGMA synchronous=NORMAL")

    stats = {
        "pages_scanned": 0,
        "leaf_pages": 0,
        "cells_found": 0,
        "rows_recovered": 0,
        "rows_skipped": 0,
        "parse_errors": 0,
        "overflow_cells": 0,
        "duplicate_ids": 0,
    }

    seen_ids = set()
    batch = []
    BATCH_SIZE = 10000
    debug_col_counts = {}  # track serial type counts

    start_time = time.time()
    last_report = start_time

    with open(CORRUPT_DB, "rb") as f:
        for page_num in range(1, page_count + 1):
            page_offset = (page_num - 1) * page_size
            f.seek(page_offset)
            page_data = f.read(page_size)

            if len(page_data) < page_size:
                break

            stats["pages_scanned"] += 1

            # Check for leaf table B-tree page (type 0x0d)
            # Page 1 has the 100-byte file header, so the page header starts at offset 100
            if page_num == 1:
                page_header_offset = 100
            else:
                page_header_offset = 0

            if page_data[page_header_offset] != 0x0D:
                continue

            stats["leaf_pages"] += 1

            # Parse leaf page header
            # Bytes: type(1), freeblock(2), cell_count(2), cell_content_offset(2), fragmented_free(1)
            try:
                cell_count = struct.unpack_from(">H", page_data, page_header_offset + 3)[0]
            except struct.error:
                continue

            if cell_count == 0 or cell_count > 1000:
                continue

            # Cell pointer array starts right after the 8-byte header
            cell_ptr_start = page_header_offset + 8

            for cell_idx in range(cell_count):
                ptr_offset = cell_ptr_start + cell_idx * 2
                if ptr_offset + 2 > len(page_data):
                    break

                cell_offset = struct.unpack_from(">H", page_data, ptr_offset)[0]
                if cell_offset == 0 or cell_offset >= page_size:
                    continue

                stats["cells_found"] += 1

                try:
                    # Read payload size
                    payload_size, pos = read_varint(page_data, cell_offset)
                    # Read rowid
                    rowid, pos = read_varint(page_data, pos)

                    if payload_size <= 0 or payload_size > 100_000_000:
                        stats["rows_skipped"] += 1
                        continue

                    # Calculate how much payload is inline vs overflow
                    # For leaf table pages: usable_size = page_size
                    # max inline = usable_size - 35 (for non-overflow)
                    # min inline = ((usable_size - 12) * 32 // 255) - 23
                    usable = page_size
                    max_inline = usable - 35
                    min_inline = ((usable - 12) * 32 // 255) - 23

                    if payload_size <= max_inline:
                        # All inline
                        inline_size = payload_size
                        has_overflow = False
                    else:
                        # Has overflow
                        inline_size = min_inline
                        has_overflow = True

                    # Get inline payload
                    inline_end = pos + inline_size
                    if inline_end > page_size:
                        stats["rows_skipped"] += 1
                        continue

                    inline_payload = page_data[pos:inline_end]

                    if has_overflow:
                        stats["overflow_cells"] += 1
                        # Read overflow page number (4 bytes after inline payload)
                        if inline_end + 4 > page_size:
                            stats["rows_skipped"] += 1
                            continue
                        overflow_page = struct.unpack_from(">I", page_data, inline_end)[0]
                        overflow_needed = payload_size - inline_size
                        overflow_data = read_overflow_chain(f, overflow_page, page_size, overflow_needed)
                        # Restore file position for main scan
                        full_payload = inline_payload + overflow_data
                    else:
                        full_payload = inline_payload

                    if len(full_payload) < payload_size:
                        # Couldn't get full payload (broken overflow chain)
                        # Try with what we have
                        pass

                    # Parse record header
                    header_size, hpos = read_varint(full_payload, 0)
                    if header_size <= 0 or header_size > len(full_payload):
                        stats["rows_skipped"] += 1
                        continue

                    # Read serial types
                    serial_types = []
                    while hpos < header_size and hpos < len(full_payload):
                        st, hpos = read_varint(full_payload, hpos)
                        serial_types.append(st)

                    # We expect 15 serial types (16 columns minus rowid which is implicit)
                    # Actually, in SQLite the rowid is NOT stored in the record body
                    # for INTEGER PRIMARY KEY columns. So we might have 15 or 16 serial types.
                    # Let's check: if id is INTEGER PRIMARY KEY, its value equals the rowid
                    # and is NOT in the record body. So we'd have 15 serial types for the
                    # remaining 15 columns.
                    # But if id is just INTEGER (not PRIMARY KEY alias), it IS in the record.
                    # In the legacy buffer_records, id was likely the rowid alias.
                    # Let's handle both cases.

                    if len(serial_types) == 15:
                        # rowid = id, 15 remaining columns in record
                        num_record_cols = 15
                        id_is_rowid = True
                    elif len(serial_types) == 16:
                        # All 16 columns in record
                        num_record_cols = 16
                        id_is_rowid = False
                    else:
                        stats["rows_skipped"] += 1
                        continue

                    # Compute expected body size
                    body_size = sum(content_size(st) for st in serial_types)
                    body_start = header_size

                    if body_start + body_size > len(full_payload):
                        # Payload truncated — skip if we're way off
                        if body_start + body_size > len(full_payload) + 100:
                            stats["rows_skipped"] += 1
                            continue

                    # Decode columns
                    values = []
                    data_offset = body_start
                    for st in serial_types:
                        if data_offset >= len(full_payload):
                            values.append(None)
                            continue
                        try:
                            val, data_offset = decode_column(st, full_payload, data_offset)
                            values.append(val)
                        except Exception:
                            values.append(None)

                    # Build full row
                    if id_is_rowid:
                        row = [rowid] + values  # id = rowid, then 15 cols
                    else:
                        row = values  # all 16 cols, row[0] = id
                        # INTEGER PRIMARY KEY stores NULL in record body; real value is rowid
                        if row[0] is None:
                            row[0] = rowid

                    if len(row) != EXPECTED_COLUMNS:
                        stats["rows_skipped"] += 1
                        continue

                    # Validate
                    if not is_valid_row(row):
                        stats["rows_skipped"] += 1
                        col_key = len(serial_types)
                        debug_col_counts[col_key] = debug_col_counts.get(col_key, 0) + 1
                        continue

                    row_id = row[0]
                    if row_id in seen_ids:
                        stats["duplicate_ids"] += 1
                        continue
                    seen_ids.add(row_id)

                    batch.append(tuple(row))
                    stats["rows_recovered"] += 1

                    if len(batch) >= BATCH_SIZE:
                        out_cur.executemany(
                            "INSERT OR IGNORE INTO buffer_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                            batch,
                        )
                        out_conn.commit()
                        batch.clear()

                except Exception as e:
                    stats["parse_errors"] += 1
                    continue

            # Progress report every 10 seconds
            now = time.time()
            if now - last_report >= 10:
                elapsed = now - start_time
                pct = page_num / page_count * 100
                rate = page_num / elapsed if elapsed > 0 else 0
                eta = (page_count - page_num) / rate if rate > 0 else 0
                print(
                    f"  [{pct:5.1f}%] Page {page_num}/{page_count} | "
                    f"Leaf: {stats['leaf_pages']} | Recovered: {stats['rows_recovered']} | "
                    f"Skipped: {stats['rows_skipped']} | Errors: {stats['parse_errors']} | "
                    f"ETA: {eta:.0f}s"
                )
                last_report = now

    # Flush remaining batch
    if batch:
        out_cur.executemany(
            "INSERT OR IGNORE INTO buffer_records VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            batch,
        )
        out_conn.commit()

    # Create useful indexes
    print("\nCreating indexes...")
    out_cur.execute("CREATE INDEX IF NOT EXISTS idx_path ON buffer_records(path)")
    out_cur.execute("CREATE INDEX IF NOT EXISTS idx_exported ON buffer_records(exported)")
    out_cur.execute("CREATE INDEX IF NOT EXISTS idx_received ON buffer_records(received_timestamp)")
    out_conn.commit()
    out_conn.close()

    elapsed = time.time() - start_time

    if debug_col_counts:
        print(f"\n--- DEBUG: Serial type count distribution ---")
        for k, v in sorted(debug_col_counts.items()):
            print(f"  {k} serial types: {v:,} rows")

    print(f"\n{'='*60}")
    print(f"Recovery complete in {elapsed:.1f}s")
    print(f"{'='*60}")
    print(f"Pages scanned:   {stats['pages_scanned']:>10,}")
    print(f"Leaf pages:      {stats['leaf_pages']:>10,}")
    print(f"Cells found:     {stats['cells_found']:>10,}")
    print(f"Rows recovered:  {stats['rows_recovered']:>10,}")
    print(f"Rows skipped:    {stats['rows_skipped']:>10,}")
    print(f"Parse errors:    {stats['parse_errors']:>10,}")
    print(f"Overflow cells:  {stats['overflow_cells']:>10,}")
    print(f"Duplicate IDs:   {stats['duplicate_ids']:>10,}")
    print(f"\nOutput: {OUTPUT_DB}")


if __name__ == "__main__":
    main()
