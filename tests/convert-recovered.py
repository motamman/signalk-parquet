#!/usr/bin/env python3
"""
Convert recovered buffer data from legacy single-table format to per-path table format.

Source: /tmp/buffer_recovered.db (legacy buffer_records table)
Target: /tmp/buffer_recovered_new.db (per-path tables matching live schema)

Skips navigation.position (object path, only 690 rows with NULL values).
"""

import sqlite3
import time

SOURCE_DB = "/tmp/buffer_recovered.db"
TARGET_DB = "/tmp/buffer_recovered_new.db"
SKIP_PATHS = {"navigation.position"}
BATCH_SIZE = 50000


def path_to_table_name(path: str) -> str:
    return "buffer_" + path.replace(".", "_")


def main():
    # Open source read-only
    src = sqlite3.connect(f"file:{SOURCE_DB}?mode=ro", uri=True)
    src.row_factory = sqlite3.Row

    # Get all distinct paths
    paths = [row[0] for row in src.execute("SELECT DISTINCT path FROM buffer_records").fetchall()]
    print(f"Found {len(paths)} paths in source DB")

    # Create target DB with manual transaction control
    dst = sqlite3.connect(TARGET_DB, isolation_level=None)
    dst.execute("PRAGMA journal_mode=WAL")
    dst.execute("PRAGMA synchronous=NORMAL")

    # Create buffer_tables metadata table
    dst.execute("""
        CREATE TABLE IF NOT EXISTS buffer_tables (
            path TEXT PRIMARY KEY,
            table_name TEXT NOT NULL,
            is_object INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    total_rows = 0
    start_time = time.time()

    for path in sorted(paths):
        if path in SKIP_PATHS:
            count = src.execute("SELECT COUNT(*) FROM buffer_records WHERE path = ?", (path,)).fetchone()[0]
            print(f"  SKIP {path}: {count:,} rows (object path)")
            continue

        table_name = path_to_table_name(path)

        # Create per-path table
        dst.execute(f"""
            CREATE TABLE [{table_name}] (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                context TEXT NOT NULL,
                received_timestamp TEXT NOT NULL,
                signalk_timestamp TEXT NOT NULL,
                value TEXT,
                source TEXT,
                source_label TEXT,
                exported INTEGER NOT NULL DEFAULT 0,
                export_batch_id TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now'))
            )
        """)
        dst.execute(f"CREATE INDEX [idx_{table_name}_ctx_exp] ON [{table_name}] (context, exported)")
        dst.execute(f"CREATE INDEX [idx_{table_name}_received] ON [{table_name}] (received_timestamp)")

        # Register in buffer_tables
        dst.execute("INSERT INTO buffer_tables (path, table_name, is_object) VALUES (?, ?, 0)", (path, table_name))

        # Copy data in batches
        path_count = 0
        cursor = src.execute("""
            SELECT context, received_timestamp, signalk_timestamp,
                   CAST(value AS TEXT), source, source_label,
                   exported, export_batch_id, created_at
            FROM buffer_records WHERE path = ?
            ORDER BY received_timestamp
        """, (path,))

        while True:
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
            dst.execute("BEGIN")
            dst.executemany(f"""
                INSERT INTO [{table_name}]
                    (context, received_timestamp, signalk_timestamp, value,
                     source, source_label, exported, export_batch_id, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, rows)
            dst.execute("COMMIT")
            path_count += len(rows)

        total_rows += path_count
        print(f"  {path}: {path_count:,} rows → {table_name}")

    elapsed = time.time() - start_time
    print(f"\nDone: {total_rows:,} rows converted in {elapsed:.1f}s")
    print(f"Output: {TARGET_DB}")

    src.close()
    dst.close()


if __name__ == "__main__":
    main()
