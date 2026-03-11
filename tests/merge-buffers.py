#!/usr/bin/env python3
"""
Merge recovered and live buffer DBs into a clean output.

Inputs (read-only):
  A (precedence): /tmp/buffer_recovered_new.db  — recovered scalar tables
  B (fallback):   /tmp/buffer.db.restore        — live DB with full schema

Output:
  /tmp/buffer.db.clean — fresh DB, union of all tables, deduped
"""

import sqlite3
import os
import sys

RECOVERED_DB = "/tmp/buffer_recovered_new.db"
LIVE_DB = "/tmp/buffer.db.restore"
OUTPUT_DB = "/tmp/buffer.db.clean"

POSITION_TABLE = "buffer_navigation_position"

SCALAR_COLUMNS = [
    "context", "received_timestamp", "signalk_timestamp", "value",
    "source", "source_label", "source_type", "source_pgn", "source_src", "meta",
    "exported", "export_batch_id", "created_at"
]

# Columns present in recovered DB (missing source_type, source_pgn, source_src, meta)
RECOVERED_COLUMNS = [
    "context", "received_timestamp", "signalk_timestamp", "value",
    "source", "source_label", "exported", "export_batch_id", "created_at"
]


def create_scalar_table(cur, table_name):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS [{table_name}] (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            context TEXT NOT NULL,
            received_timestamp TEXT NOT NULL,
            signalk_timestamp TEXT NOT NULL,
            value TEXT,
            source TEXT,
            source_label TEXT,
            source_type TEXT,
            source_pgn INTEGER,
            source_src TEXT,
            meta TEXT,
            exported INTEGER NOT NULL DEFAULT 0,
            export_batch_id TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    cur.execute(f"CREATE INDEX IF NOT EXISTS [idx_{table_name}_ctx_exp] ON [{table_name}] (context, exported)")
    cur.execute(f"CREATE INDEX IF NOT EXISTS [idx_{table_name}_received] ON [{table_name}] (received_timestamp)")


def create_position_table(cur):
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS [{POSITION_TABLE}] (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            context TEXT NOT NULL,
            received_timestamp TEXT NOT NULL,
            signalk_timestamp TEXT NOT NULL,
            value_json TEXT,
            value_latitude REAL,
            value_longitude REAL,
            source TEXT,
            source_label TEXT,
            source_type TEXT,
            source_pgn INTEGER,
            source_src TEXT,
            meta TEXT,
            exported INTEGER NOT NULL DEFAULT 0,
            export_batch_id TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)
    cur.execute(f"CREATE INDEX IF NOT EXISTS [idx_{POSITION_TABLE}_ctx_exp] ON [{POSITION_TABLE}] (context, exported)")
    cur.execute(f"CREATE INDEX IF NOT EXISTS [idx_{POSITION_TABLE}_received] ON [{POSITION_TABLE}] (received_timestamp)")


def get_buffer_tables(cur, schema_prefix=""):
    """Get dict of path -> table_name from buffer_tables metadata."""
    prefix = f"[{schema_prefix}]." if schema_prefix else ""
    try:
        rows = cur.execute(f"SELECT path, table_name FROM {prefix}buffer_tables").fetchall()
        return {row[0]: row[1] for row in rows}
    except sqlite3.OperationalError:
        return {}


def main():
    # Validate inputs exist
    for path, label in [(RECOVERED_DB, "Recovered"), (LIVE_DB, "Live")]:
        if not os.path.exists(path):
            print(f"ERROR: {label} DB not found: {path}")
            sys.exit(1)

    # Remove output if exists
    if os.path.exists(OUTPUT_DB):
        os.remove(OUTPUT_DB)

    # Open output DB
    dst = sqlite3.connect(OUTPUT_DB)
    dst.execute("PRAGMA journal_mode=WAL")
    dst.execute("PRAGMA synchronous=NORMAL")

    # Attach inputs read-only
    dst.execute("ATTACH DATABASE ? AS recovered", (f"file:{RECOVERED_DB}?mode=ro",))
    dst.execute("ATTACH DATABASE ? AS live", (f"file:{LIVE_DB}?mode=ro",))

    # Create buffer_tables metadata in output
    dst.execute("""
        CREATE TABLE buffer_tables (
            path TEXT PRIMARY KEY,
            table_name TEXT NOT NULL,
            created_at TEXT NOT NULL DEFAULT (datetime('now'))
        )
    """)

    # Get table lists from both
    recovered_tables = get_buffer_tables(dst, "recovered")
    live_tables = get_buffer_tables(dst, "live")

    print(f"Recovered DB: {len(recovered_tables)} tables")
    print(f"Live DB:      {len(live_tables)} tables")

    # Collect all paths (union)
    all_paths = set(recovered_tables.keys()) | set(live_tables.keys())

    # Handle position table from live DB
    position_path = None
    for path, tname in live_tables.items():
        if tname == POSITION_TABLE:
            position_path = path
            break

    if position_path:
        print(f"\n--- Position table: {POSITION_TABLE} ---")
        create_position_table(dst)

        pos_cols = [
            "context", "received_timestamp", "signalk_timestamp",
            "value_json", "value_latitude", "value_longitude",
            "source", "source_label", "source_type", "source_pgn", "source_src", "meta",
            "exported", "export_batch_id", "created_at"
        ]
        cols_str = ", ".join(pos_cols)
        dst.execute(f"""
            INSERT INTO [{POSITION_TABLE}] ({cols_str})
            SELECT {cols_str} FROM [live].[{POSITION_TABLE}]
        """)
        count = dst.execute(f"SELECT COUNT(*) FROM [{POSITION_TABLE}]").fetchone()[0]
        print(f"  Copied {count:,} position rows from live")

        dst.execute("INSERT INTO buffer_tables (path, table_name) VALUES (?, ?)",
                    (position_path, POSITION_TABLE))
        all_paths.discard(position_path)

    # Process scalar tables
    total_from_a = 0
    total_from_b = 0
    total_dupes = 0

    for path in sorted(all_paths):
        in_recovered = path in recovered_tables
        in_live = path in live_tables

        table_name = recovered_tables.get(path) or live_tables.get(path)
        print(f"\n--- {table_name} ({path}) ---")

        create_scalar_table(dst, table_name)

        rows_a = 0
        rows_b = 0
        dupes = 0

        if in_recovered:
            # Insert all recovered rows (NULLs for missing columns)
            recovered_select = ", ".join(RECOVERED_COLUMNS)
            # Build insert with NULLs for missing cols
            dst.execute(f"""
                INSERT INTO [{table_name}] (
                    context, received_timestamp, signalk_timestamp, value,
                    source, source_label, source_type, source_pgn, source_src, meta,
                    exported, export_batch_id, created_at
                )
                SELECT
                    context, received_timestamp, signalk_timestamp, value,
                    source, source_label, NULL, NULL, NULL, NULL,
                    exported, export_batch_id, created_at
                FROM [recovered].[{table_name}]
            """)
            rows_a = dst.execute(f"SELECT changes()").fetchone()[0]
            print(f"  Recovered (A): {rows_a:,} rows")

        if in_live:
            if in_recovered:
                # Anti-join: insert B rows not already in output from A
                before = dst.execute(f"SELECT COUNT(*) FROM [{table_name}]").fetchone()[0]
                cols_str = ", ".join(SCALAR_COLUMNS)
                b_cols = ", ".join(f"b.{c}" for c in SCALAR_COLUMNS)
                dst.execute(f"""
                    INSERT INTO [{table_name}] ({cols_str})
                    SELECT {b_cols}
                    FROM [live].[{table_name}] b
                    WHERE NOT EXISTS (
                        SELECT 1 FROM [{table_name}] a
                        WHERE a.received_timestamp = b.received_timestamp
                          AND a.value IS b.value
                          AND a.source_label IS b.source_label
                    )
                """)
                rows_b = dst.execute(f"SELECT changes()").fetchone()[0]
                after = dst.execute(f"SELECT COUNT(*) FROM [{table_name}]").fetchone()[0]
                live_total = dst.execute(f"SELECT COUNT(*) FROM [live].[{table_name}]").fetchone()[0]
                dupes = live_total - rows_b
                print(f"  Live (B):      {rows_b:,} new rows, {dupes:,} duplicates skipped (of {live_total:,})")
            else:
                # Only in live — copy straight
                cols_str = ", ".join(SCALAR_COLUMNS)
                dst.execute(f"""
                    INSERT INTO [{table_name}] ({cols_str})
                    SELECT {cols_str} FROM [live].[{table_name}]
                """)
                rows_b = dst.execute(f"SELECT changes()").fetchone()[0]
                print(f"  Live (B) only: {rows_b:,} rows")

        # Register in buffer_tables
        dst.execute("INSERT OR IGNORE INTO buffer_tables (path, table_name) VALUES (?, ?)",
                    (path, table_name))

        total_from_a += rows_a
        total_from_b += rows_b
        total_dupes += dupes

    dst.commit()

    # Final stats
    print("\n" + "=" * 60)
    print(f"TOTALS:")
    print(f"  From recovered (A): {total_from_a:,}")
    print(f"  From live (B):      {total_from_b:,}")
    print(f"  Duplicates skipped: {total_dupes:,}")
    print(f"  Grand total:        {total_from_a + total_from_b:,}")

    # Verify
    print(f"\nOutput: {OUTPUT_DB}")
    print(f"Size:   {os.path.getsize(OUTPUT_DB) / 1024 / 1024:.1f} MB")

    tables = dst.execute("SELECT path, table_name FROM buffer_tables ORDER BY path").fetchall()
    print(f"\nbuffer_tables ({len(tables)} entries):")
    for path, tname in tables:
        count = dst.execute(f"SELECT COUNT(*) FROM [{tname}]").fetchone()[0]
        print(f"  {tname}: {count:,} rows")

    dst.execute("DETACH DATABASE recovered")
    dst.execute("DETACH DATABASE live")
    dst.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
