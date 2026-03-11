#!/usr/bin/env python3
"""
Build aggregation tiers (5s, 60s, 1h) from raw parquet data using DuckDB.

Scans tier=raw for all context/path/date combinations and builds
aggregated parquet files directly — no API calls needed.

Usage:
    python3 build-tiers.py /path/to/data
    python3 build-tiers.py /path/to/data --from 2025-07-22 --to 2025-12-31
    python3 build-tiers.py /path/to/data --dry-run
"""

import argparse
import os
import sys
import glob
import math
import time
from datetime import datetime, timedelta, timezone

try:
    import duckdb
except ImportError:
    print("Install duckdb: pip3 install duckdb")
    sys.exit(1)

# Tier hierarchy: raw -> 5s -> 60s -> 1h
TIERS = [
    {"source": "raw", "target": "5s", "interval": 5},
    {"source": "5s", "target": "60s", "interval": 60},
    {"source": "60s", "target": "1h", "interval": 3600},
]

# Angular paths use vector averaging (ATAN2(AVG(SIN), AVG(COS)))
ANGULAR_PATHS = {
    "environment__wind__directionMagnetic",
    "environment__wind__directionTrue",
    "environment__outside__rapidWind__windDirection",
    "navigation__courseOverGroundMagnetic",
    "navigation__courseOverGroundTrue",
    "navigation__headingMagnetic",
    "navigation__wave__direction",
}


def day_of_year(dt):
    return dt.timetuple().tm_yday


def date_from_year_day(year, doy):
    return datetime(year, 1, 1, tzinfo=timezone.utc) + timedelta(days=doy - 1)


def find_raw_dates(data_dir):
    """Find all (context, path, year, day) combos in tier=raw."""
    raw_dir = os.path.join(data_dir, "tier=raw")
    if not os.path.isdir(raw_dir):
        print(f"No tier=raw directory at {raw_dir}")
        return []

    entries = []
    for context_dir in sorted(glob.glob(os.path.join(raw_dir, "context=*"))):
        context = os.path.basename(context_dir).split("=", 1)[1]
        for path_dir in sorted(glob.glob(os.path.join(context_dir, "path=*"))):
            sk_path = os.path.basename(path_dir).split("=", 1)[1]
            for year_dir in sorted(glob.glob(os.path.join(path_dir, "year=*"))):
                year = int(os.path.basename(year_dir).split("=", 1)[1])
                for day_dir in sorted(glob.glob(os.path.join(year_dir, "day=*"))):
                    doy = int(os.path.basename(day_dir).split("=", 1)[1])
                    # Check for actual parquet files (exclude processed/quarantine/etc)
                    parquet_files = [
                        f for f in glob.glob(os.path.join(day_dir, "*.parquet"))
                        if "/processed/" not in f
                        and "/quarantine/" not in f
                        and "/failed/" not in f
                        and "/repaired/" not in f
                    ]
                    if parquet_files:
                        entries.append((context, sk_path, year, doy, parquet_files))
    return entries


def clean_tier_dir(data_dir, tier, context, sk_path, year, doy):
    """Delete all existing parquet files in a tier/context/path/date folder."""
    target_dir = os.path.join(data_dir, f"tier={tier}", f"context={context}", f"path={sk_path}",
                              f"year={year}", f"day={doy:03d}")
    if not os.path.isdir(target_dir):
        return 0
    removed = 0
    for f in glob.glob(os.path.join(target_dir, "*.parquet")):
        os.remove(f)
        removed += 1
    return removed


def build_raw_to_tier_sql(files, interval_seconds, is_angular, output_file):
    """Build SQL to aggregate raw data into a tier."""
    file_list = ", ".join(f"'{f}'" for f in files)

    if is_angular:
        return f"""
        COPY (
            SELECT
                time_bucket(INTERVAL '{interval_seconds} seconds', received_timestamp::TIMESTAMP) as bucket_time,
                context, path,
                ATAN2(
                    AVG(SIN(CAST(value AS DOUBLE))),
                    AVG(COS(CAST(value AS DOUBLE)))
                ) as value_avg,
                NULL::DOUBLE as value_min,
                NULL::DOUBLE as value_max,
                COUNT(*) as sample_count,
                AVG(SIN(CAST(value AS DOUBLE))) as value_sin_avg,
                AVG(COS(CAST(value AS DOUBLE))) as value_cos_avg,
                MIN(received_timestamp) as first_timestamp,
                MAX(received_timestamp) as last_timestamp
            FROM read_parquet([{file_list}], union_by_name=true)
            WHERE value IS NOT NULL AND TRY_CAST(value AS DOUBLE) IS NOT NULL
            GROUP BY bucket_time, context, path
            ORDER BY bucket_time
        ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
        """

    return f"""
    COPY (
        SELECT
            time_bucket(INTERVAL '{interval_seconds} seconds', received_timestamp::TIMESTAMP) as bucket_time,
            context, path,
            AVG(CASE WHEN value IS NOT NULL AND TRY_CAST(value AS DOUBLE) IS NOT NULL THEN CAST(value AS DOUBLE) END) as value_avg,
            MIN(CASE WHEN value IS NOT NULL AND TRY_CAST(value AS DOUBLE) IS NOT NULL THEN CAST(value AS DOUBLE) END) as value_min,
            MAX(CASE WHEN value IS NOT NULL AND TRY_CAST(value AS DOUBLE) IS NOT NULL THEN CAST(value AS DOUBLE) END) as value_max,
            COUNT(*) as sample_count,
            MIN(received_timestamp) as first_timestamp,
            MAX(received_timestamp) as last_timestamp
        FROM read_parquet([{file_list}], union_by_name=true)
        GROUP BY bucket_time, context, path
        ORDER BY bucket_time
    ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
    """


def build_tier_to_tier_sql(files, interval_seconds, is_angular, output_file):
    """Build SQL to re-aggregate from one aggregated tier to the next."""
    file_list = ", ".join(f"'{f}'" for f in files)

    if is_angular:
        return f"""
        COPY (
            SELECT
                time_bucket(INTERVAL '{interval_seconds} seconds', src_bucket_time::TIMESTAMP) as bucket_time,
                context, path,
                ATAN2(
                    SUM(value_sin_avg * sample_count) / SUM(sample_count),
                    SUM(value_cos_avg * sample_count) / SUM(sample_count)
                ) as value_avg,
                NULL::DOUBLE as value_min,
                NULL::DOUBLE as value_max,
                SUM(sample_count)::BIGINT as sample_count,
                SUM(value_sin_avg * sample_count) / SUM(sample_count) as value_sin_avg,
                SUM(value_cos_avg * sample_count) / SUM(sample_count) as value_cos_avg,
                MIN(first_timestamp) as first_timestamp,
                MAX(last_timestamp) as last_timestamp
            FROM (
                SELECT bucket_time as src_bucket_time, context, path,
                       value_sin_avg, value_cos_avg, sample_count,
                       first_timestamp, last_timestamp
                FROM read_parquet([{file_list}], union_by_name=true)
            ) src
            GROUP BY time_bucket(INTERVAL '{interval_seconds} seconds', src_bucket_time::TIMESTAMP), context, path
            ORDER BY 1
        ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
        """

    return f"""
    COPY (
        SELECT
            time_bucket(INTERVAL '{interval_seconds} seconds', src_bucket_time::TIMESTAMP) as bucket_time,
            context, path,
            SUM(value_avg * sample_count) / SUM(sample_count) as value_avg,
            MIN(value_min) as value_min,
            MAX(value_max) as value_max,
            SUM(sample_count)::BIGINT as sample_count,
            MIN(first_timestamp) as first_timestamp,
            MAX(last_timestamp) as last_timestamp
        FROM (
            SELECT bucket_time as src_bucket_time, context, path,
                   value_avg, value_min, value_max, sample_count,
                   first_timestamp, last_timestamp
            FROM read_parquet([{file_list}], union_by_name=true)
        ) src
        GROUP BY time_bucket(INTERVAL '{interval_seconds} seconds', src_bucket_time::TIMESTAMP), context, path
        ORDER BY 1
    ) TO '{output_file}' (FORMAT PARQUET, COMPRESSION 'SNAPPY');
    """


def main():
    parser = argparse.ArgumentParser(description="Build aggregation tiers from raw parquet data")
    parser.add_argument("data_dir", help="Data directory (e.g. ~/.signalk/data)")
    parser.add_argument("--from", dest="from_date", help="Start date (YYYY-MM-DD)", default=None)
    parser.add_argument("--to", dest="to_date", help="End date (YYYY-MM-DD)", default=None)
    parser.add_argument("--dry-run", action="store_true", help="Show what would be done without writing")
    args = parser.parse_args()

    data_dir = os.path.expanduser(args.data_dir)
    if not os.path.isdir(data_dir):
        print(f"Data directory not found: {data_dir}")
        sys.exit(1)

    from_date = datetime.strptime(args.from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) if args.from_date else None
    to_date = datetime.strptime(args.to_date, "%Y-%m-%d").replace(tzinfo=timezone.utc) if args.to_date else None

    print(f"Scanning raw data in {data_dir}...")
    raw_entries = find_raw_dates(data_dir)
    print(f"Found {len(raw_entries)} raw context/path/date combinations")

    # Filter by date range
    if from_date or to_date:
        filtered = []
        for context, sk_path, year, doy, files in raw_entries:
            dt = date_from_year_day(year, doy)
            if from_date and dt < from_date:
                continue
            if to_date and dt > to_date:
                continue
            filtered.append((context, sk_path, year, doy, files))
        raw_entries = filtered
        print(f"After date filter: {len(raw_entries)} combinations")

    if not raw_entries:
        print("Nothing to aggregate.")
        return

    conn = duckdb.connect()

    total_files_created = 0
    total_errors = 0
    start_time = time.time()

    # Clean old broken tier files before rebuilding
    print(f"\nCleaning old tier files...")
    total_removed = 0
    for context, sk_path, year, doy, _ in raw_entries:
        for tier in ["5s", "60s", "1h"]:
            if args.dry_run:
                continue
            total_removed += clean_tier_dir(data_dir, tier, context, sk_path, year, doy)
    print(f"Removed {total_removed} old files")

    # Step 1: raw -> 5s
    print(f"\n{'='*60}")
    print(f"PASS 1: raw -> 5s ({len(raw_entries)} date/path combos)")
    print(f"{'='*60}")

    for i, (context, sk_path, year, doy, files) in enumerate(raw_entries):
        dt = date_from_year_day(year, doy)
        date_str = dt.strftime("%Y-%m-%d")
        is_angular = sk_path in ANGULAR_PATHS

        # Output path
        out_dir = os.path.join(data_dir, f"tier=5s", f"context={context}", f"path={sk_path}",
                               f"year={year}", f"day={doy:03d}")
        out_file = os.path.join(out_dir, f"signalk_data_{date_str}_aggregated.parquet")

        pct = (i+1) * 100 // len(raw_entries)
        label = f"[{i+1}/{len(raw_entries)} {pct}%] {date_str} {sk_path.replace('__', '.')}"
        if args.dry_run:
            print(f"  DRY: {label}")
            continue

        try:
            os.makedirs(out_dir, exist_ok=True)
            sql = build_raw_to_tier_sql(files, 5, is_angular, out_file)
            conn.execute(sql)
            cnt = conn.execute(f"SELECT count(*)::INTEGER FROM read_parquet('{out_file}')").fetchone()[0]
            total_files_created += 1
            print(f"  OK: {label} -> {cnt} rows")
        except Exception as e:
            total_errors += 1
            print(f"  ERR: {label} -> {e}")

    if args.dry_run:
        print("\nDry run complete.")
        return

    # Step 2: 5s -> 60s
    # Scan the 5s tier we just built
    print(f"\n{'='*60}")
    print(f"PASS 2: 5s -> 60s")
    print(f"{'='*60}")

    tier_5s_entries = []
    for context, sk_path, year, doy, _ in raw_entries:
        src_dir = os.path.join(data_dir, f"tier=5s", f"context={context}", f"path={sk_path}",
                               f"year={year}", f"day={doy:03d}")
        src_files = [f for f in glob.glob(os.path.join(src_dir, "*.parquet"))
                     if "/processed/" not in f and "/quarantine/" not in f]
        if src_files:
            tier_5s_entries.append((context, sk_path, year, doy, src_files))

    print(f"  Found {len(tier_5s_entries)} 5s entries to aggregate")

    for i, (context, sk_path, year, doy, files) in enumerate(tier_5s_entries):
        dt = date_from_year_day(year, doy)
        date_str = dt.strftime("%Y-%m-%d")
        is_angular = sk_path in ANGULAR_PATHS

        out_dir = os.path.join(data_dir, f"tier=60s", f"context={context}", f"path={sk_path}",
                               f"year={year}", f"day={doy:03d}")
        out_file = os.path.join(out_dir, f"signalk_data_{date_str}_aggregated.parquet")

        pct = (i+1) * 100 // len(tier_5s_entries)
        label = f"[{i+1}/{len(tier_5s_entries)} {pct}%] {date_str} {sk_path.replace('__', '.')}"
        try:
            os.makedirs(out_dir, exist_ok=True)
            sql = build_tier_to_tier_sql(files, 60, is_angular, out_file)
            conn.execute(sql)
            cnt = conn.execute(f"SELECT count(*)::INTEGER FROM read_parquet('{out_file}')").fetchone()[0]
            total_files_created += 1
            print(f"  OK: {label} -> {cnt} rows")
        except Exception as e:
            total_errors += 1
            print(f"  ERR: {label} -> {e}")

    # Step 3: 60s -> 1h
    print(f"\n{'='*60}")
    print(f"PASS 3: 60s -> 1h")
    print(f"{'='*60}")

    tier_60s_entries = []
    for context, sk_path, year, doy, _ in raw_entries:
        src_dir = os.path.join(data_dir, f"tier=60s", f"context={context}", f"path={sk_path}",
                               f"year={year}", f"day={doy:03d}")
        src_files = [f for f in glob.glob(os.path.join(src_dir, "*.parquet"))
                     if "/processed/" not in f and "/quarantine/" not in f]
        if src_files:
            tier_60s_entries.append((context, sk_path, year, doy, src_files))

    print(f"  Found {len(tier_60s_entries)} 60s entries to aggregate")

    for i, (context, sk_path, year, doy, files) in enumerate(tier_60s_entries):
        dt = date_from_year_day(year, doy)
        date_str = dt.strftime("%Y-%m-%d")
        is_angular = sk_path in ANGULAR_PATHS

        out_dir = os.path.join(data_dir, f"tier=1h", f"context={context}", f"path={sk_path}",
                               f"year={year}", f"day={doy:03d}")
        out_file = os.path.join(out_dir, f"signalk_data_{date_str}_aggregated.parquet")

        pct = (i+1) * 100 // len(tier_60s_entries)
        label = f"[{i+1}/{len(tier_60s_entries)} {pct}%] {date_str} {sk_path.replace('__', '.')}"
        try:
            os.makedirs(out_dir, exist_ok=True)
            sql = build_tier_to_tier_sql(files, 3600, is_angular, out_file)
            conn.execute(sql)
            cnt = conn.execute(f"SELECT count(*)::INTEGER FROM read_parquet('{out_file}')").fetchone()[0]
            total_files_created += 1
            print(f"  OK: {label} -> {cnt} rows")
        except Exception as e:
            total_errors += 1
            print(f"  ERR: {label} -> {e}")

    elapsed = time.time() - start_time
    print(f"\n{'='*60}")
    print(f"DONE: {total_files_created} files created, {total_errors} errors, {elapsed:.1f}s")
    print(f"{'='*60}")

    conn.close()


if __name__ == "__main__":
    main()
