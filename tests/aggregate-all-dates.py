#!/usr/bin/env python3
"""
Aggregate all dates that have raw tier data but may be missing higher tiers.
Scans the hive directory for all year/day combinations with raw parquet files
and triggers the aggregation API for each date.

Usage:
    python3 tests/aggregate-all-dates.py [--host HOST] [--token TOKEN] [--year YEAR]
"""

import subprocess
import json
import datetime
import os
import sys
import argparse

def find_dates_with_raw_data(data_dir, year=None):
    """Find all dates that have raw parquet files."""
    dates = set()
    raw_dir = os.path.join(data_dir, 'tier=raw')

    if not os.path.exists(raw_dir):
        print(f'Error: raw tier directory not found at {raw_dir}')
        sys.exit(1)

    for root, dirs, files in os.walk(raw_dir):
        # Skip excluded directories
        if any(d in root for d in ['/processed/', '/quarantine/', '/failed/', '/repaired/']):
            continue

        parquet_files = [f for f in files if f.endswith('.parquet')]
        if not parquet_files:
            continue

        # Extract year and day from path
        if '/year=' in root and '/day=' in root:
            try:
                y = int(root.split('/year=')[1].split('/')[0])
                d = int(root.split('/day=')[1].split('/')[0])

                if year and y != year:
                    continue

                date = datetime.datetime(y, 1, 1) + datetime.timedelta(days=d - 1)
                dates.add(date)
            except (ValueError, IndexError):
                continue

    return sorted(dates)


def aggregate_date(host, token, date):
    """Trigger aggregation for a single date via the API."""
    date_str = date.strftime('%Y-%m-%d')
    result = subprocess.run(
        ['curl', '-s', '-X', 'POST', f'{host}/plugins/signalk-parquet/api/aggregate',
         '-H', f'Cookie: JAUTHENTICATION={token}',
         '-H', 'Content-Type: application/json',
         '-d', json.dumps({'date': date_str})],
        capture_output=True, text=True
    )

    try:
        r = json.loads(result.stdout)
        return r.get('success', False), r
    except json.JSONDecodeError:
        return False, {'error': result.stdout[:200]}


def main():
    parser = argparse.ArgumentParser(description='Aggregate all dates with raw data')
    parser.add_argument('--host', default='http://localhost:3000', help='SignalK server URL')
    parser.add_argument('--token', required=True, help='JWT authentication token')
    parser.add_argument('--year', type=int, default=None, help='Only process a specific year')
    parser.add_argument('--data-dir', default=os.path.expanduser('~/.signalk/data'),
                        help='Path to data directory')
    args = parser.parse_args()

    dates = find_dates_with_raw_data(args.data_dir, args.year)
    if not dates:
        print('No dates with raw data found.')
        return

    print(f'Found {len(dates)} dates with raw data')
    print(f'Range: {dates[0].strftime("%Y-%m-%d")} to {dates[-1].strftime("%Y-%m-%d")}')
    print()

    errors = 0
    for i, date in enumerate(dates):
        date_str = date.strftime('%Y-%m-%d')
        success, result = aggregate_date(args.host, args.token, date)

        if not success:
            errors += 1
            print(f'FAIL [{i+1}/{len(dates)}] {date_str}: {result}')
        else:
            tiers = result.get('results', [])
            records = sum(t.get('recordsAggregated', 0) for t in tiers)
            if records > 0:
                print(f'  OK [{i+1}/{len(dates)}] {date_str}: {records} records aggregated')
            else:
                print(f'SKIP [{i+1}/{len(dates)}] {date_str}: no records to aggregate')

    print()
    print(f'Done: {len(dates)} dates processed, {errors} errors')


if __name__ == '__main__':
    main()
