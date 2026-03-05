#!/bin/bash
DATA_DIR="/home/maurice/.signalk/data/vessels"
DUCKDB="duckdb"

find "$DATA_DIR" -path '*/processed/signalk_data_*.parquet' -type f | \
  sed 's|/processed/signalk_data_\([0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}\)T.*|\t\1|' | \
  sort -u | \
while IFS=$'\t' read -r parent_dir date; do
  output_file="${parent_dir}/signalk_data_${date}_consolidated.parquet"
  glob_pattern="${parent_dir}/processed/signalk_data_${date}T*.parquet"

  if [ -f "$output_file" ]; then
    echo "SKIP: $output_file (already exists)"
    continue
  fi

  file_count=$(ls $glob_pattern 2>/dev/null | wc -l)
  if [ "$file_count" -eq 0 ]; then continue; fi

  echo "CONSOLIDATE: $file_count files -> $output_file"

  $DUCKDB -c "
    COPY (
      SELECT * FROM read_parquet('${glob_pattern}', union_by_name=true)
      ORDER BY signalk_timestamp
    ) TO '${output_file}' (FORMAT PARQUET, COMPRESSION SNAPPY);
  "

  if [ $? -eq 0 ]; then
    echo "  OK"
  else
    echo "  FAILED: $output_file"
    rm -f "$output_file"
  fi
done
