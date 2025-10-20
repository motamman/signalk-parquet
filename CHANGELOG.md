# Changelog

## [Unreleased]

### Fixed
- Fixed HistoryAPI failing to return data when parquet files don't have `value_json` column. The query now only selects `value_json` for paths that actually need it (like navigation.position), preventing "column not found" errors on numeric data paths like wind speed.
