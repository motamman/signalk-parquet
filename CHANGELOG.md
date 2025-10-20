# Changelog

## [Unreleased]

### Fixed
- Reverted incorrect outputDirectory path resolution that was causing data to be written to `.signalk/plugin-config-data/signalk-parquet/data/` instead of the user-configured location. The plugin now correctly uses the outputDirectory value as specified in the configuration without additional path manipulation.

### Changed
- Removed automatic path resolution for relative outputDirectory values - paths are now used exactly as configured by the user
