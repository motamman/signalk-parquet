module.exports = {
  require: ['tsx/cjs'],
  extension: ['ts'],
  spec: ['test/**/*.test.ts'],
  // Integration suites touch DuckDB and the filesystem; unit tests stay in the
  // millisecond range regardless of this ceiling.
  timeout: 10000,
};
