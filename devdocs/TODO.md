# TODO

## Analyzer walks the legacy `vessels/` layout synchronously

`ClaudeAnalyzer.getEnhancedSchemaForClaude` (`src/claude-analyzer.ts`), after
listing the own vessel's paths, checks for `<dataDir>/vessels/` — the flat
layout from before hive partitioning — and for every other vessel directory
there calls `scanVesselPaths`, which recurses with `readdirSync`/`statSync`
plus one more `readdirSync` per directory. All synchronous: the server's event
loop is blocked for the whole walk, the same failure as `/api/paths`.

Not yet known:
- whether `vessels/` exists on brain or the boat (if not, the walk costs
  nothing);
- how often `getEnhancedSchemaForClaude` is called.

Related: the example query the analyzer gives Claude points at that old layout
(`read_parquet('data/vessels/*/navigation/position/*.parquet')`), not at
`tier=raw/context=…/path=…/year=/day=`, where data has been written since the
move to hive partitioning. The legacy layout is still written only by the
in-memory-buffer fallback (`saveBufferToParquet` in `src/data-handler.ts`),
which runs when SQLite fails to open.

Options: walk it with the async hive walker, or drop the legacy scan and
correct the example query if the flat layout is no longer in use.
