# Source Error Quarantine

## Summary

Added a local source-health quarantine mechanism for exact known-bad historical
ingest-error rows. This lets operators preserve corrupted source mirrors and
ClickHouse audit rows while preventing a small set of known historical parse
errors from keeping a source in warning state forever.

## Scope

- Source status and drift health classification.
- Source error detail APIs and CLI rendering.
- Monitor source error drilldown.
- Default config comments and operations documentation.

## What Changed

- Added optional `[[source_status.ignored_ingest_errors]]` config entries with:
  - `source_name`
  - `source_file`
  - `source_line_no`
  - `source_offset`
  - `error_kind`
  - `reason`
- Normalized `~` in ignored source paths during config loading.
- Excluded configured rows from actionable source error counts used by:
  - `moraine sources status`
  - `moraine sources drift`
  - monitor source health
  - file-level latest-error classification
- Kept ignored rows visible in `moraine sources errors` and monitor error
  drilldown with `ignored = true` and `ignore_reason`.
- Ordered source error detail views so configured ignores appear first,
  actionable errors appear next, and timestamp fallback diagnostics appear last.

## Operational Impact

The mechanism does not delete or mutate source files, raw rows, canonical rows,
or `ingest_errors` rows. It only changes how source health summarizes exact
configured error coordinates.

Use this only for stable historical corruption where preserving the mirror is
preferred over editing the upstream transcript. If a current source keeps
producing new parse errors, fix the source or normalizer instead of adding broad
ignore entries.

## Validation

- `cargo fmt --all -- --check`
- `cargo test -p moraine-config -p moraine-source-status -p moraine-monitor-core -p moraine --locked`
- `bun run build`
- `make docs-build`
- Dev sandbox monitor health and `/api/sources`
- Local `sources drift --include-disabled`
- Local `sources errors pc-claude --output json`
- MCP smoke test after reinstall

## Local Deployment Note

The local operator profile quarantines five historical `pc-claude`
`json_parse_error` rows. The source now reports `ok`, and the five rows remain
visible as ignored in error detail output.
