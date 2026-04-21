# Database Doctor Deep Core

## Summary

Deepens the ClickHouse doctor core with structured integrity findings while
preserving the existing top-level report fields used by current callers.

## Implementation Notes

- Added a machine-serializable findings shape with `severity`, `code`,
  `summary`, and `remediation`.
- Kept the existing `DoctorReport` compatibility fields unchanged and introduced
  a flattened deep-report variant for richer consumers.
- Added deep ClickHouse-layer checks for:
  - expected views and materialized views with sane engine classes,
  - orphan `event_links`,
  - orphan `tool_io`,
  - normalized `events` missing backing `raw_events`,
  - inconsistent/impossible session time-range metadata,
  - search freshness drift where text-bearing `events` are missing from
    `search_documents`.
- Added focused unit tests for finding serialization and helper evaluation
  logic.

## Operational Impact

- No schema migration added.
- Existing doctor consumers can continue reading the legacy top-level fields.
- New consumers can key off stable finding codes instead of scraping human error
  strings.

## Validation

- `cargo fmt --all`
- `cargo test -p moraine-clickhouse --locked`
