## opencode ingest

- Added `opencode` as a first-class normalizer harness.
- Added `opencode_sqlite` source-format config support for OpenCode's local `opencode.db`.
- Added read-only SQLite dispatch that synthesizes canonical records from OpenCode `session`, `message`, and `part` rows.
- Normalizes OpenCode session metadata, message parts, reasoning, tool results, step-finish token usage, file snapshots, and compaction summaries.
- Added synthetic fixtures and focused ingest-core/config tests.

Validation intended:

- `cargo test -p moraine-config --locked`
- `cargo test -p moraine-ingest-core --test opencode_fixture --locked`
- `cargo test -p moraine-ingest-core --locked`
- `cargo fmt --all -- --check`
