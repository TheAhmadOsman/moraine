## kimi-cli ingest

- Added `kimi-cli` as a first-class normalizer harness.
- Normalizes Kimi `wire.jsonl` records for session metadata, user turns, assistant text, thinking, tool calls/results, lifecycle progress, and token usage.
- Supports explicitly configured Kimi `context.jsonl` records with synthetic timestamps when records have no timestamp.
- Added synthetic fixtures and focused ingest-core tests.

Validation intended:

- `cargo test -p moraine-ingest-core --test kimi_cli_fixture --locked`
- `cargo test -p moraine-ingest-core --locked`
- `cargo fmt --all -- --check`
