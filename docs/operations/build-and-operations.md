# Build and Operations

## Scope

This runbook describes local single-machine operation using the Rust workspace and `moraine` as the primary lifecycle interface.

## Build

```bash
cd ~/src/moraine
cargo build --workspace
```

This produces binaries for:

- `moraine`
- `moraine-ingest`
- `moraine-monitor`
- `moraine-mcp`

## Install Runtime Binaries

### uv tool install (recommended)

```bash
uv tool install moraine-cli
```

To upgrade: `uv tool upgrade moraine-cli`. To uninstall: `uv tool uninstall moraine-cli`.

The PyPI distribution is named `moraine-cli`; the installed entrypoint is `moraine`.

### Cargo install from source

```bash
git clone https://github.com/eric-tramel/moraine.git ~/src/moraine
cd ~/src/moraine
for crate in moraine moraine-ingest moraine-monitor moraine-mcp; do
  cargo install --path "apps/$crate" --locked
done
```

Or install directly from GitHub without cloning:

```bash
for bin in moraine moraine-ingest moraine-monitor moraine-mcp; do
  cargo install --git https://github.com/eric-tramel/moraine.git \
    --package "$bin" \
    --bin "$bin" \
    --locked
done
```

Upgrade via `cargo install --force --locked moraine` (and equivalents for the sibling binaries).

### Prebuilt release bundle (fallback)

```bash
curl -fsSL https://raw.githubusercontent.com/eric-tramel/moraine/main/scripts/install.sh \
  | bash
export PATH="$HOME/.local/bin:$PATH"
```

The installer fetches a full bundle (`moraine`, `moraine-ingest`, `moraine-monitor`, `moraine-mcp`) and overwrites binaries in place in a single bin directory.

Install directory precedence:

1. `MORAINE_INSTALL_DIR`
2. `XDG_BIN_HOME`
3. `$(dirname "$XDG_DATA_HOME")/bin`
4. `~/.local/bin`

Installer environment configuration:

- `MORAINE_INSTALL_REPO` (default `eric-tramel/moraine`)
- `MORAINE_INSTALL_VERSION` (default `latest`)
- `MORAINE_INSTALL_ASSET_BASE_URL` (requires `MORAINE_INSTALL_VERSION` to be a non-`latest` tag)
- `MORAINE_INSTALL_SKIP_CLICKHOUSE` (`1|true|yes|on` skips managed ClickHouse install)

To upgrade a curl-installed instance, re-run the installer.

## Publish prebuilt binaries

Tag-driven GitHub Actions release workflow:

1. Push a semantic tag (example: `v0.4.3`).
2. Workflow `.github/workflows/release-moraine.yml` builds:
   - `x86_64-unknown-linux-gnu`
   - `aarch64-unknown-linux-gnu`
   - `aarch64-apple-darwin`
3. Uploads `moraine-bundle-<target>.tar.gz` plus `moraine-bundle-<target>.sha256` to the tag release.

Each bundle includes `manifest.json` with target/version metadata, per-binary checksums, and build metadata.

Multiplatform functional CI (`.github/workflows/ci-functional.yml`) also packages per-target bundles and validates `scripts/install.sh` by installing from a local artifact server before running the stack + MCP smoke test.

## Config model

Use one shared config schema at `config/moraine.toml`.

Resolution precedence:

1. `--config <path>`
2. env override (`MORAINE_CONFIG`, plus `MORAINE_MCP_CONFIG` for MCP, `MORAINE_MONITOR_CONFIG` for monitor, `MORAINE_INGEST_CONFIG` for ingest)
3. `~/.moraine/config.toml` (if present)
4. `MORAINE_DEFAULT_CONFIG` fallback (if set and the referenced file exists) — packaging hook used by the `uv tool install moraine-cli` wheel to point at its bundled default
5. repo default `config/moraine.toml`

The monitor binary resolves its static asset directory (`web/monitor/dist`) in this order: `--static-dir`, then `MORAINE_MONITOR_DIST` (canonical) or `MORAINE_MONITOR_STATIC_DIR` (legacy alias), then install-dir relative, then source-tree fallback. The `uv tool install moraine-cli` wheel sets `MORAINE_MONITOR_DIST` in its exec shim so the bundled bytes are used without relying on path conventions.

## Start stack

```bash
cd ~/src/moraine
bin/moraine up
bin/moraine up --output rich
```

`moraine up` does the following:

1. Starts ClickHouse process.
2. Waits for DB health.
3. Applies versioned migrations through `moraine-clickhouse` (`schema_migrations` ledger).
4. Starts ingest and optional services from `runtime` config.

It auto-installs managed ClickHouse when missing and `runtime.clickhouse_auto_install=true`.

`moraine clickhouse status` reports managed install state, active binary source (`managed` vs `PATH`), installed version, and checksum state.

## DB lifecycle

```bash
cd ~/src/moraine
bin/moraine db migrate
bin/moraine db doctor
bin/moraine db doctor --output json
```

`db doctor` checks:

- ClickHouse health/version.
- Database existence.
- Applied vs pending migrations.
- Required table presence.

## Service entrypoints

```bash
cd ~/src/moraine
bin/moraine run ingest
bin/moraine run monitor
bin/moraine run mcp
bin/moraine run clickhouse
```

## Replay latency benchmark

Use the replay benchmark to measure current MCP `search` latency against recent worst-case telemetry:

```bash
python3 scripts/bench/replay_search_latency.py --config config/moraine.toml
```

For workload inspection only, run with `--dry-run`.
Detailed options, output fields, and troubleshooting are in `operations/replay-search-latency-benchmark.md`.

## Status, logs, shutdown

```bash
cd ~/src/moraine
bin/moraine status
bin/moraine --output rich --verbose status
bin/moraine --output json status
bin/moraine logs
bin/moraine --output plain logs ingest --lines 200
bin/moraine down
```

Status includes process state, DB health/schema checks, and latest ingest heartbeat metrics.
`bin/moraine logs clickhouse` reads ClickHouse's internal rotating log at
`~/.moraine/clickhouse/log/clickhouse-server.log`.

All subcommands support output control:

- `--output auto|rich|plain|json` (default `auto`, rich on TTY).
- `--verbose` for expanded diagnostics in rich/plain output.

## Source health

Use source health when the stack is alive but one configured source appears stale, empty, or noisy:

```bash
bin/moraine sources status
bin/moraine sources status --include-disabled
bin/moraine --output json sources status --include-disabled
```

The command returns all enabled configured sources by default, or all configured sources with `--include-disabled`. Each row includes harness, format, glob, watch root, status, checkpoint count, raw event count, ingest error count, and latest error metadata. Status classification is shared with the monitor `/api/sources` endpoint through `moraine-source-status`.

Status meanings:

- `disabled`: configured with `enabled=false`.
- `unknown`: enabled but no data/checkpoint yet, or source-health queries were partial.
- `ok`: data exists and no ingest errors are present.
- `warning`: data exists and ingest errors are present.
- `error`: ingest errors exist but no raw rows have landed.

Detailed operator guidance is in `source-health-and-monitor.md`.

## Privacy Redaction

Optional ingest-time privacy redaction is configured under `[privacy]` in the resolved config. It is disabled by default. When enabled, it runs after source-specific normalization and before ClickHouse writes, so it changes what future rows store in `raw_events.raw_json`, `events.text_content`, `events.payload_json`, and `tool_io` JSON payloads.

Use this layer for storage policy, not response policy. MCP `safety_mode` can reduce what a tool response exposes, but it cannot remove secrets that were already stored raw. Changing privacy config is not retroactive; historical rows require a backup, clean reindex or targeted rebuild, and search index refresh if `text_content_mode` changed.

Detailed policy guidance is in `privacy-and-redaction.md`.

## Monitor APIs

The monitor serves static web assets and JSON APIs on the configured monitor host/port. Key operational endpoints:

| Endpoint | Purpose |
|---|---|
| `/api/health` | Monitor and ClickHouse reachability. |
| `/api/status` | Table/process status and heartbeat-derived ingest state. |
| `/api/sources` | Configured ingest source health, counts, checkpoints, and latest errors. |
| `/api/analytics` | Dashboard time series. |
| `/api/sessions` | Session list for the monitor explorer. |
| `/api/tables` | Table inventory/debug surface. |

`/api/sources` returns partial data with `query_error` when one of the ClickHouse source-health table queries fails after config was loaded. It should not be treated as an all-or-nothing health gate.

## MCP contracts

`bin/moraine run mcp` exposes six tools: `search`, `search_conversations`, `list_sessions`, `get_session`, `get_session_events`, and `open`.

`tools/list` publishes strict input schemas (`additionalProperties: false`) and tool-specific output schemas. Runtime argument decoding is also strict, so unknown fields fail fast. Successful responses include a retrieval safety envelope: full responses add `_safety` to `structuredContent`, while prose responses start with a short untrusted-memory preamble. Use `safety_mode="strict"` when an agent should suppress payload JSON and low-information system event expansion.

See `mcp/agent-interface.md` for the full contract.

## Legacy scripts

Legacy lifecycle aliases remain as fail-fast migration stubs with a `moraine` replacement hint:

- `bin/start-clickhouse`
- `bin/init-db`
- `bin/status`
- `bin/stop-all`

Legacy wrappers remain only as fail-fast stubs:

- `bin/start-ingestor` -> `bin/moraine up`
- `bin/run-codex-mcp` -> `bin/moraine run mcp`
- `bin/moraine-monitor` -> `bin/moraine run monitor`

## Failure triage

1. If `up` fails before migration, run `bin/moraine db doctor` and inspect ClickHouse logs via `bin/moraine logs clickhouse`.
2. If ingest stalls, run `bin/moraine status` and confirm heartbeat recency/queue depth.
3. If one source is stale or noisy, run `bin/moraine sources status --include-disabled` and inspect `latest_error_*`.
4. If monitor APIs degrade, run `bin/moraine run monitor` in foreground for direct error output.
5. If MCP retrieval degrades, verify `search_*` tables in doctor output and rerun `db migrate`.
6. If MCP hosts reject tool calls, inspect `tools/list`; unknown input fields are intentionally rejected by strict schemas.

## Remote Import Profiles

Import profiles describe how agent session files from remote machines should be mirrored into a local directory that can be ingested as a source. The current CLI slice previews and validates these profiles; live rsync execution is reserved for the next implementation slice. Configure a profile under `[imports.<name>]` in `config.toml`:

```toml
[imports.vm503]
host = "vm503.local"
remote_paths = ["~/.codex/sessions", "~/.claude/projects"]
local_mirror = "~/.moraine/imports/vm503"
include_patterns = ["**/*.jsonl", "**/*.json"]
exclude_patterns = ["**/.git"]
cadence = "manual"
```

Commands:

- `moraine import sync <name>` — preview the profile and planned local mirror.
- `moraine import status` — show all profiles and their last sync manifest.

Future live sync will store manifests as JSON under `~/.moraine/imports/<name>.json`.

## Portable Archives

Preview and verify portable JSONL archive contracts. Live ClickHouse export/import is intentionally not wired in this slice.

Export:

```bash
moraine archive export --out-dir ./archive --since 7d
moraine archive export --out-dir ./archive --session-ids sess-1,sess-2 --raw
```

Import:

```bash
moraine archive import --input ./archive
```

Verify an archive without importing:

```bash
moraine archive verify ./archive
```

Each archive is expected to contain a `manifest.json` with schema version and row counts, plus `.jsonl` files per table (`events`, `event_links`, `tool_io`, and optionally `raw_events`). Passing `--execute` currently returns a clear not-implemented error instead of touching ClickHouse.

## Config Wizard and Validation

Auto-discover common agent session directories:

```bash
moraine config detect --json
```

Validate the current config for missing directories, unknown formats, and overlapping watch roots:

```bash
moraine config validate
```

Run an interactive wizard to add discovered sources to your config (with automatic backup):

```bash
moraine config wizard
```
