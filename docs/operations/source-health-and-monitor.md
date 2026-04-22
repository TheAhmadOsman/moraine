# Monitor and Source Health

## Purpose

Moraine exposes source health in two places: `moraine sources status` for operators at the CLI, and `GET /api/sources` for the monitor UI. Both surfaces use the same shared snapshot builder in `moraine-source-status`, so status labels, configured-source inventory, and count/error semantics do not drift between terminal output and the dashboard. [src: crates/moraine-source-status/src/lib.rs, apps/moraine/src/main.rs, crates/moraine-monitor-core/src/lib.rs]

Source health is intentionally source-centric rather than process-centric. `moraine status` answers whether the runtime stack is alive; source health answers whether each configured ingest source is contributing data, has checkpoint progress, or is showing parse/schema errors.

## CLI Surface

Use:

```bash
moraine sources status
moraine sources status --include-disabled
moraine --output json sources status --include-disabled
moraine sources drift
moraine sources drift --include-disabled
moraine --output json sources drift
```

The command reads `[[ingest.sources]]` from the resolved config and joins that configured inventory with ClickHouse state from:

- `ingest_checkpoints` for checkpoint count and latest checkpoint timestamp.
- `raw_events` for raw ingested row count.
- `events` for normalized canonical event count in file/drift diagnostics.
- `ingest_errors` for error count plus latest error timestamp, kind, and text.

The JSON output is the most stable integration surface for scripts. It returns:

```json
{
  "sources": [
    {
      "name": "opencode",
      "harness": "opencode",
      "format": "opencode_sqlite",
      "enabled": true,
      "glob": "~/.local/share/opencode/opencode.db",
      "watch_root": "~/.local/share/opencode",
      "status": "ok",
      "checkpoint_count": 1,
      "latest_checkpoint_at": "2026-04-20 10:15:00",
      "raw_event_count": 128,
      "ingest_error_count": 0,
      "latest_error_at": null,
      "latest_error_kind": null,
      "latest_error_text": null
    }
  ],
  "query_error": null
}
```

`--include-disabled` keeps disabled configured sources in the response. Without it, disabled sources are omitted from CLI output. The monitor always includes disabled sources because dashboard inventory should reflect all configured sources.

`moraine sources drift` is the deeper consistency check. It compares each configured source against local files matched by the source glob, checkpoint state, raw rows, normalized `events`, and ingest errors. The command is read-only and reports categories such as:

- `expected_idle`: no files currently match and no ingest state exists.
- `missing_on_disk`: ClickHouse still has source state for files that no longer exist locally.
- `unobserved_disk_files`: files match the glob but have no checkpoint/raw/canonical/error state yet.
- `checkpoint_only_files`: checkpoints exist without raw rows, canonical events, or errors.
- `raw_without_canonical`: raw rows exist but no normalized events were emitted.
- `canonical_without_raw`: normalized events exist without raw backing rows.
- `ingest_errors`: one or more ingest errors are recorded.

Zero-byte Kimi CLI `.jsonl` files are treated as intentionally skipped when they have no checkpoint, raw, canonical, or error state. This keeps widened Kimi globs such as `~/.kimi/sessions/**/*.jsonl` from reporting stale or unobserved drift for placeholder sidecars, while non-empty Kimi sidecars and empty files from other harnesses still surface normally.

The JSON shape is intended for automation. Human output summarizes source-level counts and then lists typed findings with example paths.

## Monitor API

The monitor backend exposes:

```http
GET /api/sources
GET /api/sources/:source
GET /api/sources/:source/files
```

Successful responses use HTTP 200 even when one of the ClickHouse table queries fails after the config was loaded. In that partial state, configured sources are still returned and `query_error` describes the first query failure that occurred. This is deliberate: a dashboard that can show configured sources plus an explicit partial-query warning is more useful than a blanket 503 with no inventory. [src: crates/moraine-monitor-core/src/lib.rs, crates/moraine-source-status/src/lib.rs]

Top-level response shape:

```json
{
  "ok": true,
  "sources": [],
  "query_error": null
}
```

The endpoint can still return a non-200 response for failures before partial querying is possible, such as invalid ClickHouse client construction from config.

`GET /api/sources/:source` returns the shared source summary for one configured source using the same status classifier and partial-query behavior as `GET /api/sources`. It also includes a runtime block built from `ingest_heartbeats` plus a small warning list that separates file-state issues from ingest-heartbeat lag and watcher degradation:

```json
{
  "ok": true,
  "source": {
    "name": "opencode",
    "harness": "opencode",
    "format": "opencode_sqlite",
    "enabled": true,
    "glob": "~/.local/share/opencode/opencode.db",
    "watch_root": "~/.local/share/opencode",
    "status": "warning",
    "checkpoint_count": 1,
    "latest_checkpoint_at": "2026-04-20 10:15:00",
    "latest_checkpoint_age_seconds": 12,
    "raw_event_count": 128,
    "ingest_error_count": 2,
    "latest_error_at": "2026-04-20 10:18:00",
    "latest_error_kind": "schema_drift",
    "latest_error_text": "missing field"
  },
  "runtime": {
    "latest_heartbeat_at": "2026-04-20 10:18:05",
    "latest_heartbeat_age_seconds": 4,
    "queue_depth": 0,
    "files_active": 1,
    "files_watched": 8,
    "append_to_visible_p50_ms": 20,
    "append_to_visible_p95_ms": 140,
    "watcher_backend": "native",
    "watcher_error_count": 0,
    "watcher_reset_count": 0,
    "watcher_last_reset_at": null,
    "heartbeat_cadence_seconds": 5.0,
    "reconcile_cadence_seconds": 30.0,
    "lag_indicator": "healthy"
  },
  "warnings": [
    {
      "kind": "file_state",
      "severity": "warning",
      "summary": "This source is ingesting data, but recent file processing also recorded ingest errors."
    }
  ],
  "query_error": null,
  "runtime_query_error": null
}
```

If one of the status-table reads fails, the monitor still returns the configured source plus the first `query_error` it saw instead of failing the entire drilldown. If the heartbeat read fails independently, the response keeps the source summary and sets `runtime_query_error` instead. This keeps partial failures localized to the degraded part of the detail view.

## Monitor UI

The Svelte monitor fetches `/api/sources` on the fast polling cadence alongside health and status. The `SourcesStrip` is intentionally compact: it shows one chip per configured source with the shared health status label and a query-warning chip when `query_error` is present. [src: web/monitor/src/App.svelte, web/monitor/src/lib/api/client.ts, web/monitor/src/lib/components/SourcesStrip.svelte, web/monitor/src/lib/types/api.ts]

The strip is not a file browser and does not replace the session explorer. Its job is early operational signal:

- A configured source is disabled.
- A source has no checkpoints or raw rows yet.
- A source has ingest errors.
- ClickHouse source-health queries are degraded.

Selecting a source opens the detail panel. The drilldown now keeps a compact source summary visible above the files/errors tabs so operators can still see status, harness, format, watch root, glob, counts, latest checkpoint/error metadata, heartbeat age, watcher backend/error/reset state, append-to-visible lag, and the configured reconcile cadence while they inspect per-file state or recent ingest failures. The summary is loaded from `GET /api/sources/:source`; the `GET /api/sources/:source/files` endpoint now adds actionable per-file runtime detail instead of only shallow checkpoint counts.

Each file row is additive and can now include:

- `on_disk` plus `modified_at` and `modified_age_seconds`, so a file that still has checkpoints/raw rows but no longer exists on disk is obvious.
- `checkpoint_updated_at` and `checkpoint_age_seconds`, so operators can tell when a file last advanced its checkpoint.
- `latest_raw_event_at` and `latest_raw_event_age_seconds`, so operators can see when the file last produced raw rows.
- `canonical_event_count`, so raw rows that failed to produce normalized events are visible without writing a custom SQL query.
- `latest_error_at`, `latest_error_age_seconds`, `latest_error_kind`, and `latest_error_text`, so recent parse/schema failures stay attached to the exact file that produced them.
- `issues`, a small additive classification list that can flag `missing_on_disk`, `stale`, `erroring`, `sqlite_wal_present`, and `sqlite_shm_present`.
- `stale_reason`, which explains the heuristic when a file appears behind its on-disk writes.
- `sqlite_wal_present` and `sqlite_shm_present` for `opencode_sqlite` sources only. These fields are intentionally heuristic: they report whether sibling `*.db-wal` and `*.db-shm` files are currently visible next to a base `.db` path, not whether every SQLite-based source format necessarily uses sidecars.

For Kimi CLI sources, zero-byte `.jsonl` files with no ingest state are left out of `stale` and `unobserved_disk_files` classification. They still appear in disk/glob counts and file listings, which makes the skip auditable without turning known-empty sidecars into warnings.

Partial failures stay localized to the affected query surface and are shown as `query_error` or `runtime_query_error` warnings in the panel instead of collapsing the whole view.

The warning chips in the detail panel are intentionally typed:

- `file_state`: the shared per-source status says the source has no data yet, or has ingest errors attached to its files/checkpoints.
- `ingest_heartbeat`: the ingest runtime heartbeat is missing, delayed, stale, or backed up with queue depth.
- `watcher`: the watcher backend reported errors or rescans/resets in heartbeat state.

## Status Semantics

The shared classifier emits five states:

| Status | Meaning |
|---|---|
| `disabled` | The source is configured but `enabled=false`. |
| `unknown` | The source is enabled but has no raw rows/checkpoints yet, or health queries were partial. |
| `ok` | The source has data and no recorded ingest errors in the queried state. |
| `warning` | The source has ingested data and also has one or more ingest errors. |
| `error` | The source has ingest errors but no raw rows. This usually means the source is configured and reachable enough to parse attempts, but no usable records are landing. |

The classifier deliberately does not mark any nonzero error count as fatal. Existing data plus errors is a warning, because historical or partially successful ingestion can still be useful while the latest error is being triaged. Error-only with no data is the stronger failure signal. [src: crates/moraine-source-status/src/lib.rs]

`query_error` is not a source-specific ingest error. It means the health snapshot could not read one of the ClickHouse tables cleanly. In that state, per-source counts may be incomplete and statuses intentionally degrade to `unknown` instead of guessing.

## Troubleshooting

When a source is `unknown`:

1. Confirm it is enabled in `~/.moraine/config.toml`.
2. Confirm `glob` matches existing files or databases.
3. Confirm `watch_root` exists and is inside the tree where writes occur.
4. Run `moraine logs ingest --lines 200` and look for watcher registration or parse messages.
5. Wait one reconcile interval if the source was just created or copied into place.

When a source is `warning`:

1. Inspect `latest_error_kind` and `latest_error_text`.
2. Open the source drilldown and sort mentally by the file-state badges: `missing`, `stale`, and `erroring` are the quickest path to the offending file.
3. Query `ingest_errors` by `source_name` for examples.
4. Compare raw source records against the relevant normalizer path in `crates/moraine-ingest-core/src/normalize.rs` or dispatcher path in `crates/moraine-ingest-core/src/dispatch.rs`.
5. Treat repeated new errors after an upstream tool upgrade as likely schema drift.

When a source is `error`:

1. Verify the source format matches the data: `jsonl`, `session_json`, or `opencode_sqlite`.
2. For OpenCode, confirm the configured path points at the base `.db`, not `.db-wal` or `.db-shm`.
3. In the drilldown, check whether the file is missing on disk, whether raw rows ever landed, and whether WAL/SHM siblings are still present.
4. For Hermes live sessions, use `format = "session_json"` because those files are rewritten snapshots, not append-only JSONL.
5. For Kimi, prefer the default `wire.jsonl` source unless intentionally indexing `context.jsonl`.

When `query_error` is present:

1. Run `moraine db doctor`.
2. Verify migrations have created `raw_events`, `ingest_errors`, and `ingest_checkpoints`.
3. Check ClickHouse availability and credentials in config.
4. Rerun `moraine --output json sources status` to see the same shared snapshot outside the monitor server.
5. Run `moraine --output json sources drift` if the source count surface looks healthy but file-level raw/canonical/checkpoint state appears inconsistent.

When `runtime_query_error` is present:

1. Query `ingest_heartbeats` directly to confirm the table exists and the latest row deserializes cleanly.
2. Check whether the deployment is still on a pre-watcher-metrics schema; the detail API falls back to legacy heartbeat columns, but a broader heartbeat query failure still surfaces here.
3. Confirm the ingest service is running long enough to emit heartbeats on the configured cadence.

## Source-Specific Notes

Codex and Claude Code are append-oriented JSONL sources and should usually show checkpoint and raw row growth shortly after a session writes.

Kimi CLI defaults to `~/.kimi/sessions/**/wire.jsonl`. Kimi `context.jsonl` can be indexed when explicitly configured, but many context rows do not carry real timestamps. Moraine assigns deterministic synthetic timestamps for untimestamped Kimi rows and downstream latency statistics skip those synthetic timestamps so append-to-visible metrics are not polluted. Operators may widen the Kimi glob to include sidecar JSONL files; empty sidecars are considered no-op files for drift purposes, but non-empty sidecars are expected to produce checkpoint/raw/event/error state like any other configured source file.

OpenCode uses `format = "opencode_sqlite"` and is read through a defensive read-only SQLite connection. The watcher maps `opencode.db-wal` and `opencode.db-shm` sibling notifications back to the configured base database path, so live WAL writes do not wait for the periodic reconcile loop. The dispatcher validates expected tables and includes `PRAGMA user_version` in schema drift errors.

Hermes live sessions use `format = "session_json"` because the files are rewritten atomically. The dispatcher compares message count against checkpoint state and emits only newly appeared messages. Hermes trajectory exports remain JSONL.

## Related Files

- Shared snapshot and classifier: `crates/moraine-source-status/src/lib.rs`
- CLI command and rendering: `apps/moraine/src/main.rs`
- Monitor endpoint: `crates/moraine-monitor-core/src/lib.rs`
- Monitor frontend wiring: `web/monitor/src/App.svelte`
- Source strip component: `web/monitor/src/lib/components/SourcesStrip.svelte`
- Ingest config defaults and validation: `crates/moraine-config/src/lib.rs`
