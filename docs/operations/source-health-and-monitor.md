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
```

The command reads `[[ingest.sources]]` from the resolved config and joins that configured inventory with ClickHouse state from:

- `ingest_checkpoints` for checkpoint count and latest checkpoint timestamp.
- `raw_events` for raw ingested row count.
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

## Monitor API

The monitor backend exposes:

```http
GET /api/sources
GET /api/sources/:source
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

`GET /api/sources/:source` returns the shared source summary for one configured source using the same status classifier and partial-query behavior as `GET /api/sources`:

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
    "raw_event_count": 128,
    "ingest_error_count": 2,
    "latest_error_at": "2026-04-20 10:18:00",
    "latest_error_kind": "schema_drift",
    "latest_error_text": "missing field"
  },
  "query_error": null
}
```

If one of the ClickHouse reads fails, the monitor still returns the configured source plus the first `query_error` it saw instead of failing the entire drilldown.

## Monitor UI

The Svelte monitor fetches `/api/sources` on the fast polling cadence alongside health and status. The `SourcesStrip` is intentionally compact: it shows one chip per configured source with the shared health status label and a query-warning chip when `query_error` is present. [src: web/monitor/src/App.svelte, web/monitor/src/lib/api/client.ts, web/monitor/src/lib/components/SourcesStrip.svelte, web/monitor/src/lib/types/api.ts]

The strip is not a file browser and does not replace the session explorer. Its job is early operational signal:

- A configured source is disabled.
- A source has no checkpoints or raw rows yet.
- A source has ingest errors.
- ClickHouse source-health queries are degraded.

Selecting a source opens the detail panel. The drilldown now keeps a compact source summary visible above the files/errors tabs so operators can still see status, harness, format, watch root, glob, counts, and latest checkpoint/error metadata while they inspect per-file state or recent ingest failures. The summary is loaded from `GET /api/sources/:source`; the existing `GET /api/sources/:source/files` and `GET /api/sources/:source/errors` endpoints remain focused on file and error detail. Partial failures stay localized to the affected query surface and are shown as `query_error` warnings in the panel instead of collapsing the whole view.

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
2. Query `ingest_errors` by `source_name` for examples.
3. Compare raw source records against the relevant normalizer path in `crates/moraine-ingest-core/src/normalize.rs` or dispatcher path in `crates/moraine-ingest-core/src/dispatch.rs`.
4. Treat repeated new errors after an upstream tool upgrade as likely schema drift.

When a source is `error`:

1. Verify the source format matches the data: `jsonl`, `session_json`, or `opencode_sqlite`.
2. For OpenCode, confirm the configured path points at the base `.db`, not `.db-wal` or `.db-shm`.
3. For Hermes live sessions, use `format = "session_json"` because those files are rewritten snapshots, not append-only JSONL.
4. For Kimi, prefer the default `wire.jsonl` source unless intentionally indexing `context.jsonl`.

When `query_error` is present:

1. Run `moraine db doctor`.
2. Verify migrations have created `raw_events`, `ingest_errors`, and `ingest_checkpoints`.
3. Check ClickHouse availability and credentials in config.
4. Rerun `moraine --output json sources status` to see the same shared snapshot outside the monitor server.

## Source-Specific Notes

Codex and Claude Code are append-oriented JSONL sources and should usually show checkpoint and raw row growth shortly after a session writes.

Kimi CLI defaults to `~/.kimi/sessions/**/wire.jsonl`. Kimi `context.jsonl` can be indexed when explicitly configured, but many context rows do not carry real timestamps. Moraine assigns deterministic synthetic timestamps for untimestamped Kimi rows and downstream latency statistics skip those synthetic timestamps so append-to-visible metrics are not polluted.

OpenCode uses `format = "opencode_sqlite"` and is read through a defensive read-only SQLite connection. The watcher maps `opencode.db-wal` and `opencode.db-shm` sibling notifications back to the configured base database path, so live WAL writes do not wait for the periodic reconcile loop. The dispatcher validates expected tables and includes `PRAGMA user_version` in schema drift errors.

Hermes live sessions use `format = "session_json"` because the files are rewritten atomically. The dispatcher compares message count against checkpoint state and emits only newly appeared messages. Hermes trajectory exports remain JSONL.

## Related Files

- Shared snapshot and classifier: `crates/moraine-source-status/src/lib.rs`
- CLI command and rendering: `apps/moraine/src/main.rs`
- Monitor endpoint: `crates/moraine-monitor-core/src/lib.rs`
- Monitor frontend wiring: `web/monitor/src/App.svelte`
- Source strip component: `web/monitor/src/lib/components/SourcesStrip.svelte`
- Ingest config defaults and validation: `crates/moraine-config/src/lib.rs`
