# Migration Guide: Legacy Scripts to `moraine`

This guide maps historical script commands to the `moraine` command contracts.

## Updating an existing install

`moraine update` has been removed. Updates are now handled by your package manager:

- `uv tool install moraine-cli` (recommended) — installs, and `uv tool upgrade moraine-cli` keeps it current. The PyPI distribution is named `moraine-cli`; the installed entrypoint is `moraine`.
- `cargo install --force --locked moraine` — for source installs.
- Re-run `scripts/install.sh` — for existing curl-based installs. The installer will overwrite the binaries in place.

Installs from prior versions leave a stale `${XDG_CONFIG_HOME:-~/.config}/moraine/install-receipt.json` file; it is no longer read or written and can be deleted at will.

## Command mapping

- `bin/start-clickhouse` -> `bin/moraine up --no-ingest`
- `bin/init-db` -> `bin/moraine db migrate`
- `bin/status` -> `bin/moraine status`
- `bin/stop-all` -> `bin/moraine down`

Legacy lifecycle aliases (`start-clickhouse`, `init-db`, `status`, `stop-all`) remain as fail-fast migration stubs. Service wrapper scripts are retired; use `bin/moraine run ingest|monitor|mcp` directly.

## Runtime changes

1. Runtime supervision is now in Rust (`moraine`), not shell scripts.
2. ClickHouse schema application is versioned and tracked via `schema_migrations`.
3. One shared config schema is used for all services (`config/moraine.toml`).
4. `moraine run ingest|monitor|mcp` resolves installed binaries first; source-tree fallback is opt-in via `MORAINE_SOURCE_TREE_MODE=1`.

## Recommended workflow

```bash
cd ~/src/moraine
cargo build --workspace
bin/moraine up
bin/moraine status
```

For DB checks:

```bash
bin/moraine backup create
bin/moraine up
bin/moraine db migrate
bin/moraine reindex --search-only --execute
bin/moraine db doctor
bin/moraine db doctor --deep
```

Three maintenance paths now have a conservative backup gate:

- `bin/moraine up`, when it would auto-apply pending migrations to an existing database
- `bin/moraine db migrate`
- `bin/moraine reindex --search-only --execute`

By default they look for a backup of the active ClickHouse database under `~/.moraine/backups/` that still passes the same verification logic as `moraine backup verify` and is no older than 24 hours. First boot with no database and no-op migration checks do not require a backup. That 24-hour freshness window is a documented heuristic, not proof that the backup is sufficient for every operator workflow. Pass `--no-backup-check` only when you are deliberately accepting that risk.

The ClickHouse doctor core now distinguishes between compatibility fields and
deeper integrity findings:

- Existing top-level status remains: reachability, database existence, applied vs
  pending migrations, missing tables, raw error strings, and additive ClickHouse
  version compatibility fields.
- `bin/moraine db doctor --deep` exposes deep integrity checks as structured findings with:
  `severity` (`ok` / `warning` / `error`), `code`, `summary`, and
  `remediation`.
- Compatibility is intentionally explicit rather than inferred: `25.12.x` is
  `supported`, `26.3.x` is `experimental`, other parsed lines are
  `unsupported`, and missing or unparseable version strings are `unknown`.
- Current deep checks are aimed at schema/corpus integrity from the ClickHouse
  layer: expected views/materialized views, orphan `event_links`, orphan
  `tool_io`, normalized events missing `raw_events`, inconsistent session time
  ranges, and search freshness drift relative to `events`.

This keeps older consumers compatible while giving JSON-oriented surfaces a
stable machine-readable way to explain what is wrong and how to fix it.
