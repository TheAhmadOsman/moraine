# Dev sandbox

Containerized per-worktree moraine stack for developer and agent testing.
Orchestrated by `scripts/dev/sandbox/moraine-sandbox`. Background: RFC #232.

## Purpose

- Isolate dev testing from the host's live moraine install. No shared
  ports, no shared `~/.moraine/`, no shared ClickHouse data dir.
- Exercise the **current worktree's binaries** against optionally real
  host session archives, read-only.
- One command up, one command down. Multi-sandbox friendly: two worktrees
  can run two sandboxes simultaneously.

## When to use this vs. `scripts/ci/e2e-stack.sh`

`scripts/ci/e2e-stack.sh` is the CI gate: non-interactive, temp-rooted,
synthetic `.jsonl` fixtures, asserts heartbeats / search rows / monitor
routes / MCP smoke, tears itself down. Run it before opening a PR for
changes to ingest, monitor, MCP, or schema.

The sandbox is the dev loop. It keeps the stack running for hand-poking,
uses the worktree binaries, and can optionally expose the host's real
`~/.codex/sessions` and `~/.claude/projects` read-only so ingest
exercises real-shaped data. Use it to reproduce bugs on real corpus
shape or iterate on UI/UX without round-tripping through the CI script.

## Bringup

```bash
scripts/dev/sandbox/moraine-sandbox up [flags]
```

The `up` flow:

1. Validate docker + compose v2, and that the current directory is the
   moraine workspace root.
2. Generate a sandbox id (`sb-xxxxxx`) unless one is passed with `--id`.
3. Produce Linux binaries via the selected build strategy (see below)
   and bind-mount them at `/opt/moraine/bin`.
4. Rebuild `web/monitor/dist/` with `bun` if stale, bind-mount at
   `/opt/moraine/web`.
5. Pick random host ports for the monitor (`:8080` in-container) and
   the ClickHouse HTTP / TCP / interserver trio.
6. Write `/tmp/moraine-sandbox-<id>/moraine.toml` and mount it at
   `/sandbox`.
7. `docker compose build`, `docker compose up -d`, then wait up to 120s
   for the container healthcheck (monitor `/api/health`) to pass.
8. Print the summary block (monitor URL, ClickHouse URL, config path,
   teardown command).

### `up` flags

- `--id <sb-xxxxxx>` — reuse a specific id. Must match `^sb-[a-f0-9]{6}$`.
  Useful when re-upping after an iterative rebuild or when scripting.
- `--mount-host-sessions` — layer on `compose.sessions.yaml`, which
  bind-mounts `~/.codex/sessions` at `/host/codex/sessions:ro` and
  `~/.claude/projects` at `/host/claude/projects:ro`. Overridable with
  `SANDBOX_CODEX_SESSIONS_DIR` / `SANDBOX_CLAUDE_PROJECTS_DIR`. Without
  this flag, ingest sources are pointed at empty fixture dirs under the
  generated config dir so you can drop in your own `.jsonl` files.
- `--build-in-container` — build release binaries inside the Dockerfile
  `builder` stage (`FROM rust:1-bookworm`) and copy them out. Hermetic;
  slower; no host Rust toolchain required.
- `--use-host-binaries` — trust pre-built binaries at
  `target/debug` (Linux host) or `target/<linux-triple>/debug` (macOS
  host). Errors with a pointer to `cargo build --workspace --locked` or
  `cross build --workspace --locked --target <triple>` when missing.
- `--skip-build` — same as `--use-host-binaries` in spirit but quieter;
  just asserts the binary exists.

The three build-strategy flags are mutually exclusive.

## Build strategies

- **Linux default** — `cargo build --workspace --locked` on the host,
  mounted from `target/debug`. Fast; reuses your rustup/sccache state.
- **macOS default** — `cross build --workspace --locked --target
  <aarch64|x86_64>-unknown-linux-gnu`. If `cross` is not installed, the
  CLI errors with `On macOS, host builds require `cross` (cargo install
  cross). Alternatively re-run with --build-in-container.` Fix: install
  cross, or re-run with `--build-in-container`.
- **`--build-in-container`** — multi-stage Dockerfile `builder` stage.
  No host toolchain needed. Slow first run (rust image + cold cache).
- **`--use-host-binaries` / `--skip-build`** — trust caller-built
  binaries. On macOS these must be Linux ELFs from `cross`; native
  macOS binaries will not run. Missing binaries produce an error
  pointing at the expected path.

## What gets mounted

Always, from `compose.yaml`:

- `$SANDBOX_REPO_ROOT` → `/repo` (ro) — source visibility for debugging.
- `$SANDBOX_BIN_DIR` → `/opt/moraine/bin` (ro) — `moraine`,
  `moraine-ingest`, `moraine-monitor`, `moraine-mcp`.
- `$SANDBOX_WEB_DIR` → `/opt/moraine/web` (ro) — monitor frontend dist.
- `$SANDBOX_CONFIG_DIR` → `/sandbox` (ro) — generated `moraine.toml`
  plus, without `--mount-host-sessions`, empty fixture dirs.
- Named volume `state` → `/home/moraine/.moraine` (rw) — ClickHouse
  data, ingest state, logs. Project-prefixed, tied to `down -v`.

Optionally, from `compose.sessions.yaml` when `--mount-host-sessions`:

- `$SANDBOX_CODEX_SESSIONS_DIR` → `/host/codex/sessions` (ro).
- `$SANDBOX_CLAUDE_PROJECTS_DIR` → `/host/claude/projects` (ro).

## Config generation

`moraine-sandbox up` writes `/tmp/moraine-sandbox-<id>/moraine.toml`.
It is regenerated on every `up` and never edited by hand. Keys:

- `[clickhouse] url = "http://127.0.0.1:8123"`, `database = "moraine"` —
  container-local ClickHouse; the host sees it via the published port.
- `[ingest]` — `backfill_on_start = true`,
  `reconcile_interval_seconds = 5.0`, `heartbeat_interval_seconds = 2.0`,
  `flush_interval_seconds = 0.5`, `state_dir =
  "/home/moraine/.moraine/ingestor"`.
- `[[ingest.sources]]` — either `host-codex` / `host-claude` pointing at
  `/host/...` (with `--mount-host-sessions`) or `fixture-codex` /
  `fixture-claude` pointing at `/sandbox/fixtures/...` (default).
- `[monitor] host = "0.0.0.0"`, `port = 8080`.
- `[runtime]` — `root_dir = "/home/moraine/.moraine"`, `service_bin_dir
  = "/opt/moraine/bin"`, `managed_clickhouse_dir =
  "/home/moraine/.moraine/clickhouse"`, `clickhouse_auto_install = true`,
  `clickhouse_version` copied from `config/moraine.toml`,
  `start_monitor_on_up = true`, `start_mcp_on_up = false`.

The canonical key set and defaults live in `config/moraine.toml`; the
sandbox's generated file is a minimal per-id specialization of it.

## Ports

Each `up` picks fresh random host ports: one for the monitor, and a
ClickHouse HTTP port (plus offset TCP and interserver ports, so the
trio is contiguous and free). Container-internal ports are fixed
(`8080`, `8123`, `9000`). This is what lets two worktrees run two
sandboxes at once without negotiating.

## Host sessions mount — caveats

- Host mounts are **read-only by design**. The container cannot corrupt
  `~/.codex/sessions` or `~/.claude/projects`.
- Docker Desktop on macOS does not reliably propagate inotify across
  its VM boundary. Mitigations are baked into the generated config:
  `reconcile_interval_seconds = 5.0` gives a short polling loop for
  catchup, and `backfill_on_start = true` indexes anything present at
  `up` time. Belt-and-braces on Linux; load-bearing on macOS.

## Lifecycle / cleanup

- Everything lives in two places on the host: `/tmp/moraine-sandbox-<id>/`
  and the `moraine-sandbox-<id>_state` named volume (plus the project's
  container).
- `moraine-sandbox down <id>` runs `docker compose down -v` for the
  project and `rm -rf` on the config dir.
- `moraine-sandbox down --all` does the same for every sandbox prefixed
  `moraine-sandbox-` owned by the current Docker context, and sweeps
  stray `/tmp/moraine-sandbox-*` dirs left by failed ups.
- Agents **must** call `down` before reporting their task complete.
  Leftover sandboxes leak disk (ClickHouse data is not small) and can
  exhaust ports across many iterations.

## Multi-sandbox

Each sandbox has its own random host ports, compose project name,
config dir, and named volume, so two worktrees can each run
`moraine-sandbox up` with no coordination. `moraine-sandbox list`
shows everything currently running.

## Relationship to the prod install

The sandbox is dev-only. `install.sh` and PyPI remain the single
user-facing install path (issue #219). The sandbox image is never
tagged or pushed. If you find yourself wanting to "just run this for
real", stop — reach for `install.sh`, not a container.

## Troubleshooting

- **`'docker compose' plugin not available`** — install Docker Desktop
  (macOS) or the `docker-compose-plugin` package (Linux). Compose v1
  is not supported.
- **`monitor frontend needs a build but 'bun' is not installed`** —
  install bun (`curl -fsSL https://bun.sh/install | bash`) or prebuild
  `web/monitor/dist/` and re-run.
- **`--use-host-binaries: no binary at .../moraine`** on macOS — the
  sandbox needs Linux ELFs. Run `cross build --workspace --locked
  --target <triple>` and re-up, or drop the flag to use the default
  cross strategy.
- **`sandbox <id> did not become healthy`** — inspect with
  `moraine-sandbox logs <id>`. Usual causes: ClickHouse tarball
  download failed (container needs outbound HTTPS), mounted binaries
  are the wrong arch, or a stale `state` volume (run `down <id>` then
  re-up).
- **Ports exhausted / bind errors** — `moraine-sandbox list` then
  `down --all` to clear stragglers. Port picking retries 500 times.
