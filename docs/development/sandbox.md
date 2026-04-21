# Dev sandbox

Containerized per-worktree moraine stack for developer and agent testing.
Orchestrated by `scripts/dev/sandbox/moraine-sandbox`. Background: RFC #232.

## Purpose

- Isolate dev testing from the host's live moraine install. No shared
  ports, no shared `~/.moraine/`, no shared ClickHouse data dir.
- Build and run the **current worktree's binaries** against optionally-real
  host session archives (read-only).
- Give agents a full rust dev environment inside the container — cargo,
  rustc, rustup, sccache, native build deps — so they can iterate on the
  workspace without round-tripping through the host.
- One command up, one command down. Multi-sandbox friendly: two worktrees
  can run two sandboxes simultaneously with zero coordination.

## When to use this vs. `scripts/ci/e2e-stack.sh`

`scripts/ci/e2e-stack.sh` is the CI gate: non-interactive, temp-rooted,
synthetic `.jsonl` fixtures, asserts heartbeats / search rows / monitor
routes / MCP smoke, tears itself down. Run it before opening a PR for
changes to ingest, monitor, MCP, or schema.

The sandbox is the dev loop. It keeps the stack running for hand-poking,
builds the worktree's binaries *inside* the container (so linux-only
behavior gets exercised from a macOS host), and can optionally expose
the host's real `~/.codex/sessions` and `~/.claude/projects` read-only
so ingest exercises real-shaped data. Use it to reproduce bugs on real
corpus shape or iterate on UI/UX without round-tripping through the CI
script.

## Architecture

One shared runtime image, one container per sandbox, zero per-id image
tags.

### Image

One shared `moraine-sandbox-runtime:latest`, built from a single-stage
`FROM rust:1-bookworm` Dockerfile. Contains:

- Rust toolchain (cargo, rustc, rustup — pinned by the `rust:1-bookworm`
  tag, currently 1.95.x).
- sccache binary (linux-musl, matching `TARGETARCH`). Same version as
  the host's `sccache` — entries are wire-compatible.
- Native build dependencies: `pkg-config`, `libssl-dev`, `cmake`,
  `build-essential`.
- Runtime utilities: `tini` (PID 1), `curl`, `ca-certificates`, `tzdata`.
- A `moraine:moraine` (uid/gid 1000) non-root user.

Everything that varies per sandbox — worktree source, built binaries,
cargo target dir, cargo home, generated config, entrypoint.sh — arrives
via bind mount or named volume, so the image is built once on first
`up` and reused for every subsequent sandbox on the host.

### Volumes (per sandbox)

| Volume | Mount point | Purpose |
|---|---|---|
| `binaries` | `/opt/moraine/bin` | The four binaries produced by the boot-build. Survives container restart; cleared by `down`. |
| `cargo-home` | `/home/moraine/.cargo` | Cargo registry + git cache. |
| `cargo-target` | `/home/moraine/target` | `CARGO_TARGET_DIR`. Incremental across exec sessions. |
| `state` | `/home/moraine/.moraine` | moraine runtime state (ingest state, logs). |
| `clickhouse-data` | `/var/lib/clickhouse` | ClickHouse storage (on the sibling service). |

All are scoped to the compose project (`moraine-sandbox-<id>_<volume>`)
and removed on `down`.

### Bind mounts

| Host path | Container path | Mode | Notes |
|---|---|---|---|
| `$SANDBOX_REPO_ROOT` (active worktree) | `/repo` | ro | cargo reads from here; target dir is elsewhere |
| `$SCCACHE_DIR` (host, default `~/.cache/sccache`) | `/home/moraine/.cache/sccache` | rw | shared with host and all other sandboxes |
| `web/monitor/dist` | `/opt/moraine/web` | ro | built on the host (bun); currently not in-container |
| Generated config dir (`${XDG_CACHE_HOME:-$HOME/.cache}/moraine-sandbox/moraine-sandbox-<id>/` by default) | `/sandbox` | ro | `moraine.toml` + fixture dirs |
| `scripts/dev/sandbox/entrypoint.sh` | `/usr/local/bin/entrypoint.sh` | ro | so edits to the entrypoint don't require an image rebuild |

`--mount-host-sessions` layers on `compose.sessions.yaml` to also
bind-mount `~/.codex/sessions`, `~/.claude/projects`, and
`~/.hermes/sessions` read-only. The Hermes mount points the generated
config at `/host/hermes/sessions/session_*.json` with
`format = "session_json"` so the sandbox exercises the
rewrite-in-place live-session path against real host data.

The generated config root defaults to a user cache directory instead of
`/tmp`. This matters on macOS/Colima because the Docker daemon runs inside a
VM that does not necessarily see macOS `/tmp` bind mounts, even though it can
mount paths under the user home directory.

## Boot flow

```
moraine-sandbox up
        ▼
1. validate docker, compose v2, worktree root
2. generate sandbox id (sb-xxxxxx) unless --id
3. ensure monitor frontend is built (bun build on host)
4. pick random host ports (monitor :8080, CH :8123 / :9000)
5. write `moraine.toml` under the sandbox config root
6. export env, `docker compose up -d`
        ▼
7. inside moraine container, entrypoint.sh:
   a. wait for sibling ClickHouse health at http://clickhouse:8123
   b. register sentinel pid for managed-CH suppression
   c. if /opt/moraine/bin is empty OR SANDBOX_REBUILD=1:
        cd /repo && cargo build --workspace --locked
        install the four binaries into /opt/moraine/bin
      (else: reuse the volume's prior output)
   d. moraine up --config /sandbox/moraine.toml --no-backup-check
   e. tail logs (keeps PID 1 alive)
        ▼
8. host CLI waits on docker health (up to 900s); prints summary
```

The backup gate is bypassed only inside the sandbox. The sandbox database is a
new disposable ClickHouse volume that exists before migrations because the
sibling ClickHouse container creates it during startup. Host installs should
still create and verify backups before migrations or destructive operations.

The cargo build step is what makes the first boot slow. Subsequent
boots of the same sandbox id skip the build entirely; fresh sandboxes
from the same (or any) worktree hit the shared sccache and finish in
~30 s.

## Commands

### `up [--id <sb-xxxxxx>] [--rebuild] [--mount-host-sessions] [--quiet|-q]`

Bring up a sandbox.

- `--id <sb-xxxxxx>` — reuse a specific id. Must match `^sb-[a-f0-9]{6}$`.
  Useful when scripting or re-upping after `down`.
- `--rebuild` — force a fresh `cargo build` on boot even if the
  `binaries` volume already has output from a prior boot. Exported to
  the container as `SANDBOX_REBUILD=1`.
- `--mount-host-sessions` — layer on `compose.sessions.yaml` to mount
  `~/.codex/sessions`, `~/.claude/projects`, and `~/.hermes/sessions`
  read-only inside the container. Overridable with
  `$SANDBOX_CODEX_SESSIONS_DIR` / `$SANDBOX_CLAUDE_PROJECTS_DIR` /
  `$SANDBOX_HERMES_SESSIONS_DIR`. Without this flag, ingest sources
  point at empty fixture dirs under the generated config dir so you
  can drop in your own `.jsonl` or `session_*.json` files.
- `--quiet` / `-q` — redirect all progress output (log lines, docker
  compose build + up chatter, the summary block) to stderr, and emit
  only the sandbox id on stdout. Designed for scripting and agents:
  `id=$(moraine-sandbox up --quiet)`. This closes a footgun where
  piping `up` through `tail`/`head` truncated the `[sandbox] up: <id>`
  banner, leaving the caller with a running sandbox it could no longer
  identify.

### `shell [<id>]`

`docker exec -it -u moraine ... bash`. Agents live here. cargo,
rustc, rustup, sccache are all on `PATH`; `CARGO_TARGET_DIR` and
`CARGO_HOME` point at the per-sandbox volumes so builds are
incremental across exec sessions and shell exits.

If `<id>` is omitted and exactly one sandbox is running, it's
selected automatically.

### `logs [<id>] [-f]`

`docker compose logs`. Includes the bootstrap cargo build output on
first boot, which is the easiest way to watch a long compile.

### `status [<id>]`

Prints the summary block (URLs, config path) plus `docker compose ps`.

### `list`

One-line-per-sandbox table: id, status, monitor URL. Used internally
for id disambiguation.

### `down <id>` / `down --all`

`docker compose down -v --remove-orphans` for the project, plus removal of the
generated config directory. `--all` iterates over every sandbox owned by the
current Docker context and sweeps stray generated config dirs left by failed
ups, including the historical `/tmp/moraine-sandbox-*` location.

**Agents must `down` before reporting task complete.** Leftover
sandboxes leak ClickHouse data (not small) and can exhaust ports
across many iterations.

## sccache

The host's `$SCCACHE_DIR` (`~/.cache/sccache` by default) is
bind-mounted rw at `/home/moraine/.cache/sccache`. Inside the
container, `RUSTC_WRAPPER=sccache`, `SCCACHE_CACHE_SIZE=20G`, and the
sccache binary is pinned to the same version as the host's so cache
entries are wire-compatible.

sccache keys entries by `{compiler, target, flags, source hash}`, so:

- Host cargo (darwin target) and container cargo (linux-aarch64 or
  linux-amd64) write to the **same directory** with **different keys**.
  No collision, no cross-target reuse (physically impossible anyway),
  but they share one sccache quota.
- Two sandboxes from two worktrees with the same `Cargo.lock` state
  hit each other's cache entries perfectly.
- Two sandboxes from the same worktree skip the build entirely on the
  second boot (reuse the `binaries` volume) or get 100% sccache hits
  if the volume is cleared.

Set `SCCACHE_CACHE_SIZE` on the host to change the budget; the
container inherits `20G` from its env unless overridden.

## Config generation

`moraine-sandbox up` writes
`${XDG_CACHE_HOME:-$HOME/.cache}/moraine-sandbox/moraine-sandbox-<id>/moraine.toml`
by default.
It is regenerated on every `up` and never edited by hand. Keys:

- `[clickhouse] url = "http://clickhouse:8123"`, `database = "moraine"` —
  compose-DNS address of the sibling ClickHouse service.
- `[ingest]` — `backfill_on_start = true`,
  `reconcile_interval_seconds = 5.0`, `heartbeat_interval_seconds = 2.0`,
  `flush_interval_seconds = 0.5`,
  `state_dir = "/home/moraine/.moraine/ingestor"`.
- `[[ingest.sources]]` — either `host-codex` / `host-claude` pointing at
  `/host/...` (with `--mount-host-sessions`) or `fixture-codex` /
  `fixture-claude` pointing at `/sandbox/fixtures/...` (default).
- `[monitor] host = "0.0.0.0"`, `port = 8080`.
- `[runtime]` — `root_dir = "/home/moraine/.moraine"`, `service_bin_dir
  = "/opt/moraine/bin"`, `clickhouse_auto_install = false` (the sibling
  container owns CH), `clickhouse_version` copied from
  `config/moraine.toml`, `start_monitor_on_up = true`,
  `start_mcp_on_up = false`.

The canonical key set and defaults live in `config/moraine.toml`; the
sandbox's generated file is a minimal per-id specialization of it.

## ClickHouse topology

ClickHouse runs as a sibling compose service pinned to the version in
`config/moraine.toml` (`runtime.clickhouse_version`, e.g.
`v25.12.5.44-stable` → image tag `25.12.5.44`). It publishes three
random host ports (HTTP 8123, TCP 9000, interserver-offset) and exposes
`clickhouse` as the container DNS name inside the compose network.
moraine connects via `http://clickhouse:8123`; managed-CH auto-install
is explicitly suppressed in the generated config plus a sentinel
pidfile the entrypoint pre-registers.

## Ports

Each `up` picks fresh random host ports: one for the monitor, one for
ClickHouse HTTP, and an offset for ClickHouse TCP. Container-internal
ports are fixed (`8080`, `8123`, `9000`). Two worktrees can run two
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

- Everything lives in two places on the host: the generated config directory
  under `${XDG_CACHE_HOME:-$HOME/.cache}/moraine-sandbox/` by default and the
  project's named volumes.
- `moraine-sandbox down <id>` runs `docker compose down -v` for the
  project and `rm -rf` on the config dir.
- `moraine-sandbox down --all` does the same for every sandbox and
  sweeps stray generated config dirs left by failed ups.
- Agents **must** call `down` before reporting their task complete.

## Multi-sandbox

Each sandbox has its own random host ports, compose project name,
config dir, and named volumes, so two worktrees can each run
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
  `web/monitor/dist/` and re-run. (Monitor frontend is still built on
  the host; moving it in-container is a potential follow-up.)
- **`sandbox <id> did not become healthy`** — `moraine-sandbox logs
  <id> -f` to watch the build + startup. Usual causes: cargo build
  error (surface and resolve), sibling ClickHouse failed to start
  (rarer), or an unusually slow cold sccache compile (wait; health
  timeout is 900 s).
- **`cargo build` fails on a missing native dep** — the runtime image
  includes `pkg-config`, `libssl-dev`, `cmake`, `build-essential`. If a
  new crate needs something else, add it to the Dockerfile `apt-get
  install` block and rebuild the shared image:
  `docker build -f scripts/dev/sandbox/Dockerfile -t moraine-sandbox-runtime:latest scripts/dev/sandbox`.
- **Ports exhausted / bind errors** — `moraine-sandbox list` then
  `moraine-sandbox down --all` to clear stragglers. Port picking
  retries 500 times before giving up.
- **Stale binaries after a code change** — `moraine-sandbox up
  --rebuild`, or `moraine-sandbox shell` in and run `cargo build
  --workspace --locked` manually (the monitor/ingest services keep
  running against the old binaries until you restart them; for a
  clean swap, `down` and re-up).
