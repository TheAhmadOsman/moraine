#!/usr/bin/env bash
# Moraine dev sandbox — container entrypoint. See RFC #232.
#
# Run by tini as PID 1 child. Expects the host CLI to have bind-mounted:
#   /opt/moraine/bin  — Linux moraine* binaries (ro)
#   /opt/moraine/web  — monitor dist assets    (ro)
#   /sandbox          — generated moraine.toml (ro)
# and to have declared a named volume on /home/moraine/.moraine for state.

set -euo pipefail

MORAINE_BIN=/opt/moraine/bin/moraine
MORAINE_CONFIG_PATH=/sandbox/moraine.toml
LOG_DIR=/home/moraine/.moraine/logs

log()  { printf '[sandbox] %s\n' "$*"; }
warn() { printf '[sandbox] WARN: %s\n' "$*" >&2; }
die()  { printf '[sandbox] ERROR: %s\n' "$*" >&2; exit 1; }

dump_logs() {
    if ! compgen -G "$LOG_DIR/*.log" >/dev/null 2>&1; then
        warn "no log files under $LOG_DIR to display"
        return 0
    fi
    warn "--- last 80 lines of $LOG_DIR/*.log ---"
    # shellcheck disable=SC2012
    tail -n 80 "$LOG_DIR"/*.log >&2 || true
    warn "--- end log dump ---"
}

graceful_shutdown() {
    log "signal received, running 'moraine down'"
    if [[ -n "${tail_pid:-}" ]] && kill -0 "$tail_pid" 2>/dev/null; then
        kill "$tail_pid" 2>/dev/null || true
    fi
    "$MORAINE_BIN" down --config "$MORAINE_CONFIG_PATH" || true
    exit 0
}

trap graceful_shutdown TERM INT

# ---------------------------------------------------------------------------
# Pre-flight checks
# ---------------------------------------------------------------------------

if [[ ! -x "$MORAINE_BIN" ]]; then
    die "moraine binary not found at $MORAINE_BIN — host CLI should bind-mount it"
fi

if [[ ! -f "$MORAINE_CONFIG_PATH" ]]; then
    die "sandbox config not found at $MORAINE_CONFIG_PATH — host CLI should generate and mount it"
fi

for helper in moraine-ingest moraine-monitor; do
    if [[ ! -x "/opt/moraine/bin/$helper" ]]; then
        die "$helper not found at /opt/moraine/bin/$helper — host CLI should bind-mount the full release dir"
    fi
done

if [[ ! -x "/opt/moraine/bin/moraine-mcp" ]]; then
    warn "moraine-mcp not found at /opt/moraine/bin/moraine-mcp (MCP is not started by default; ignoring)"
fi

if [[ ! -f "/opt/moraine/web/index.html" ]]; then
    warn "monitor assets not found at /opt/moraine/web/index.html — the monitor will not serve UI assets"
fi

mkdir -p "$LOG_DIR"

# ---------------------------------------------------------------------------
# Bring the stack up
# ---------------------------------------------------------------------------

log "bringing up moraine stack"

log "running: moraine clickhouse install --config $MORAINE_CONFIG_PATH"
if ! "$MORAINE_BIN" clickhouse install --config "$MORAINE_CONFIG_PATH"; then
    warn "'moraine clickhouse install' failed"
    dump_logs
    exit 1
fi

log "running: moraine up --config $MORAINE_CONFIG_PATH"
if ! "$MORAINE_BIN" up --config "$MORAINE_CONFIG_PATH"; then
    warn "'moraine up' failed"
    dump_logs
    exit 1
fi

log "moraine stack is up (container-internal addresses)"
log "  monitor          : http://127.0.0.1:8080"
log "  clickhouse http  : http://127.0.0.1:8123"
log "  clickhouse native: tcp://127.0.0.1:9000"
log "(the host CLI prints the matching host-published URLs)"

# ---------------------------------------------------------------------------
# Keep PID 1 alive and surface logs via `docker logs`
# ---------------------------------------------------------------------------

mkdir -p "$LOG_DIR"
# Pre-create a placeholder so `tail -F` has at least one file to watch even if
# moraine hasn't flushed its first log yet.
: >>"$LOG_DIR/.sandbox-entrypoint.log"

tail -F "$LOG_DIR"/*.log &
tail_pid=$!

wait "$tail_pid"
