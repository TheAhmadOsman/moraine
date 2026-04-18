#!/usr/bin/env bash
# Moraine dev sandbox — container entrypoint. See RFC #232.
#
# Run by tini as PID 1 child. Expects:
#   /opt/moraine/bin   — Linux moraine* binaries (ro, host bind mount)
#   /opt/moraine/web   — monitor dist assets       (ro, host bind mount)
#   /sandbox           — generated moraine.toml    (ro, host bind mount)
#   /home/moraine/.moraine — named volume for runtime state (rw)
# and that a sibling `clickhouse` service in the same compose project is up
# and healthy on http://${SANDBOX_CLICKHOUSE_HOST:-clickhouse}:${SANDBOX_CLICKHOUSE_PORT:-8123}.

set -euo pipefail

MORAINE_BIN=/opt/moraine/bin/moraine
MORAINE_CONFIG_PATH=/sandbox/moraine.toml
LOG_DIR=/home/moraine/.moraine/logs
RUN_DIR=/home/moraine/.moraine/run
CH_HOST="${SANDBOX_CLICKHOUSE_HOST:-clickhouse}"
CH_PORT="${SANDBOX_CLICKHOUSE_PORT:-8123}"
CH_URL="http://${CH_HOST}:${CH_PORT}"

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
    if [[ -n "${ch_sentinel_pid:-}" ]] && kill -0 "$ch_sentinel_pid" 2>/dev/null; then
        kill "$ch_sentinel_pid" 2>/dev/null || true
    fi
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

mkdir -p "$LOG_DIR" "$RUN_DIR"

# ---------------------------------------------------------------------------
# Wait for the compose-managed ClickHouse to be reachable
# ---------------------------------------------------------------------------

log "waiting for clickhouse at ${CH_URL}"
for attempt in $(seq 1 60); do
    if curl -fsS --max-time 2 "${CH_URL}/ping" >/dev/null 2>&1; then
        log "clickhouse reachable after ${attempt} attempt(s)"
        break
    fi
    if (( attempt == 60 )); then
        die "clickhouse did not become reachable at ${CH_URL} within 60s"
    fi
    sleep 1
done

# ---------------------------------------------------------------------------
# Fake the ClickHouse pid file so `moraine up` skips starting its own.
#
# moraine up treats a running pid at $runtime.root/run/clickhouse.pid as
# "already running" and short-circuits start_clickhouse(). We spawn a
# long-lived sentinel and register its pid, so moraine never attempts to
# download or exec clickhouse-server. The real ClickHouse lives in the
# sibling compose service; connectivity is through clickhouse.url in
# moraine.toml (set to http://clickhouse:8123 by the host CLI).
# ---------------------------------------------------------------------------

(sleep infinity) &
ch_sentinel_pid=$!
echo "$ch_sentinel_pid" > "$RUN_DIR/clickhouse.pid"
log "registered clickhouse sentinel pid ${ch_sentinel_pid} (real CH is in sibling container)"

# ---------------------------------------------------------------------------
# Bring the moraine services up
# ---------------------------------------------------------------------------

log "bringing up moraine stack against ${CH_URL}"
log "running: moraine up --config $MORAINE_CONFIG_PATH"
if ! "$MORAINE_BIN" up --config "$MORAINE_CONFIG_PATH"; then
    warn "'moraine up' failed"
    dump_logs
    exit 1
fi

log "moraine stack is up"
log "  monitor (container): http://127.0.0.1:8080"
log "  clickhouse (compose DNS): ${CH_URL}"
log "(the host CLI prints the matching host-published URLs)"

# ---------------------------------------------------------------------------
# Keep PID 1 alive and surface logs via `docker logs`
# ---------------------------------------------------------------------------

# Pre-create a placeholder so `tail -F` has at least one file to watch even if
# moraine hasn't flushed its first log yet.
: >>"$LOG_DIR/.sandbox-entrypoint.log"

tail -F "$LOG_DIR"/*.log &
tail_pid=$!

wait "$tail_pid"
