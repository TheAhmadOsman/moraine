#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
SANDBOX_CLI="${REPO_ROOT}/scripts/dev/sandbox/moraine-sandbox"
PYTHON_BIN="${PYTHON_BIN:-python3}"
MORAINE_BIN="${MORAINE_BIN:-/opt/moraine/bin/moraine}"

KEEP_SANDBOX=0
REBUILD=0
SANDBOX_ID=""
PROJECT=""
TMP_ROOT=""
CLICKHOUSE_URL=""
CONFIG_PATH=""
CONFIG_DIR=""
RUN_STAMP=""

log() { printf '[operational-e2e] %s\n' "$*" >&2; }
die() { printf '[operational-e2e] ERROR: %s\n' "$*" >&2; exit 1; }

usage() {
  cat >&2 <<EOF
Usage: operational-safety-e2e.sh [--keep] [--rebuild]

Boots a disposable moraine dev sandbox and validates the combined operational
safety path:

  - fixture ingest and source drift diagnostics
  - deep doctor
  - import sync manifest v2 writing/status
  - backup create/verify
  - restore execution into a staging database
  - search-only reindex execute and resume

Environment:
  PYTHON_BIN      Python interpreter for JSON assertions (default: python3)
  MORAINE_BIN    in-sandbox moraine binary (default: /opt/moraine/bin/moraine)
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --keep) KEEP_SANDBOX=1; shift ;;
    --rebuild) REBUILD=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) usage; die "unknown flag: $1" ;;
  esac
done

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "required command not found: $1"
}

cleanup() {
  local rc=$?
  if [[ "$rc" -ne 0 && -n "$PROJECT" ]]; then
    log "failure diagnostics for ${PROJECT}"
    docker logs --tail 160 "$PROJECT" >&2 2>/dev/null || true
    docker logs --tail 120 "${PROJECT}-clickhouse" >&2 2>/dev/null || true
  fi
  if [[ -n "$SANDBOX_ID" && "$KEEP_SANDBOX" -ne 1 ]]; then
    log "tearing down ${SANDBOX_ID}"
    "$SANDBOX_CLI" down "$SANDBOX_ID" >/dev/null 2>&1 || true
  elif [[ -n "$SANDBOX_ID" ]]; then
    log "leaving sandbox up: ${SANDBOX_ID}"
  fi
  if [[ -n "$TMP_ROOT" && "$KEEP_SANDBOX" -ne 1 ]]; then
    rm -rf "$TMP_ROOT"
  fi
  exit "$rc"
}
trap cleanup EXIT INT TERM

run_moraine() {
  docker exec --user moraine "$PROJECT" "$MORAINE_BIN" --config /sandbox/moraine.toml "$@"
}

run_moraine_with_fake_rsync() {
  docker exec \
    --user moraine \
    -e PATH="/home/moraine/smoke-bin:/opt/moraine/bin:/usr/local/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin" \
    "$PROJECT" \
    "$MORAINE_BIN" \
    --config /sandbox/moraine.toml \
    "$@"
}

wait_for_http_ok() {
  local url="$1"
  local timeout_seconds="${2:-120}"
  local started
  started="$(date +%s)"
  while true; do
    if curl -fsS --max-time 3 "$url" >/dev/null 2>&1; then
      return 0
    fi
    if (( $(date +%s) - started >= timeout_seconds )); then
      die "timed out waiting for ${url}"
    fi
    sleep 2
  done
}

wait_for_clickhouse_count() {
  local query="$1"
  local timeout_seconds="${2:-120}"
  local started
  started="$(date +%s)"
  while true; do
    local count
    count="$(curl -sS --max-time 5 "$CLICKHOUSE_URL" --data-binary "$query" 2>/dev/null | tr -d '[:space:]' || true)"
    if [[ "$count" =~ ^[0-9]+$ ]] && (( count > 0 )); then
      return 0
    fi
    if (( $(date +%s) - started >= timeout_seconds )); then
      die "timed out waiting for ClickHouse count > 0: ${query}"
    fi
    sleep 2
  done
}

json_check() {
  local kind="$1"
  local file="$2"
  "$PYTHON_BIN" - "$kind" "$file" <<'PY'
import json
import sys

kind, path = sys.argv[1], sys.argv[2]
with open(path, "r", encoding="utf-8") as fh:
    data = json.load(fh)

def fail(message):
    raise SystemExit(f"{kind}: {message}")

if kind == "doctor":
    if not data.get("clickhouse_healthy"):
        fail("ClickHouse is not healthy")
    if not data.get("database_exists"):
        fail("database does not exist")
    if data.get("pending_migrations"):
        fail(f"pending migrations: {data['pending_migrations']}")
    if data.get("missing_tables"):
        fail(f"missing tables: {data['missing_tables']}")
    if data.get("errors"):
        fail(f"errors: {data['errors']}")
    errors = [f for f in data.get("findings", []) if f.get("severity") == "error"]
    if errors:
        fail(f"error findings: {errors}")
elif kind == "drift":
    if data.get("query_error"):
        fail(f"query_error: {data['query_error']}")
    sources = data.get("sources", [])
    if not sources:
        fail("no source rows returned")
    partial = [
        (s.get("name"), f)
        for s in sources
        for f in s.get("findings", [])
        if f.get("kind") == "partial_query"
    ]
    if partial:
        fail(f"partial query findings: {partial}")
    active = [
        s for s in sources
        if s.get("raw_event_count", 0) > 0 and s.get("canonical_event_count", 0) > 0
    ]
    if not active:
        fail("no source has both raw and canonical events")
elif kind == "import-sync":
    manifest = data.get("manifest") or {}
    if not data.get("success"):
        fail("sync result success=false")
    if manifest.get("manifest_version") != 2:
        fail(f"unexpected manifest version: {manifest.get('manifest_version')}")
    if manifest.get("status") != "success":
        fail(f"unexpected manifest status: {manifest.get('status')}")
    if not manifest.get("last_success"):
        fail("missing last_success")
    if len(manifest.get("sources", [])) != 2:
        fail(f"expected two per-source manifest rows, got {len(manifest.get('sources', []))}")
elif kind == "import-status":
    profiles = data.get("profiles", [])
    matches = [p for p in profiles if p.get("name") == "sandbox-smoke"]
    if len(matches) != 1:
        fail(f"expected one sandbox-smoke profile, got {len(matches)}")
    last_sync = matches[0].get("last_sync")
    if not last_sync or last_sync.get("status") != "success":
        fail(f"unexpected last_sync: {last_sync}")
elif kind == "backup":
    if not data.get("backup_dir"):
        fail("missing backup_dir")
    manifest = data.get("manifest") or {}
    tables = manifest.get("tables", [])
    if not tables:
        fail("backup manifest has no tables")
    if sum(t.get("row_count", 0) for t in tables) == 0:
        fail("backup contains zero rows")
elif kind == "backup-verify":
    if not data.get("ok"):
        fail(f"backup verify failed: {data.get('errors')}")
elif kind == "restore":
    if not data.get("can_restore"):
        fail(f"restore cannot proceed: {data.get('blockers')}")
    restored = data.get("restored_tables", [])
    if not restored:
        fail("no restored tables reported")
    mismatches = [t for t in restored if t.get("expected_rows") != t.get("restored_rows")]
    if mismatches:
        fail(f"restore row mismatches: {mismatches}")
elif kind == "reindex-dry":
    if data.get("mode") != "dry_run":
        fail(f"unexpected mode: {data.get('mode')}")
    if data.get("target") != "search_only":
        fail(f"unexpected target: {data.get('target')}")
    if (data.get("projected") or {}).get("events", 0) == 0:
        fail("projected event count is zero")
elif kind == "reindex-exec":
    if data.get("mode") != "execute":
        fail(f"unexpected mode: {data.get('mode')}")
    if data.get("documents_inserted", 0) == 0:
        fail("execute inserted zero documents")
    if data.get("batches_completed", 0) == 0:
        fail("execute completed zero batches")
elif kind == "reindex-resume":
    if data.get("mode") != "execute" or not data.get("resume"):
        fail(f"unexpected resume snapshot: mode={data.get('mode')} resume={data.get('resume')}")
    if data.get("documents_inserted", 0) == 0:
        fail("resume snapshot lost inserted document count")
else:
    fail(f"unknown check kind: {kind}")
PY
}

write_fixture_files() {
  local codex_session_id="00000000-0000-4000-8000-${RUN_STAMP:0:12}"
  local claude_session_id="00000000-0000-4000-8001-${RUN_STAMP:0:12}"
  local codex_file="${CONFIG_DIR}/fixtures/codex/sessions/2026/04/22/session-${codex_session_id}.jsonl"
  local claude_file="${CONFIG_DIR}/fixtures/claude/projects/smoke/session-${claude_session_id}.jsonl"

  mkdir -p "$(dirname "$codex_file")" "$(dirname "$claude_file")"

  cat >"$codex_file" <<EOF
{"timestamp":"2026-04-22T12:00:00.000Z","type":"session_meta","payload":{"id":"${codex_session_id}"}}
{"timestamp":"2026-04-22T12:00:01.000Z","type":"turn_context","payload":{"turn_id":"1","model":"gpt-5.4-codex"}}
{"timestamp":"2026-04-22T12:00:02.000Z","type":"response_item","payload":{"type":"message","role":"user","id":"codex-user-${RUN_STAMP}","content":[{"type":"text","text":"operational safety codex prompt ${RUN_STAMP}"}],"phase":"completed"}}
{"timestamp":"2026-04-22T12:00:03.000Z","type":"response_item","payload":{"type":"message","role":"assistant","id":"codex-assistant-${RUN_STAMP}","content":[{"type":"text","text":"operational safety codex reply ${RUN_STAMP}"}],"phase":"completed"}}
EOF

  cat >"$claude_file" <<EOF
{"type":"user","sessionId":"${claude_session_id}","uuid":"claude-user-${RUN_STAMP}","timestamp":"2026-04-22T12:00:04.000Z","message":{"role":"user","content":[{"type":"text","text":"operational safety claude prompt ${RUN_STAMP}"}]}}
{"type":"assistant","sessionId":"${claude_session_id}","uuid":"claude-assistant-${RUN_STAMP}","parentUuid":"claude-user-${RUN_STAMP}","requestId":"req-${RUN_STAMP}","timestamp":"2026-04-22T12:00:05.000Z","message":{"model":"claude-opus-4-7-20260401","role":"assistant","usage":{"input_tokens":9,"output_tokens":5},"content":[{"type":"text","text":"operational safety claude reply ${RUN_STAMP}"}]}}
EOF
}

append_import_profile() {
  cat >>"$CONFIG_PATH" <<EOF

[imports.sandbox-smoke]
host = "example.invalid"
remote_paths = ["/remote/codex", "/remote/claude"]
local_mirror = "/home/moraine/.moraine/imports/sandbox-smoke-mirror"
cadence = "manual"
include_patterns = ["*.jsonl"]
exclude_patterns = ["tmp/**"]
EOF
}

install_fake_rsync() {
  docker exec --user root "$PROJECT" bash -lc 'mkdir -p /home/moraine/smoke-bin && cat >/home/moraine/smoke-bin/rsync <<'"'"'EOF'"'"'
#!/usr/bin/env bash
printf "rsync  version 3.2.7  protocol version 31\n"
printf "Number of files: 10\n"
printf "Number of regular files transferred: 4\n"
printf "Total transferred file size: 246 bytes\n"
exit 0
EOF
chmod +x /home/moraine/smoke-bin/rsync
chown -R moraine:moraine /home/moraine/smoke-bin'
}

main() {
  need_cmd docker
  need_cmd curl
  need_cmd "$PYTHON_BIN"
  [[ -x "$SANDBOX_CLI" ]] || die "sandbox CLI is not executable: ${SANDBOX_CLI}"

  TMP_ROOT="$(mktemp -d)"
  RUN_STAMP="$(date +%s)_$$_$RANDOM"
  RUN_STAMP="${RUN_STAMP//[^[:alnum:]_]/_}"

  local up_args=(--quiet)
  if [[ "$REBUILD" -eq 1 ]]; then
    up_args+=(--rebuild)
  fi

  log "booting sandbox"
  SANDBOX_ID="$("$SANDBOX_CLI" up "${up_args[@]}")"
  PROJECT="moraine-sandbox-${SANDBOX_ID}"

  local status
  status="$("$SANDBOX_CLI" status "$SANDBOX_ID")"
  CONFIG_PATH="$(printf '%s\n' "$status" | awk -F': ' '/^\[sandbox\] config:/{print $2; exit}')"
  CLICKHOUSE_URL="$(printf '%s\n' "$status" | awk -F': ' '/^\[sandbox\] clickhouse:/{print $2; exit}')"
  CONFIG_DIR="$(dirname "$CONFIG_PATH")"
  [[ -n "$CONFIG_PATH" && -f "$CONFIG_PATH" ]] || die "could not resolve sandbox config path"
  [[ -n "$CLICKHOUSE_URL" ]] || die "could not resolve sandbox ClickHouse URL"

  log "sandbox=${SANDBOX_ID}"
  log "config=${CONFIG_PATH}"
  log "clickhouse=${CLICKHOUSE_URL}"

  write_fixture_files
  append_import_profile
  install_fake_rsync

  log "waiting for monitor health"
  local monitor_url
  monitor_url="$(printf '%s\n' "$status" | awk -F': ' '/^\[sandbox\] monitor:/{print $2; exit}')"
  wait_for_http_ok "${monitor_url}/api/health" 120

  log "waiting for fixture ingest"
  wait_for_clickhouse_count "SELECT count() FROM moraine.raw_events WHERE source_name IN ('fixture-codex', 'fixture-claude')" 120
  wait_for_clickhouse_count "SELECT count() FROM moraine.events WHERE positionCaseInsensitiveUTF8(text_content, '${RUN_STAMP}') > 0" 120
  wait_for_clickhouse_count "SELECT count() FROM moraine.search_documents WHERE positionCaseInsensitiveUTF8(text_content, '${RUN_STAMP}') > 0" 120

  log "checking deep doctor"
  local doctor_json="${TMP_ROOT}/doctor.json"
  run_moraine --output json db doctor --deep >"$doctor_json"
  json_check doctor "$doctor_json"

  log "checking source drift"
  local drift_json="${TMP_ROOT}/drift.json"
  run_moraine --output json sources drift >"$drift_json"
  json_check drift "$drift_json"

  log "checking import sync manifest writing"
  local import_sync_json="${TMP_ROOT}/import-sync.json"
  run_moraine_with_fake_rsync --output json import sync sandbox-smoke --execute >"$import_sync_json"
  json_check import-sync "$import_sync_json"

  local import_status_json="${TMP_ROOT}/import-status.json"
  run_moraine --output json import status >"$import_status_json"
  json_check import-status "$import_status_json"

  log "checking backup create and verify"
  local backup_json="${TMP_ROOT}/backup.json"
  run_moraine --output json backup create >"$backup_json"
  json_check backup "$backup_json"

  local backup_dir
  backup_dir="$("$PYTHON_BIN" -c 'import json,sys; print(json.load(open(sys.argv[1]))["backup_dir"])' "$backup_json")"
  local backup_verify_json="${TMP_ROOT}/backup-verify.json"
  run_moraine --output json backup verify "$backup_dir" >"$backup_verify_json"
  json_check backup-verify "$backup_verify_json"

  log "checking restore execution into staging database"
  local restore_db="moraine_restore_${RUN_STAMP}"
  local restore_json="${TMP_ROOT}/restore.json"
  run_moraine --output json restore --input "$backup_dir" --target-database "$restore_db" --execute >"$restore_json"
  json_check restore "$restore_json"
  wait_for_clickhouse_count "SELECT count() FROM ${restore_db}.events" 60

  log "checking search reindex execute and resume"
  local reindex_dry_json="${TMP_ROOT}/reindex-dry.json"
  run_moraine --output json reindex --search-only --dry-run --batch-size 1 >"$reindex_dry_json"
  json_check reindex-dry "$reindex_dry_json"

  local reindex_exec_json="${TMP_ROOT}/reindex-exec.json"
  run_moraine --output json reindex --search-only --execute --batch-size 1 --reset-state >"$reindex_exec_json"
  json_check reindex-exec "$reindex_exec_json"

  local reindex_resume_json="${TMP_ROOT}/reindex-resume.json"
  run_moraine --output json reindex --search-only --execute --resume >"$reindex_resume_json"
  json_check reindex-resume "$reindex_resume_json"

  log "checking final deep doctor"
  run_moraine --output json db doctor --deep >"$doctor_json"
  json_check doctor "$doctor_json"

  log "operational safety smoke passed"
}

main "$@"
