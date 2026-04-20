# P09 — Policy Engine for Retrieval and Exports

**Priority:** P2  
**Effort:** L  
**Status:** Specification / ready for design review  
**Dependencies:** R09 (privacy encryption), R12 (retrieval controls), C10 (MCP resources), P02 (notes)

## Objective

Add explicit policy for what can be retrieved, exported, or shown to agents. Policy is declarative, auditable, and fail-closed for sensitive destinations. It applies consistently across MCP tools, monitor raw payload view, export, and Python client.

## Design Principles

1. **Policy is explicit and versioned.** Rules are stored in config and ClickHouse, not implicit in code. [src: ADR-011]
2. **Policy is fail-closed.** If a rule is ambiguous or a destination is unrecognized, default is deny. [src: ADR-010]
3. **Audit trail is complete.** Every deny decision is logged with rule ID, reason, and actor. [src: ADR-011]
4. **Dry-run mode must work.** Users can preview policy effects before enforcing them.

## Schema Design

### New Tables

```sql
-- Policy rules: declarative access control rules.
CREATE TABLE IF NOT EXISTS moraine.policy_rules (
  rule_id String,
  rule_version String,
  policy_name String,
  effect LowCardinality(String),       -- 'allow', 'deny', 'redact'
  priority Int32,                      -- lower number = higher priority
  source_names Array(String),          -- empty = any
  projects Array(String),              -- empty = any
  event_classes Array(String),         -- empty = any
  event_kinds Array(String),           -- empty = any
  privacy_statuses Array(String),      -- 'redacted', 'encrypted', 'clear'
  age_min_days UInt32,
  age_max_days UInt32,
  destinations Array(String),          -- 'mcp', 'monitor', 'export', 'python_client', 'backup'
  action LowCardinality(String),       -- 'retrieve', 'export', 'view_raw', 'view_redacted'
  description String,
  created_at DateTime64(3),
  updated_at DateTime64(3),
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
PARTITION BY tuple()
ORDER BY (priority, rule_id);

-- Policy audit log: every enforce/decision event.
CREATE TABLE IF NOT EXISTS moraine.policy_audit_log (
  decision_id String,
  rule_id String,
  decision LowCardinality(String),     -- 'allow', 'deny', 'redact'
  reason String,
  actor String,                        -- user, mcp_client_id, etc.
  destination LowCardinality(String),
  action LowCardinality(String),
  session_id String,
  event_uid String,
  source_name LowCardinality(String),
  project LowCardinality(String),
  event_class LowCardinality(String),
  privacy_status LowCardinality(String),
  event_age_days UInt32,
  decided_at DateTime64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(decided_at)
ORDER BY (decided_at, decision_id);
```

### Config Format

```toml
[[policy.rules]]
name = "deny-encrypted-in-mcp"
effect = "deny"
priority = 10
destinations = ["mcp"]
privacy_statuses = ["encrypted"]
action = "retrieve"
description = "Encrypted rows are never returned through MCP tools"

[[policy.rules]]
name = "redact-old-exports"
effect = "redact"
priority = 20
destinations = ["export"]
age_min_days = 365
event_classes = ["message"]
action = "export"
description = "Messages older than 1 year are redacted in exports"

[[policy.rules]]
name = "allow-all-local-monitor"
effect = "allow"
priority = 100
destinations = ["monitor"]
action = "view_raw"
description = "Local monitor can view raw payloads"
```

## API Sketches

### MCP Tools

Policy enforcement is transparent to MCP tools. The retrieval layer (`moraine-conversations`) evaluates policy before hydrating hits. MCP tools receive filtered results with a `policy_applied` notice in `_safety`:

```json
{
  "_safety": {
    ...,
    "policy": {
      "rules_evaluated": 3,
      "rows_denied": 5,
      "rows_redacted": 2,
      "notice": "Some results were filtered by policy. Use strict mode for minimal exposure."
    }
  }
}
```

### CLI Commands

```bash
moraine policy validate                 # check policy config for conflicts
moraine policy test --event-uid ... --destination mcp --action retrieve
moraine policy audit --from 2026-04-01 --to 2026-04-20
moraine policy explain --session ...    # show which rules apply to a session
```

### Monitor Endpoints

- `GET /api/policy/rules` — list active rules.
- `POST /api/policy/test` — dry-run policy decision.
- `GET /api/policy/audit` — audit log query.
- `GET /api/policy/explain?session_id=...` — show applicable rules.

## Data Flow

1. **Config Load:** `moraine` parses `[policy]` section at startup.
2. **Sync:** Policy rules are written to `policy_rules` table for audit consistency.
3. **Evaluate:** On every retrieval/export/view operation:
   a. Match row metadata against rules in priority order.
   b. First matching rule wins.
   c. If no rule matches, default is `deny` for sensitive actions, `allow` for non-sensitive.
4. **Log:** Write `policy_audit_log` row for every `deny` and `redact` decision.
5. **Report:** Monitor shows rule hit counts, deny rates, and recent audit events.

## Policy Decision Matrix

| Destination | Default (no rule) | Sensitive Actions |
|---|---|---|
| `mcp` | deny encrypted, allow clear/redacted | retrieve, view_raw |
| `monitor` | allow (local UI) | view_raw |
| `export` | deny encrypted, redact old by default | export |
| `python_client` | same as `mcp` | retrieve, export |
| `backup` | allow all (backup must be complete) | backup |

## Edge Cases & Mitigations

| Edge Case | Mitigation |
|---|---|
| Local single-user mode becomes cumbersome | Default rules are permissive for local monitor; only MCP and export have conservative defaults. |
| Policy must be fail-closed | Default deny for unrecognized destinations; explicit `allow` rule required. |
| Policies need dry-run | `moraine policy test` simulates decision without logging or side effects. |
| Encrypted rows and redacted rows need different semantics | `privacy_statuses` array distinguishes them; `effect = redact` vs `effect = deny`. |
| MCP bypass through another tool format | Policy evaluated at data layer (`moraine-conversations`), not at tool boundary. All tools share enforcement. |
| Rule conflicts | Lower `priority` wins. `validate` warns on overlapping rules with same priority. |

## Acceptance Contract

### Functional
- [ ] A `deny` rule for encrypted data in MCP prevents encrypted rows from appearing in `search` results.
- [ ] A `redact` rule for old exports replaces `text_content` with `[redacted by policy]` in export output.
- [ ] `moraine policy test` returns the same decision as real enforcement for a given row.
- [ ] Audit log contains every deny and redact decision with rule ID and reason.

### Operational
- [ ] Policy evaluation adds <5ms per result row.
- [ ] Audit log retention is configurable TTL (default 90 days).
- [ ] Policy config reloads without full restart (`moraine policy reload` or SIGHUP).

### Safety
- [ ] Policy engine itself is auditable: config file checksum stored in `policy_rules`.
- [ ] No raw payloads in audit log; only metadata and decision.
- [ ] Backup destination is exempt from redaction by default (backups must be restorable).

### Compatibility
- [ ] Existing behavior preserved when no policy config is present (defaults match current behavior).
- [ ] New MCP `policy` field in `_safety` is additive.

### Observability
- [ ] Monitor shows active rules, decision counts by rule, and recent denials.
- [ ] `moraine doctor` warns on policy config syntax errors or unreachable rules.

## PR Sequencing

1. `schema(policy): add policy_rules and policy_audit_log tables`  
   - SQL only.
2. `feat(policy): add policy engine core with rule evaluation and defaults`  
   - New crate `moraine-policy-core`; integrates with `moraine-conversations`.
3. `feat(cli): add policy validate, test, audit, and explain commands`  
   - CLI surface.
4. `feat(mcp): integrate policy enforcement into retrieval layer`  
   - Update `_safety` envelope; no tool schema changes.
5. `feat(monitor): add policy dashboard and audit log viewer`  
   - UI surface.
6. `feat(export): apply policy to export commands`  
   - C11 / P04 integration.
7. `test(policy): add policy conformance fixtures`  
   - Matrix of rule × destination × privacy_status; expected outcomes.

## Open Questions

1. **Policy language complexity:** TOML arrays are simple but limited. Is a DSL needed? Recommendation: start with TOML; evaluate DSL only if users need complex boolean logic.
2. **Per-user policies:** S01 (team mode) will need user-scoped rules. Design `policy_rules` with `user_id` column now (nullable for local mode).
3. **Policy and search index interaction:** Should denied rows be excluded at SQL level or post-filtered? Recommendation: post-filter for simplicity; pre-filter only if performance demands it.
