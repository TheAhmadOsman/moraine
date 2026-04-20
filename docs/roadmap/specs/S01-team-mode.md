# S01 — Team and Multi-User Mode

**Priority:** P3  
**Effort:** XL  
**Status:** Specification / ready for design review  
**Dependencies:** R09 (privacy encryption), P09 (policy engine), S02 (auth primitives), R01 (backup/restore)

## Objective

Enable shared corpora only after local backup, privacy, policy, and auth are solid. Team mode adds tenancy, user identity, row ownership, and access control without weakening local-first defaults.

## Design Principles

1. **Team mode is opt-in, not default.** Single-user local mode remains the primary experience. Team mode requires explicit configuration and setup. [src: ADR-001]
2. **Row ownership is explicit.** Every canonical event, summary, note, and entity occurrence carries a `tenant_id` and `user_id`. [src: ADR-004]
3. **Access control is coarse-grained first.** Start with project-level and source-level permissions, not per-event ACLs. Fine-grained can come later.
4. **Encryption keys are per-user.** Even in team mode, one user's private memory is encrypted with their key and not readable by others.

## Schema Design

### New Tables

```sql
-- Tenants: isolated organizational units.
CREATE TABLE IF NOT EXISTS moraine.tenants (
  tenant_id String,
  tenant_name String,
  created_at DateTime64(3),
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
ORDER BY (tenant_id);

-- Users: identity within a tenant.
CREATE TABLE IF NOT EXISTS moraine.users (
  user_id String,
  tenant_id String,
  email String,
  display_name String,
  role LowCardinality(String),         -- 'admin', 'member', 'viewer'
  public_key String,                   -- for envelope encryption
  created_at DateTime64(3),
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
ORDER BY (tenant_id, user_id);

-- Project memberships: which users can access which projects.
CREATE TABLE IF NOT EXISTS moraine.project_memberships (
  project_key String,
  user_id String,
  tenant_id String,
  role LowCardinality(String),         -- 'owner', 'contributor', 'viewer'
  granted_at DateTime64(3),
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
ORDER BY (project_key, user_id);

-- Source permissions: which users can read which sources.
CREATE TABLE IF NOT EXISTS moraine.source_permissions (
  source_name LowCardinality(String),
  user_id String,
  tenant_id String,
  permission LowCardinality(String),   -- 'read', 'write', 'admin'
  granted_at DateTime64(3),
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
ORDER BY (source_name, user_id);
```

### Canonical Table Modifications

All canonical tables gain `tenant_id` and `user_id` columns:

```sql
ALTER TABLE moraine.events
  ADD COLUMN IF NOT EXISTS tenant_id String DEFAULT '';
ALTER TABLE moraine.events
  ADD COLUMN IF NOT EXISTS user_id String DEFAULT '';

-- Similar ALTERs for raw_events, event_links, tool_io, summaries, notes, entities, etc.
```

In single-user mode, `tenant_id` and `user_id` are empty strings (backward compatible). In team mode, they are populated.

## API Sketches

### Authentication

Team mode requires S02 auth primitives. Assume OAuth 2.1 or local JWT:

```
Authorization: Bearer <jwt>
X-Tenant-ID: <tenant_id>
```

### MCP Tools

MCP tools gain implicit `user_id` from the authenticated session context. No new arguments needed.

### CLI Commands

```bash
moraine team init --name "Acme Engineering"    # create tenant
moraine team invite --email bob@example.com --role member
moraine team members                           # list members
moraine team projects                          # list project memberships
moraine team sources                           # list source permissions
moraine team switch --tenant <tenant_id>       # switch active tenant
```

### Monitor Endpoints

- `POST /api/team/tenants` — create tenant.
- `GET /api/team/tenants/:id/members` — list members.
- `POST /api/team/tenants/:id/invite` — invite user.
- `GET /api/team/projects/:key/members` — project membership.
- `PUT /api/team/projects/:key/members/:user_id` — update role.

## Data Flow

1. **Auth:** User authenticates via OAuth/S02; JWT contains `user_id`, `tenant_id`, `roles`.
2. **Ingest:** Ingestor tags events with `tenant_id` and `user_id` from source config or auth context.
3. **Retrieve:** Query layer appends `tenant_id = ? AND (user_id = ? OR project_key IN (...))` to all queries.
4. **Policy:** P09 policy engine evaluates team-scoped rules (e.g., "members cannot export").
5. **Encrypt:** Per-user encrypted rows use the user's public key; only their private key can decrypt.

## Edge Cases & Mitigations

| Edge Case | Mitigation |
|---|---|
| Personal traces mixed with team traces | Sources are owned by a user or tenant. Personal sources retain `user_id` and are invisible to team members unless explicitly shared. |
| Right to delete | User deletion is soft-delete (anonymize rows) or hard-delete with admin approval. Audit log records deletion. |
| Per-user encryption keys | Key rotation supported; old rows re-encrypted async. Backup includes key metadata but not private keys. |
| MCP clients acting on behalf of different users | Each MCP connection authenticates separately; no shared stdio session between users in team mode. |
| Tenant isolation leak | Query layer ALWAYS appends tenant filter. Integration tests verify no cross-tenant reads. |

## Acceptance Contract

### Functional
- [ ] User A cannot read user B's personal sessions in team mode.
- [ ] Project members can read project-linked sessions.
- [ ] Admin can see aggregate counts but not decrypt member private rows.
- [ ] Single-user mode is unchanged when team config is absent.

### Operational
- [ ] Tenant filter adds <5ms to query latency (indexed column).
- [ ] Team mode backup includes tenant and user metadata.

### Safety
- [ ] Encryption keys never leave the user's machine (envelope encryption with server-held encrypted key blobs).
- [ ] Audit log records all access control decisions.
- [ ] Team mode cannot be enabled without first configuring privacy encryption (R09).

### Compatibility
- [ ] Empty `tenant_id`/`user_id` preserves single-user behavior.
- [ ] ALTER TABLE migrations are idempotent and backward compatible.

### Observability
- [ ] Monitor shows tenant member list, project permissions, and recent access logs.
- [ ] `moraine doctor` checks for orphaned rows (missing tenant or user).

## PR Sequencing

1. `schema(team): add tenant_id and user_id to all canonical tables`  
   - SQL ALTERs; default empty strings.
2. `feat(team): add tenants, users, project_memberships, source_permissions tables`  
   - Auth and membership backend.
3. `feat(auth): integrate JWT/OAuth authentication layer`  
   - S02 dependency.
4. `feat(retrieval): add tenant and user filtering to all queries`  
   - Query layer changes; integration tests for isolation.
5. `feat(cli): add team init, invite, switch commands`  
   - CLI surface.
6. `feat(monitor): add team admin panel`  
   - UI for members, projects, permissions.
7. `test(team): add cross-tenant isolation tests`  
   - Security-critical; must prove no data leakage.

## Open Questions

1. **Self-hosted vs managed auth:** Start with generic OAuth 2.1 provider support (GitHub, Google, Okta). No custom auth server.
2. **Row-level vs table-level tenancy:** Row-level (tenant_id on every row) is more flexible but slightly more overhead. Table-level (separate DB per tenant) is simpler but harder to manage. Recommendation: row-level.
3. **Should team mode support on-premise ClickHouse clusters?** Yes, but out of scope for initial implementation. Target single shared ClickHouse instance first.
