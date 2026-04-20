# S02 — Hosted or Remote-Server Mode

**Priority:** P3  
**Effort:** XL  
**Status:** Specification / ready for design review  
**Dependencies:** R09 (privacy encryption), R11 (MCP conformance), S01 (tenancy model)

## Objective

Support remote access with proper MCP authorization instead of assuming local stdio trust. HTTP transport hardening, OAuth 2.1 authorization, token audience validation, and an admin UI for keys, users, and scopes.

## Design Principles

1. **Remote mode is not local mode exposed on a port.** It requires explicit auth, TLS, rate limits, and audit logging. [src: ADR-010]
2. **MCP over HTTP/SSE is first-class.** The server supports stdio (local) and HTTP/SSE (remote) transports with the same tool surface but different auth models.
3. **Tokens are short-lived and scoped.** No long-lived API keys by default. Access tokens expire; refresh tokens are rotatable.
4. **Admin capabilities are separate from user capabilities.** Admin UI runs on a separate port or path with stricter auth.

## Schema Design

### New Tables

```sql
-- OAuth clients / MCP client registrations.
CREATE TABLE IF NOT EXISTS moraine.oauth_clients (
  client_id String,
  tenant_id String,
  client_name String,
  redirect_uris Array(String),
  grant_types Array(String),
  scopes Array(String),
  created_at DateTime64(3),
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
ORDER BY (client_id);

-- Access tokens (metadata only; actual tokens are JWTs or hashed).
CREATE TABLE IF NOT EXISTS moraine.access_tokens (
  token_jti String,
  client_id String,
  user_id String,
  tenant_id String,
  scopes Array(String),
  issued_at DateTime64(3),
  expires_at DateTime64(3),
  revoked_at DateTime64(3),
  event_version UInt64
)
ENGINE = ReplacingMergeTree(event_version)
ORDER BY (token_jti);

-- Audit log for auth events.
CREATE TABLE IF NOT EXISTS moraine.auth_audit_log (
  event_id String,
  event_kind LowCardinality(String),     -- 'login', 'token_issue', 'token_refresh', 'token_revoke', 'access_denied'
  client_id String,
  user_id String,
  tenant_id String,
  ip_address String,
  user_agent String,
  success UInt8,
  reason String,
  occurred_at DateTime64(3)
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(occurred_at)
ORDER BY (occurred_at, event_id);
```

## API Sketches

### MCP Transport

Local mode: JSON-RPC over stdio (current).  
Remote mode: JSON-RPC over HTTP POST or Server-Sent Events (SSE) per MCP 2025-11-25 spec.

**HTTP endpoint:** `POST /mcp/v1/message`  
**SSE endpoint:** `GET /mcp/v1/sse`  
**Headers:**
```
Authorization: Bearer <access_token>
X-MCP-Protocol-Version: 2025-11-25
```

### OAuth 2.1 Flow

1. **Authorization Code Flow:**
   - User clicks "Connect Moraine" in client.
   - Redirect to `/oauth/authorize?client_id=...&scope=...&state=...`.
   - User approves; redirect back with `code`.
   - Client exchanges `code` for `access_token` + `refresh_token`.

2. **Token Validation:**
   - Every MCP message validates `access_token` JWT signature, audience (`aud`), expiry, and scopes.
   - Scope `mcp:read` allows search/list/get tools.
   - Scope `mcp:write` allows note creation, summary promotion (future).

### CLI Commands

```bash
moraine server start --remote --tls-cert ... --tls-key ...
moraine server status                     # show transport and auth status
moraine server keys rotate                # rotate signing keys
moraine server clients register --name "Claude Desktop" --redirect-uri ...
moraine server clients list
moraine server tokens revoke <token_jti>
```

### Monitor Admin Endpoints

- `GET /admin/oauth/clients` — list clients.
- `POST /admin/oauth/clients` — register client.
- `GET /admin/oauth/tokens` — list active tokens.
- `POST /admin/oauth/tokens/:jti/revoke` — revoke token.
- `GET /admin/audit/auth` — auth audit log.
- `GET /admin/metrics` — Prometheus metrics (protected).

## Data Flow

1. **Startup:** `moraine server start --remote` loads TLS certs, initializes OAuth issuer.
2. **Auth:** Client requests authorization; user approves; tokens issued.
3. **MCP:** Client sends JSON-RPC over HTTP/SSE with Bearer token.
4. **Validate:** Server validates token, extracts `user_id`/`tenant_id`, checks scopes.
5. **Execute:** Tool handler runs with authenticated context; S01 tenancy filters apply.
6. **Audit:** Auth events written to `auth_audit_log`; access events to `policy_audit_log` (P09).

## Edge Cases & Mitigations

| Edge Case | Mitigation |
|---|---|
| Local stdio and remote HTTP need different auth models | Transport detected at connection time. Stdio skips OAuth; HTTP requires it. |
| HTTPS and localhost redirect rules | Support `https://localhost:*` and `http://localhost:*` for development; production requires HTTPS. |
| Session hijacking | Short-lived access tokens (15 min); refresh tokens bound to client_id + code_verifier (PKCE). |
| Confused-deputy risk | Tokens include `aud` claim restricting them to Moraine server; no token passthrough to other services. |
| Resource server metadata | `.well-known/oauth-authorization-server` endpoint publishes issuer, endpoints, and supported scopes. |

## Acceptance Contract

### Functional
- [ ] MCP over HTTP returns same tool results as stdio for identical requests.
- [ ] Unauthorized requests receive 401 with `WWW-Authenticate` header.
- [ ] Token with insufficient scope receives 403 with missing scopes listed.
- [ ] Token refresh works without re-authorization.

### Operational
- [ ] HTTP server handles 100 concurrent MCP connections without latency degradation.
- [ ] TLS configuration supports modern cipher suites; old TLS versions rejected.
- [ ] Signing key rotation does not invalidate in-flight requests (grace period with old key).

### Safety
- [ ] No long-lived tokens by default; max refresh token lifetime 30 days.
- [ ] Auth audit log records all token issuance and access denials.
- [ ] Admin endpoints require admin role (S01) and additional MFA if configured.

### Compatibility
- [ ] Local stdio mode is unchanged when remote mode is not enabled.
- [ ] MCP protocol version negotiation works over HTTP same as stdio.

### Observability
- [ ] Monitor admin panel shows active connections, token counts, and auth event rate.
- [ ] Prometheus metrics for `mcp_http_requests_total`, `mcp_http_request_duration_seconds`, `auth_events_total`.

## PR Sequencing

1. `feat(transport): add MCP HTTP/SSE transport layer`  
   - New crate `moraine-mcp-transport`; abstract over stdio and HTTP.
2. `feat(auth): add OAuth 2.1 authorization server`  
   - Authorization endpoint, token endpoint, PKCE support.
3. `feat(auth): add JWT token validation and scope enforcement`  
   - Middleware on HTTP transport; integrates with S01 tenancy.
4. `feat(cli): add server start, keys, and client management commands`  
   - CLI surface.
5. `feat(monitor): add admin UI for OAuth clients and tokens`  
   - Admin panel.
6. `test(auth): add OAuth conformance and security tests`  
   - Token lifecycle, scope enforcement, PKCE validation, replay resistance.
7. `docs(auth): publish remote server setup and client configuration guide`  
   - Operational docs.

## Open Questions

1. **Which OAuth library?** `oxide-auth` or custom implementation. Recommendation: `oxide-auth` for correctness, but evaluate maintenance status.
2. **SSE vs WebSocket for MCP:** MCP spec favors SSE. Implement SSE first; WebSocket is future extension.
3. **Should remote mode support serverless deployment?** Out of scope for P3 initial. Target long-running server first.
