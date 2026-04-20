# S04 — Desktop App or Browser Extension

**Priority:** P3  
**Effort:** XL  
**Status:** Specification / ready for design review  
**Dependencies:** C03 (config wizard), S02 (remote HTTP mode), P07 (monitor UX)

## Objective

Improve discoverability and capture, but keep core services independent. A desktop wrapper provides convenient access to monitor, stack status, notifications, and backup prompts. A browser extension captures web research sessions with explicit user consent.

## Design Principles

1. **App/extension adds convenience without becoming required infrastructure.** Core Moraine services run independently; the app is a client. [src: ADR-001]
2. **Browser capture is opt-in per session.** No passive scraping of all browser activity. [src: ADR-011]
3. **Desktop app is a thin wrapper.** It does not reimplement ingestion, search, or storage. It communicates via HTTP API to local moraine services.
4. **Signing and auto-update are required for distribution.** Untrusted binaries are a security risk. [src: R13]

## Architecture

### Desktop App

**Technology:** Tauri (Rust backend + web frontend) or similar lightweight wrapper.

**Responsibilities:**
- Show monitor UI in a native window.
- System tray icon with stack status (green/yellow/red).
- Desktop notifications for source stale, backup due, alerts (C14).
- Native menu bar: `moraine up`, `moraine down`, `Open Monitor`, `Settings`.
- Auto-launch on login (optional).
- Backup reminder dialogs.

**Communication:**
- HTTP to `127.0.0.1:8080` (monitor).
- WebSocket or polling for real-time status.

**Security:**
- App bundles are code-signed (R13).
- Auto-updater verifies signature before applying update.
- No privileged operations (no sudo required).

### Browser Extension

**Technology:** WebExtension Manifest V3 (Chrome, Firefox, Safari).

**Responsibilities:**
- Capture web research sessions: user clicks extension icon → "Start research session" → captures page title, URL, selected text, and optional screenshot.
- Writes captured data to local Moraine via HTTP API (S02 remote mode with local auth).
- Shows research session timeline in popup.
- Stops capture on explicit "End session" or tab close.

**Permissions:**
- `activeTab` only (not `tabs` or `<all_urls>`).
- `storage` for local settings.
- `host_permissions` limited to `http://127.0.0.1:8080/*`.

**Privacy:**
- No data leaves the browser without explicit user action.
- No cloud analytics or telemetry.
- Screenshot capture requires per-page consent.

## API Sketches

### Desktop App APIs

Uses existing monitor HTTP API plus:

- `GET /api/status/compact` — lightweight status for tray icon polling.
- `POST /api/notifications/dismiss` — dismiss desktop notification.

### Browser Extension APIs

New ingest endpoint for extension data:

```
POST /api/ingest/research-session
Authorization: Bearer <local_token>
Content-Type: application/json

{
  "session_id": "research_20260420_abc123",
  "title": "ClickHouse projections",
  "url": "https://clickhouse.com/blog/projections-secondary-indices",
  "captured_text": "...",
  "screenshot_path": "optional/local/path.png",
  "tags": ["research", "database"],
  "ended_at": "2026-04-20T17:09:00Z"
}
```

This endpoint writes to a special `research` source table or standard `events` with `harness = "browser-extension"`.

## Schema Design

No new canonical tables if browser data maps to standard `events`:

```sql
-- browser extension events reuse events table with harness = 'browser-extension'
-- payload_type = 'web_page', 'web_selection', 'web_screenshot'
```

Optional dedicated table for rich browser captures:

```sql
CREATE TABLE IF NOT EXISTS moraine.browser_captures (
  capture_uid String,
  session_id String,
  user_id String,
  url String,
  page_title String,
  selected_text String,
  screenshot_path String,              -- local file path, not embedded blob
  capture_at DateTime64(3),
  event_version UInt64
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(capture_at)
ORDER BY (session_id, capture_at);
```

## Edge Cases & Mitigations

| Edge Case | Mitigation |
|---|---|
| Browser content can contain secrets | Extension only captures explicit user selections or page metadata, not form inputs or cookies. |
| Extension permissions are high-risk | Minimal permission set (`activeTab` only); no content script injection on all pages. |
| Desktop packaging and auto-update need signing | Code-signing certificates for macOS/Windows; notarization for macOS. R13 provides signing infra. |
| App becomes required infrastructure | Clear messaging: app is optional; all features available via CLI and browser. |
| Browser capture without consent | Explicit "Start session" click required; visual indicator (badge icon) when recording. |

## Acceptance Contract

### Functional
- [ ] Desktop app launches monitor UI in native window.
- [ ] System tray shows stack status color (green = healthy, yellow = warning, red = error).
- [ ] Browser extension captures research session and makes it searchable in Moraine.
- [ ] Browser extension does not capture any data without explicit user initiation.

### Operational
- [ ] Desktop app bundle size < 50MB (monitor assets + Tauri runtime).
- [ ] Browser extension size < 2MB.
- [ ] Auto-updater checks for updates weekly; applies only with user consent.

### Safety
- [ ] Desktop app verifies update signatures (Ed25519 or code signing cert).
- [ ] Browser extension does not execute remote scripts or load external resources.
- [ ] Captured data respects local privacy policy (redaction applied at ingest time).

### Compatibility
- [ ] Desktop app supports macOS, Windows, and Linux.
- [ ] Browser extension supports Chrome, Firefox, Safari (MV3).

### Observability
- [ ] Desktop app logs to `~/.moraine/logs/desktop-app.log`.
- [ ] Monitor shows connected desktop apps and browser extension sessions.

## PR Sequencing

1. `feat(desktop): scaffold Tauri app with monitor embedding`  
   - `desktop/` directory; load `http://127.0.0.1:8080` in WebView.
2. `feat(desktop): add system tray, notifications, and auto-launch`  
   - Native integrations.
3. `feat(extension): scaffold WebExtension MV3 with capture popup`  
   - `browser-extension/` directory.
4. `feat(extension): add research session capture and HTTP ingest`  
   - Capture logic; POST to Moraine.
5. `feat(api): add /api/ingest/research-session endpoint`  
   - Backend support for extension data.
6. `feat(desktop): add auto-updater and code signing integration`  
   - R13 signing integration.
7. `test(desktop): add end-to-end desktop app tests`  
   - Playwright or WebDriver tests against Tauri build.

## Open Questions

1. **Tauri vs Electron:** Tauri is smaller and Rust-native; recommended. Electron is larger but more mature. Tauri chosen for consistency with Rust codebase.
2. **Safari extension:** Requires native app wrapper on macOS. Can share Tauri app bundle.
3. **Should desktop app bundle ClickHouse?** No — keep services separate. App checks if `moraine` services are running and prompts to start them.
