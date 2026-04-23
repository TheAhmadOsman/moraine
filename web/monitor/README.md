# Moraine Monitor Web

Local Svelte + Vite frontend for the Moraine monitor UI.

Release packaging expects built assets at `web/monitor/dist`.

## Prerequisites

- `bun` on your `PATH`
- Playwright browser binaries for e2e tests:

```bash
cd web/monitor
bunx playwright install chromium
```

## Setup

```bash
cd web/monitor
bun install --frozen-lockfile
```

## Local Development

- Start Vite dev server:

```bash
bun run dev
```

- Build production assets:

```bash
bun run build
```

- Preview the production build locally:

```bash
bun run preview -- --host 127.0.0.1 --port 4173
```

Note: the app calls `/api/*` on the same origin. When running `bun run dev`, API calls fail unless you provide a same-origin proxy/backend.

Core runtime endpoints used by the dashboard:

| Endpoint | Consumer | Purpose |
|---|---|---|
| `/api/health` | `StatusStrip` | Monitor and ClickHouse reachability. |
| `/api/status` | `StatusStrip` | Table counts and ingest heartbeat status. |
| `/api/sources` | `SourcesStrip` | Configured ingest sources, health labels, counts, checkpoints, and latest errors. |
| `/api/analytics` | `AnalyticsPanel` | Time-series dashboard metrics. Loaded after initial render so status/sources paint first, with a manual load control in the panel. |
| `/api/sessions` | `SessionsPanel` | Session explorer summaries. Deferred by default in the UI, with explicit load/refresh, page-limit control, cursor-based next-page pagination, and server-side query/model/status/harness filtering because centralized corpora can make this query materially slower than the status shell. |
| `/api/sessions/:id` | `SessionDetail` | Session-detail turn pages. Detail now loads in bounded turn windows with explicit `detailMeta` pagination/truncation metadata instead of shipping the full turn list for pathological sessions. |

`/api/sources` can return `ok=true` with a `query_error` string when ClickHouse source-health queries are partial. The UI should render the configured source inventory plus a warning instead of treating this as a hard dashboard failure.

## Test Workflow

- Typecheck:

```bash
bun run typecheck
```

- Unit tests (Vitest):

```bash
bun run test
```

- Unit tests in watch mode:

```bash
bun run test:watch
```

- Playwright smoke e2e (local preview server + mocked API responses):

```bash
bun run test:e2e
```

- Playwright live e2e against a running Moraine monitor instance:

```bash
MONITOR_BASE_URL=http://127.0.0.1:8080 bun run test:e2e -- e2e/monitor.live.spec.ts
```

- Endpoint latency benchmark from the Moraine repo:

```bash
MONITOR_BASE_URL=http://127.0.0.1:8080 bun run bench:monitor
MONITOR_BASE_URL=http://127.0.0.1:8080 MONITOR_SESSION_ID=<session-id> bun run bench:monitor
```

Optional live-test assertions can be tuned with:

- `MORAINE_E2E_CODEX_KEYWORD`
- `MORAINE_E2E_CLAUDE_KEYWORD`
- `MORAINE_E2E_CODEX_TRACE_MARKER`
- `MORAINE_E2E_CLAUDE_TRACE_MARKER`
