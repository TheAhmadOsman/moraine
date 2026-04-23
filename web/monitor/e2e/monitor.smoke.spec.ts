import { expect, test, type Page, type Route } from '@playwright/test';

const TOTAL_SESSIONS = 60;
const PAGE_SIZE = 25;

function analyticsFixture(range: string) {
  const rangeMap: Record<string, { label: string; bucket_seconds: number }> = {
    '15m': { label: 'Last 15m', bucket_seconds: 60 },
    '1h': { label: 'Last 1h', bucket_seconds: 300 },
    '6h': { label: 'Last 6h', bucket_seconds: 900 },
    '24h': { label: 'Last 24h', bucket_seconds: 3600 },
    '7d': { label: 'Last 7d', bucket_seconds: 21_600 },
    '30d': { label: 'Last 30d', bucket_seconds: 86_400 },
  };

  const picked = rangeMap[range] || rangeMap['6h'];

  return {
    ok: true,
    range: {
      key: range,
      label: picked.label,
      window_seconds: 21_600,
      bucket_seconds: picked.bucket_seconds,
      from_unix: 1_700_000_000,
      to_unix: 1_700_021_600,
    },
    series: {
      tokens: [
        { bucket_unix: 1_700_000_000, model: 'gpt-5.4', tokens: 1200 },
        { bucket_unix: 1_700_021_600, model: 'gpt-5.4', tokens: 900 },
      ],
      turns: [
        { bucket_unix: 1_700_000_000, model: 'gpt-5.4', turns: 9 },
        { bucket_unix: 1_700_021_600, model: 'gpt-5.4', turns: 8 },
      ],
      concurrent_sessions: [
        { bucket_unix: 1_700_000_000, concurrent_sessions: 3 },
        { bucket_unix: 1_700_021_600, concurrent_sessions: 4 },
      ],
    },
  };
}

function makeSummarySession(index: number) {
  const startedAt = 1_700_000_000_000 + index * 10_000;
  const endedAt = startedAt + 6_000;
  return {
    id: `session-${index.toString().padStart(3, '0')}`,
    title: `Session ${index.toString().padStart(3, '0')}`,
    previewText: `Prompt ${index}`,
    harness: {
      id: 'codex',
      label: 'codex',
      short: 'CO',
      hue: 150,
    },
    startedAt,
    endedAt,
    durationMs: endedAt - startedAt,
    status: 'completed',
    models: ['gpt-5.4'],
    turnCount: 100,
    turns: [],
    totalTokens: 12_000 + index,
    totalToolCalls: 6,
    tags: [],
    traceId: `trace-${index}`,
    hasDetail: false,
  };
}

function sessionsPage(cursor: string | null) {
  const page = cursor === 'page-2' ? 1 : 0;
  const start = page * PAGE_SIZE;
  const end = Math.min(start + PAGE_SIZE, TOTAL_SESSIONS);
  const sessions = [];
  for (let i = start; i < end; i += 1) {
    sessions.push(makeSummarySession(i + 1));
  }

  return {
    ok: true,
    sessions,
    meta: {
      requested_limit: PAGE_SIZE,
      effective_limit: PAGE_SIZE,
      loaded_count: sessions.length,
      has_more: end < TOTAL_SESSIONS,
      since_seconds: 2_592_000,
      next_cursor: end < TOTAL_SESSIONS ? 'page-2' : null,
    },
  };
}

function makeTurn(idx: number) {
  const startedAt = 1_700_000_000_000 + idx * 1_000;
  const endedAt = startedAt + 600;
  return {
    idx,
    model: 'gpt-5.4',
    startedAt,
    endedAt,
    durationMs: endedAt - startedAt,
    promptTokens: 100,
    completionTokens: 200,
    totalTokens: 300,
    toolCalls: 0,
    steps: [
      {
        kind: 'user',
        at: startedAt,
        text: `user turn ${idx + 1}`,
      },
      {
        kind: 'assistant',
        at: endedAt,
        text: `assistant turn ${idx + 1}`,
        tokens: 200,
      },
    ],
  };
}

function sessionDetail(sessionId: string, turnCursor: string | null) {
  const pageStart = turnCursor === 'older-0' ? 0 : 50;
  const pageEnd = pageStart + 50;
  const turns = [];
  for (let idx = pageStart; idx < pageEnd; idx += 1) {
    turns.push(makeTurn(idx));
  }

  return {
    ok: true,
    session: {
      id: sessionId,
      title: sessionId.replace('session-', 'Session '),
      previewText: `Prompt ${sessionId}`,
      harness: {
        id: 'codex',
        label: 'codex',
        short: 'CO',
        hue: 150,
      },
      startedAt: 1_700_000_000_000,
      endedAt: 1_700_000_100_000,
      durationMs: 100_000,
      status: 'completed',
      models: ['gpt-5.4'],
      turnCount: 100,
      turns,
      totalTokens: 30_000,
      totalToolCalls: 0,
      tags: [],
      traceId: `trace-${sessionId}`,
      hasDetail: true,
      detailMeta: {
        requestedTurnLimit: 50,
        loadedTurnCount: 50,
        totalTurnCount: 100,
        hasMoreTurns: pageStart > 0,
        hasPreviousTurns: pageEnd < 100,
        nextTurnCursor: pageStart > 0 ? 'older-0' : null,
        truncatedReason: 'detail paginated to 50 turns per page',
      },
    },
  };
}

async function installCoreRoutes(page: Page): Promise<void> {
  await page.route('**/api/health', async (route) => {
    await route.fulfill({
      json: {
        ok: true,
        url: 'http://127.0.0.1:8123',
        database: 'moraine',
        version: '25.1.2',
        ping_ms: 8.75,
        connections: { total: 16 },
      },
    });
  });

  await page.route('**/api/status', async (route) => {
    await route.fulfill({
      json: {
        ok: true,
        ingestor: {
          present: true,
          alive: true,
          age_seconds: 3,
          latest: {
            queue_depth: 0,
            files_active: 1,
            files_watched: 10,
          },
        },
      },
    });
  });

  await page.route('**/api/sources', async (route) => {
    await route.fulfill({
      json: {
        ok: true,
        sources: [],
        query_error: null,
      },
    });
  });
}

async function fulfillSessionsRoute(route: Route): Promise<void> {
  const requestUrl = new URL(route.request().url());
  const cursor = requestUrl.searchParams.get('cursor');
  await route.fulfill({ json: sessionsPage(cursor) });
}

test('deferred analytics and sessions load cleanly with the default 25 limit', async ({ page }) => {
  await page.addInitScript(() => {
    window.requestIdleCallback = ((cb: IdleRequestCallback) =>
      window.setTimeout(
        () =>
          cb({
            didTimeout: false,
            timeRemaining: () => 0,
          } as IdleDeadline),
        60_000,
      )) as typeof window.requestIdleCallback;
    window.cancelIdleCallback = ((id: number) => window.clearTimeout(id)) as typeof window.cancelIdleCallback;
  });

  await installCoreRoutes(page);
  await page.route('**/api/analytics?range=*', async (route) => {
    const requestUrl = new URL(route.request().url());
    const range = requestUrl.searchParams.get('range') || '6h';
    await route.fulfill({ json: analyticsFixture(range) });
  });
  await page.route('**/api/sessions*', fulfillSessionsRoute);

  await page.goto('/');

  await expect(page.getByRole('heading', { name: 'Moraine Monitor' })).toBeVisible();
  await expect(page.locator('#analyticsMeta')).toContainText('Analytics ready to load.');
  await expect(page.locator('#sessionsPanel')).toContainText('Load Sessions Now');
  await expect(page.locator('#sessionsPanel .mv-select')).toHaveValue('25');

  await page.getByRole('button', { name: 'Load Analytics' }).click();
  await expect(page.locator('#analyticsMeta')).toContainText('Last 6h');
  await expect(page.locator('#tokensChart')).toBeVisible();

  await page.getByRole('button', { name: 'Load Sessions Now' }).click();
  await expect.poll(async () => page.locator('.mv-card').count()).toBe(PAGE_SIZE);
  await expect(page.locator('#sessionsPanel')).toContainText('page limit 25');
  await expect(page.locator('#sessionsPanel')).toContainText('more available');
});

test('session pagination resets scroll position and detail paging stays responsive', async ({ page }) => {
  await page.emulateMedia({ reducedMotion: 'reduce' });
  await installCoreRoutes(page);
  await page.route('**/api/analytics?range=*', async (route) => {
    const requestUrl = new URL(route.request().url());
    const range = requestUrl.searchParams.get('range') || '6h';
    await route.fulfill({ json: analyticsFixture(range) });
  });
  await page.route('**/api/sessions/*', async (route) => {
    const requestUrl = new URL(route.request().url());
    const sessionId = requestUrl.pathname.split('/').pop() || 'session-001';
    const turnCursor = requestUrl.searchParams.get('turn_cursor');
    await route.fulfill({ json: sessionDetail(sessionId, turnCursor) });
  });
  await page.route('**/api/sessions*', fulfillSessionsRoute);

  await page.goto('/');
  const loadSessionsButton = page.getByRole('button', { name: 'Load Sessions Now' });
  if (await loadSessionsButton.isVisible().catch(() => false)) {
    await loadSessionsButton.click();
  }
  await expect.poll(async () => page.locator('.mv-card').count()).toBe(PAGE_SIZE);

  const list = page.locator('.mv-v1-list');
  await list.evaluate((node) => {
    node.scrollTop = 500;
  });
  await page.getByRole('button', { name: 'Next Page' }).click();
  await expect(page.locator('.mv-card').first()).toContainText('Session 050');
  await expect(list.evaluate((node) => node.scrollTop)).resolves.toBe(0);

  await list.evaluate((node) => {
    node.scrollTop = 480;
  });
  await page.getByRole('button', { name: 'Previous Page' }).click();
  await expect(page.locator('.mv-card').first()).toContainText('Session 025');
  await expect(list.evaluate((node) => node.scrollTop)).resolves.toBe(0);

  await page.locator('.mv-card').first().click();
  await expect(page.locator('.mv-sidepanel')).toBeVisible();
  await expect(page.locator('.mv-detail-page-meta')).toContainText('showing 50 of 100 turns');
  await expect(page.locator('.mv-sidepanel')).toContainText('user turn 51');

  await page.getByRole('button', { name: 'Older Turns' }).click();
  await expect(page.locator('.mv-sidepanel')).toContainText('user turn 1');
  await expect(page.locator('.mv-sidepanel')).not.toContainText('Loading…');

  await page.getByRole('button', { name: 'Newer Turns' }).click();
  await expect(page.locator('.mv-sidepanel')).toContainText('user turn 51');
  await expect(page.locator('.mv-sidepanel')).not.toContainText('Loading…');
});
