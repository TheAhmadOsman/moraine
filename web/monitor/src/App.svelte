<script lang="ts">
  import { get } from 'svelte/store';
  import { onMount, tick } from 'svelte';
  import AnalyticsPanel from './lib/components/AnalyticsPanel.svelte';
  import SourceDetail from './lib/components/SourceDetail.svelte';
  import SourcesStrip from './lib/components/SourcesStrip.svelte';
  import StatusStrip from './lib/components/StatusStrip.svelte';
  import SessionsPanel from './lib/components/sessions/SessionsPanel.svelte';
  import TopBar from './lib/components/TopBar.svelte';
  import {
    fetchAnalytics,
    fetchHealth,
    fetchSources,
    fetchStatus,
    isRequestAbortError,
  } from './lib/api/client';
  import { fetchSessions } from './lib/api/sessions';
  import { FAST_POLL_INTERVAL_MS, SLOW_POLL_INTERVAL_MS } from './lib/constants';
  import { analyticsRangeStore } from './lib/state/monitor';
  import {
    filteredSessionsStore,
    sessionsErrorStore,
    sessionsFilterStore,
    sessionsLoadingStore,
    sessionsMetaStore,
    sessionsStore,
  } from './lib/state/sessions';
  import { initializeTheme, setTheme, themeStore } from './lib/state/theme';
  import type {
    AnalyticsRangeKey,
    AnalyticsResponse,
    HealthResponse,
    SourcesResponse,
    StatusResponse,
  } from './lib/types/api';
  import type { Harness, Session, SessionsFilter, SessionsSinceKey } from './lib/types/sessions';
  import type { ThemeMode } from './lib/types/ui';

  const SESSIONS_POLL_INTERVAL_MS = 30_000;
  type IdleHandle = number | ReturnType<typeof globalThis.setTimeout>;

  let healthData: HealthResponse | null = null;
  let healthError: string | null = null;

  let statusData: StatusResponse | null = null;
  let statusError: string | null = null;

  let analyticsPayload: AnalyticsResponse | null = null;
  let analyticsError: string | null = null;
  let analyticsDeferred = true;
  let analyticsLoading = false;

  let sourcesData: SourcesResponse | null = null;
  let sourcesError: string | null = null;
  let selectedSource: string | null = null;
  let sessionsDeferred = true;
  let sessionsLimit = 25;
  let sessionsSince: SessionsSinceKey = '30d';
  let sessionsCursor: string | null = null;
  let sessionsCursorHistory: Array<string | null> = [];
  let analyticsController: AbortController | null = null;
  let sessionsController: AbortController | null = null;
  let analyticsIdleHandle: IdleHandle | null = null;
  let sessionsIdleHandle: IdleHandle | null = null;

  $: sessions = $sessionsStore;
  $: filteredSessions = $filteredSessionsStore;
  $: sessionsFilter = $sessionsFilterStore;
  $: sessionsLoading = $sessionsLoadingStore;
  $: sessionsError = $sessionsErrorStore;
  $: sessionsMeta = $sessionsMetaStore;

  $: sessionModels = deriveModels(sessions);
  $: sessionHarnesses = deriveHarnesses(sessions);

  function deriveModels(list: Session[]): string[] {
    const set = new Set<string>();
    for (const s of list) {
      for (const m of s.models) set.add(m);
    }
    return [...set].sort();
  }

  function deriveHarnesses(list: Session[]): Harness[] {
    const map = new Map<string, Harness>();
    for (const s of list) {
      if (!map.has(s.harness.id)) map.set(s.harness.id, s.harness);
    }
    return [...map.values()].sort((a, b) => a.label.localeCompare(b.label));
  }

  function errorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
  }

  function clearAnalyticsController(): void {
    analyticsController?.abort();
    analyticsController = null;
  }

  function clearSessionsController(): void {
    sessionsController?.abort();
    sessionsController = null;
  }

  function cancelIdleHandle(handle: IdleHandle | null): void {
    if (handle === null || typeof window === 'undefined') {
      return;
    }
    if ('cancelIdleCallback' in window) {
      window.cancelIdleCallback(handle as number);
    } else {
      globalThis.clearTimeout(handle);
    }
  }

  function scrollSessionsToTop(): void {
    if (typeof document === 'undefined') {
      return;
    }
    const panel = document.getElementById('sessionsPanel');
    if (!panel) {
      return;
    }
    const prefersReducedMotion =
      typeof window !== 'undefined' &&
      typeof window.matchMedia === 'function' &&
      window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    const behavior: ScrollBehavior = prefersReducedMotion ? 'auto' : 'smooth';
    const list = panel.querySelector<HTMLElement>('.mv-v1-list');
    if (list) {
      list.scrollTo({ top: 0, behavior });
    }
    panel.scrollIntoView({
      block: 'start',
      behavior,
    });
  }

  async function loadHealth(): Promise<void> {
    try {
      healthData = await fetchHealth();
      healthError = null;
    } catch (error) {
      healthError = errorMessage(error);
      healthData = null;
    }
  }

  async function loadStatus(): Promise<void> {
    try {
      statusData = await fetchStatus();
      statusError = null;
    } catch (error) {
      statusError = errorMessage(error);
      statusData = null;
    }
  }

  async function loadAnalytics(): Promise<void> {
    clearAnalyticsController();
    analyticsController = new AbortController();
    const signal = analyticsController.signal;
    analyticsLoading = true;
    analyticsError = null;
    try {
      analyticsPayload = await fetchAnalytics(get(analyticsRangeStore), signal);
      if (signal.aborted) {
        return;
      }
    } catch (error) {
      if (isRequestAbortError(error)) {
        return;
      }
      analyticsError = `Analytics unavailable: ${errorMessage(error)}`;
    } finally {
      if (analyticsController?.signal === signal) {
        analyticsController = null;
      }
      if (!signal.aborted) {
        analyticsLoading = false;
        analyticsDeferred = false;
      }
    }
  }

  async function loadSources(): Promise<void> {
    try {
      sourcesData = await fetchSources();
      sourcesError = null;
    } catch (error) {
      sourcesError = errorMessage(error);
      sourcesData = null;
    }
  }

  function resetSessionsPagination(): void {
    sessionsCursor = null;
    sessionsCursorHistory = [];
  }

  async function loadSessions(targetCursor: string | null = sessionsCursor): Promise<boolean> {
    if (get(sessionsLoadingStore)) {
      return false;
    }
    clearSessionsController();
    sessionsController = new AbortController();
    const signal = sessionsController.signal;
    sessionsLoadingStore.set(true);
    sessionsErrorStore.set(null);
    try {
      const result = await fetchSessions({
        allowMock: false,
        limit: sessionsLimit,
        since: sessionsSince,
        cursor: targetCursor,
        signal,
      });
      if (signal.aborted) {
        return false;
      }
      sessionsStore.set(result.sessions);
      sessionsMetaStore.set(result.meta);
      sessionsErrorStore.set(null);
      sessionsCursor = targetCursor;
      sessionsDeferred = false;
      return true;
    } catch (error) {
      if (isRequestAbortError(error)) {
        return false;
      }
      sessionsErrorStore.set(`Sessions unavailable: ${errorMessage(error)}`);
      return false;
    } finally {
      if (sessionsController?.signal === signal) {
        sessionsController = null;
      }
      if (!signal.aborted) {
        sessionsLoadingStore.set(false);
      }
    }
  }

  async function hydrateFast(): Promise<void> {
    await Promise.all([loadHealth(), loadStatus(), loadSources()]);
  }

  async function handleRangeChange(event: CustomEvent<AnalyticsRangeKey>): Promise<void> {
    cancelIdleHandle(analyticsIdleHandle);
    analyticsIdleHandle = null;
    analyticsRangeStore.set(event.detail);
    analyticsDeferred = false;
    await loadAnalytics();
  }

  async function handleAnalyticsLoadRequested(): Promise<void> {
    cancelIdleHandle(analyticsIdleHandle);
    analyticsIdleHandle = null;
    await loadAnalytics();
  }

  async function handleSessionsLoadRequested(): Promise<void> {
    cancelIdleHandle(sessionsIdleHandle);
    sessionsIdleHandle = null;
    sessionsDeferred = false;
    await loadSessions();
  }

  async function handleSessionsLimitChange(event: CustomEvent<number>): Promise<void> {
    sessionsLimit = event.detail;
    resetSessionsPagination();
    if (!sessionsDeferred && !sessionsLoading) {
      await loadSessions();
      await tick();
      scrollSessionsToTop();
    }
  }

  async function handleSessionsPreviousPage(): Promise<void> {
    if (sessionsLoading || sessionsCursorHistory.length === 0) return;
    const nextHistory = [...sessionsCursorHistory];
    const previousCursor = nextHistory.pop() ?? null;
    if (await loadSessions(previousCursor)) {
      sessionsCursorHistory = nextHistory;
      await tick();
      scrollSessionsToTop();
    }
  }

  async function handleSessionsNextPage(): Promise<void> {
    const nextCursor = sessionsMeta?.nextCursor ?? null;
    if (sessionsLoading || !nextCursor) return;
    const currentCursor = sessionsCursor;
    if (await loadSessions(nextCursor)) {
      sessionsCursorHistory = [...sessionsCursorHistory, currentCursor];
      await tick();
      scrollSessionsToTop();
    }
  }

  function scheduleInitialAnalyticsLoad(): void {
    const runner = () => {
      void loadAnalytics();
    };

    if (typeof window !== 'undefined' && 'requestIdleCallback' in window) {
      analyticsIdleHandle = window.requestIdleCallback(runner, { timeout: 5_000 });
      return;
    }

    analyticsIdleHandle = globalThis.setTimeout(runner, 250);
  }

  function scheduleInitialSessionsLoad(): void {
    const runner = () => {
      if (!sessionsDeferred || get(sessionsLoadingStore)) {
        return;
      }
      sessionsDeferred = false;
      void loadSessions();
    };

    if (typeof window !== 'undefined' && 'requestIdleCallback' in window) {
      sessionsIdleHandle = window.requestIdleCallback(runner, { timeout: 7_500 });
      return;
    }

    sessionsIdleHandle = globalThis.setTimeout(runner, 600);
  }

  function handleSetTheme(event: CustomEvent<ThemeMode>): void {
    setTheme(event.detail);
  }

  function handleSourceSelect(event: CustomEvent<string>): void {
    selectedSource = event.detail;
  }

  function handleFilterChange(event: CustomEvent<SessionsFilter>): void {
    sessionsFilterStore.set(event.detail);
  }

  onMount(() => {
    initializeTheme();

    void hydrateFast();
    scheduleInitialAnalyticsLoad();
    scheduleInitialSessionsLoad();

    const fastInterval = window.setInterval(() => {
      void hydrateFast();
    }, FAST_POLL_INTERVAL_MS);

    const slowInterval = window.setInterval(() => {
      if (!analyticsDeferred) {
        void loadAnalytics();
      }
    }, SLOW_POLL_INTERVAL_MS);

    const sessionsInterval = window.setInterval(() => {
      if (!sessionsDeferred) {
        void loadSessions();
      }
    }, SESSIONS_POLL_INTERVAL_MS);

    return () => {
      cancelIdleHandle(analyticsIdleHandle);
      cancelIdleHandle(sessionsIdleHandle);
      clearAnalyticsController();
      clearSessionsController();
      window.clearInterval(fastInterval);
      window.clearInterval(slowInterval);
      window.clearInterval(sessionsInterval);
    };
  });
</script>

<div class="app-shell">
  <TopBar theme={$themeStore} on:setTheme={handleSetTheme} />

  <main class="layout">
    <StatusStrip health={healthData} {healthError} status={statusData} {statusError} />

    <SourcesStrip
      sources={sourcesData?.sources ?? null}
      error={sourcesError}
      queryError={sourcesData?.query_error ?? null}
      on:select={handleSourceSelect}
    />

    <SourceDetail bind:sourceName={selectedSource} />

    <AnalyticsPanel
      payload={analyticsPayload}
      selectedRange={$analyticsRangeStore}
      errorMessage={analyticsError}
      deferred={analyticsDeferred}
      loading={analyticsLoading}
      theme={$themeStore}
      on:rangeChange={handleRangeChange}
      on:loadRequested={handleAnalyticsLoadRequested}
    />

    <SessionsPanel
      sessions={sessions}
      filtered={filteredSessions}
      filter={sessionsFilter}
      models={sessionModels}
      harnesses={sessionHarnesses}
      loading={sessionsLoading}
      errorMessage={sessionsError}
      deferred={sessionsDeferred}
      meta={sessionsMeta}
      selectedLimit={sessionsLimit}
      pageNumber={sessionsCursorHistory.length + 1}
      canGoPrevious={sessionsCursorHistory.length > 0}
      canGoNext={Boolean(sessionsMeta?.nextCursor)}
      on:filterChange={handleFilterChange}
      on:loadRequested={handleSessionsLoadRequested}
      on:limitChange={handleSessionsLimitChange}
      on:previousPage={handleSessionsPreviousPage}
      on:nextPage={handleSessionsNextPage}
    />
  </main>
</div>
