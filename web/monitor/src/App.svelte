<script lang="ts">
  import { get } from 'svelte/store';
  import { onMount } from 'svelte';
  import AnalyticsPanel from './lib/components/AnalyticsPanel.svelte';
  import SourceDetail from './lib/components/SourceDetail.svelte';
  import SourcesStrip from './lib/components/SourcesStrip.svelte';
  import StatusStrip from './lib/components/StatusStrip.svelte';
  import SessionsPanel from './lib/components/sessions/SessionsPanel.svelte';
  import TopBar from './lib/components/TopBar.svelte';
  import { fetchAnalytics, fetchHealth, fetchSources, fetchStatus } from './lib/api/client';
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

  let healthData: HealthResponse | null = null;
  let healthError: string | null = null;

  let statusData: StatusResponse | null = null;
  let statusError: string | null = null;

  let analyticsPayload: AnalyticsResponse | null = null;
  let analyticsError: string | null = null;
  let analyticsDeferred = true;

  let sourcesData: SourcesResponse | null = null;
  let sourcesError: string | null = null;
  let selectedSource: string | null = null;
  let sessionsDeferred = true;
  let sessionsLimit = 25;
  let sessionsSince: SessionsSinceKey = '30d';

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
    try {
      analyticsPayload = await fetchAnalytics(get(analyticsRangeStore));
      analyticsError = null;
      analyticsDeferred = false;
    } catch (error) {
      analyticsError = `Analytics unavailable: ${errorMessage(error)}`;
      analyticsDeferred = false;
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

  async function loadSessions(): Promise<void> {
    sessionsLoadingStore.set(true);
    try {
      const result = await fetchSessions({ limit: sessionsLimit, since: sessionsSince });
      sessionsStore.set(result.sessions);
      sessionsMetaStore.set(result.meta);
      sessionsErrorStore.set(null);
      sessionsDeferred = false;
    } catch (error) {
      sessionsErrorStore.set(`Sessions unavailable: ${errorMessage(error)}`);
    } finally {
      sessionsLoadingStore.set(false);
    }
  }

  async function hydrateFast(): Promise<void> {
    await Promise.all([loadHealth(), loadStatus(), loadSources()]);
  }

  async function handleRangeChange(event: CustomEvent<AnalyticsRangeKey>): Promise<void> {
    analyticsRangeStore.set(event.detail);
    analyticsDeferred = false;
    await loadAnalytics();
  }

  async function handleAnalyticsLoadRequested(): Promise<void> {
    analyticsDeferred = false;
    await loadAnalytics();
  }

  async function handleSessionsLoadRequested(): Promise<void> {
    sessionsDeferred = false;
    await loadSessions();
  }

  async function handleSessionsLimitChange(event: CustomEvent<number>): Promise<void> {
    sessionsLimit = event.detail;
    if (!sessionsDeferred) {
      await loadSessions();
    }
  }

  function scheduleInitialAnalyticsLoad(): void {
    const runner = () => {
      void loadAnalytics();
    };

    if (typeof window !== 'undefined' && 'requestIdleCallback' in window) {
      window.requestIdleCallback(runner, { timeout: 5_000 });
      return;
    }

    globalThis.setTimeout(runner, 250);
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
      on:filterChange={handleFilterChange}
      on:loadRequested={handleSessionsLoadRequested}
      on:limitChange={handleSessionsLimitChange}
    />
  </main>
</div>
