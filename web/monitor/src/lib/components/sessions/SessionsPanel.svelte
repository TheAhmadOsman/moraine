<script lang="ts">
  import { createEventDispatcher } from 'svelte';
  import FilterBar from './FilterBar.svelte';
  import V1Library from './variations/V1Library.svelte';
  import type { Harness, Session, SessionsFilter, SessionsMeta } from '../../types/sessions';

  export let sessions: Session[] = [];
  export let filtered: Session[] = [];
  export let filter: SessionsFilter = { query: '', model: 'all', status: 'all', harness: 'all' };
  export let models: string[] = [];
  export let harnesses: Harness[] = [];
  export let loading = false;
  export let errorMessage: string | null = null;
  export let deferred = true;
  export let meta: SessionsMeta | null = null;
  export let selectedLimit = 25;

  const limitOptions = [25, 50, 100, 200];

  const dispatch = createEventDispatcher<{
    filterChange: SessionsFilter;
    loadRequested: void;
    limitChange: number;
  }>();

  function handleFilter(next: SessionsFilter): void {
    dispatch('filterChange', next);
  }

  function formatSince(seconds: number): string {
    if (!seconds) return 'all time';
    if (seconds % 86_400 === 0) return `${seconds / 86_400}d`;
    if (seconds % 3_600 === 0) return `${seconds / 3_600}h`;
    return `${seconds}s`;
  }
</script>

<section class="panel mv-root" id="sessionsPanel">
  <div class="mv-section-head">
    <div class="mv-section-title">
      <h2>Sessions</h2>
      <span class="mv-section-subtitle">Search, inspect, and replay agent sessions.</span>
    </div>
    <div class="mv-panel-actions">
      <label class="mv-inline-control">
        <span class="mv-filter-k">limit</span>
        <select
          class="mv-select"
          value={String(selectedLimit)}
          on:change={(e) => dispatch('limitChange', Number(e.currentTarget.value))}
        >
          {#each limitOptions as option (option)}
            <option value={option}>{option}</option>
          {/each}
        </select>
      </label>
      <button class="mv-button" type="button" on:click={() => dispatch('loadRequested')} disabled={loading}>
        {#if loading}
          Loading…
        {:else if deferred}
          Load Sessions
        {:else}
          Refresh Sessions
        {/if}
      </button>
    </div>
  </div>

  {#if errorMessage}
    <div class="mv-empty" role="status" aria-live="polite">{errorMessage}</div>
  {/if}

  {#if !deferred || sessions.length > 0}
    <FilterBar
      {filter}
      {models}
      {harnesses}
      count={filtered.length}
      total={sessions.length}
      on:change={(e) => handleFilter(e.detail)}
    />
  {/if}

  {#if deferred && sessions.length === 0}
    <div class="mv-empty">
      Sessions are deferred on initial load because central history queries can be slow.
      Use <span class="mono">Load Sessions</span> when you want the current page.
    </div>
  {:else if loading && sessions.length === 0}
    <div class="mv-empty">Loading sessions…</div>
  {:else}
    {#if meta}
      <div class="mv-sessions-meta mono">
        Loaded {meta.loadedCount} session{meta.loadedCount === 1 ? '' : 's'}
        {#if meta.hasMore}
          of at least {meta.loadedCount + 1}
        {/if}
        · page limit {meta.effectiveLimit}
        · window {formatSince(meta.sinceSeconds)}
        {#if meta.hasMore}
          · more available
        {/if}
      </div>
    {/if}
    <V1Library sessions={filtered} />
  {/if}
</section>
