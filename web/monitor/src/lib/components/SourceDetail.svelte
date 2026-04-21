<script lang="ts">
  import { fetchSourceDetail, fetchSourceErrors, fetchSourceFiles } from '../api/client';
  import type {
    SourceDetailResponse,
    SourceErrorsResponse,
    SourceFilesResponse,
    SourceHealthStatus,
    SourceLagIndicator,
    SourceWarningSeverity,
  } from '../types/api';

  export let sourceName: string | null = null;

  let summaryData: SourceDetailResponse | null = null;
  let summaryError: string | null = null;
  let filesData: SourceFilesResponse | null = null;
  let filesError: string | null = null;
  let errorsData: SourceErrorsResponse | null = null;
  let errorsError: string | null = null;
  let activeTab: 'files' | 'errors' = 'files';
  let loading = false;
  let loadId = 0;

  async function load(source: string) {
    const currentLoadId = ++loadId;
    loading = true;
    summaryData = null;
    filesData = null;
    errorsData = null;
    summaryError = null;
    filesError = null;
    errorsError = null;

    const [summaryResult, filesResult, errorsResult] = await Promise.allSettled([
      fetchSourceDetail(source),
      fetchSourceFiles(source),
      fetchSourceErrors(source, 50),
    ]);

    if (currentLoadId !== loadId) {
      return;
    }

    if (summaryResult.status === 'fulfilled') {
      summaryData = summaryResult.value;
    } else {
      summaryError = summaryResult.reason instanceof Error ? summaryResult.reason.message : String(summaryResult.reason);
      summaryData = null;
    }

    if (filesResult.status === 'fulfilled') {
      filesData = filesResult.value;
    } else {
      filesError = filesResult.reason instanceof Error ? filesResult.reason.message : String(filesResult.reason);
      filesData = null;
    }

    if (errorsResult.status === 'fulfilled') {
      errorsData = errorsResult.value;
    } else {
      errorsError = errorsResult.reason instanceof Error ? errorsResult.reason.message : String(errorsResult.reason);
      errorsData = null;
    }

    loading = false;
  }

  $: if (sourceName) {
    void load(sourceName);
  }

  function statusTone(status: SourceHealthStatus): 'good' | 'warn' | 'bad' | 'subtle' {
    if (status === 'ok') return 'good';
    if (status === 'warning') return 'warn';
    if (status === 'error') return 'bad';
    return 'subtle';
  }

  function formatBytes(n: number): string {
    if (n === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KiB', 'MiB', 'GiB'];
    const i = Math.floor(Math.log(n) / Math.log(k));
    return `${parseFloat((n / Math.pow(k, i)).toFixed(1))} ${sizes[i]}`;
  }

  function formatCount(n: number): string {
    return n.toLocaleString();
  }

  function formatSeconds(seconds: number | null | undefined): string {
    if (seconds == null) return '—';
    if (seconds < 60) return `${seconds}s`;
    if (seconds < 3600) return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    return `${hours}h ${minutes}m`;
  }

  function formatLagMs(ms: number | null | undefined): string {
    if (ms == null) return '—';
    if (ms < 1000) return `${ms} ms`;
    return `${(ms / 1000).toFixed(1)} s`;
  }

  function cadenceLabel(seconds: number): string {
    if (seconds < 60) return `${seconds.toFixed(seconds % 1 === 0 ? 0 : 1)}s`;
    return `${(seconds / 60).toFixed((seconds / 60) % 1 === 0 ? 0 : 1)}m`;
  }

  function lagTone(indicator: SourceLagIndicator | null | undefined): 'good' | 'warn' | 'bad' | 'subtle' {
    if (indicator === 'healthy') return 'good';
    if (indicator === 'delayed') return 'warn';
    if (indicator === 'stale') return 'bad';
    return 'subtle';
  }

  function warningTone(severity: SourceWarningSeverity): 'warn' | 'bad' {
    return severity === 'error' ? 'bad' : 'warn';
  }

  function warningLabel(kind: string): string {
    if (kind === 'file_state') return 'File state';
    if (kind === 'ingest_heartbeat') return 'Heartbeat';
    return 'Watcher';
  }

  function close() {
    loadId += 1;
    loading = false;
    sourceName = null;
    summaryData = null;
    summaryError = null;
    filesData = null;
    filesError = null;
    errorsData = null;
    errorsError = null;
  }
</script>

{#if sourceName}
  <section class="panel mv-root" id="sourceDetail">
    <div class="mv-section-head">
      <div class="mv-section-title">
        <h2>Source: {sourceName}</h2>
      </div>
      <button class="mv-iconbtn" on:click={close} aria-label="Close source detail" title="Close">
        ✕
      </button>
    </div>

    <div class="sd-tabs">
      <button
        class="sd-tab"
        class:is-active={activeTab === 'files'}
        on:click={() => (activeTab = 'files')}
      >
        Files ({filesData?.files.length ?? 0})
      </button>
      <button
        class="sd-tab"
        class:is-active={activeTab === 'errors'}
        on:click={() => (activeTab = 'errors')}
      >
        Errors ({errorsData?.errors.length ?? 0})
      </button>
    </div>

    {#if summaryError}
      <div class="sd-warn" role="status">Summary: {summaryError}</div>
    {:else if summaryData}
      {#if summaryData.query_error}
        <div class="sd-warn" role="status">Summary query: {summaryData.query_error}</div>
      {/if}
      {#if summaryData.runtime_query_error}
        <div class="sd-warn" role="status">Runtime query: {summaryData.runtime_query_error}</div>
      {/if}
      {#if summaryData.warnings.length > 0}
        <div class="sd-warning-list">
          {#each summaryData.warnings as warning}
            <div class="sd-warning-card">
              <div class="sd-summary-label">{warningLabel(warning.kind)}</div>
              <div class="sd-warning-copy">
                <span
                  class="sd-status"
                  class:sd-status-warn={warningTone(warning.severity) === 'warn'}
                  class:sd-status-bad={warningTone(warning.severity) === 'bad'}
                >
                  {warning.severity}
                </span>
                <span>{warning.summary}</span>
              </div>
            </div>
          {/each}
        </div>
      {/if}
      <div class="sd-summary">
        <div class="sd-summary-main">
          <div class="sd-summary-item">
            <div class="sd-summary-label">Status</div>
            <div class="sd-summary-value">
              <span
                class="sd-status"
                class:sd-status-good={statusTone(summaryData.source.status) === 'good'}
                class:sd-status-warn={statusTone(summaryData.source.status) === 'warn'}
                class:sd-status-bad={statusTone(summaryData.source.status) === 'bad'}
              >
                {summaryData.source.status}
              </span>
            </div>
          </div>
          <div class="sd-summary-item">
            <div class="sd-summary-label">Harness</div>
            <div class="sd-summary-value mono">{summaryData.source.harness}</div>
          </div>
          <div class="sd-summary-item">
            <div class="sd-summary-label">Format</div>
            <div class="sd-summary-value mono">{summaryData.source.format}</div>
          </div>
          <div class="sd-summary-item sd-summary-wide">
            <div class="sd-summary-label">Watch Root</div>
            <div class="sd-summary-value mono sd-summary-path" title={summaryData.source.watch_root}>
              {summaryData.source.watch_root}
            </div>
          </div>
          <div class="sd-summary-item sd-summary-wide">
            <div class="sd-summary-label">Glob</div>
            <div class="sd-summary-value mono sd-summary-path" title={summaryData.source.glob}>
              {summaryData.source.glob}
            </div>
          </div>
        </div>

        <div class="sd-summary-side">
          <div class="sd-summary-item">
            <div class="sd-summary-label">Counts</div>
            <div class="sd-summary-stack mono">
              <span>Raw: {formatCount(summaryData.source.raw_event_count)}</span>
              <span>Checkpoints: {formatCount(summaryData.source.checkpoint_count)}</span>
              <span>Errors: {formatCount(summaryData.source.ingest_error_count)}</span>
            </div>
          </div>
          <div class="sd-summary-item">
            <div class="sd-summary-label">Latest Checkpoint</div>
            <div class="sd-summary-stack">
              <span class="mono">{summaryData.source.latest_checkpoint_at ?? '—'}</span>
              <span>Age: {formatSeconds(summaryData.source.latest_checkpoint_age_seconds)}</span>
            </div>
          </div>
          <div class="sd-summary-item">
            <div class="sd-summary-label">Latest Error</div>
            {#if summaryData.source.latest_error_at || summaryData.source.latest_error_kind || summaryData.source.latest_error_text}
              <div class="sd-summary-stack">
                <span class="mono">{summaryData.source.latest_error_at ?? '—'}</span>
                <span class="mono">{summaryData.source.latest_error_kind ?? '—'}</span>
                <span class="sd-summary-error-text">{summaryData.source.latest_error_text ?? '—'}</span>
              </div>
            {:else}
              <div class="sd-summary-value">—</div>
            {/if}
          </div>
          <div class="sd-summary-item">
            <div class="sd-summary-label">Heartbeat</div>
            <div class="sd-summary-stack">
              <span class="mono">{summaryData.runtime.latest_heartbeat_at ?? '—'}</span>
              <span>Age: {formatSeconds(summaryData.runtime.latest_heartbeat_age_seconds)}</span>
              <span>Every {cadenceLabel(summaryData.runtime.heartbeat_cadence_seconds)}</span>
            </div>
          </div>
          <div class="sd-summary-item">
            <div class="sd-summary-label">Runtime Lag</div>
            <div class="sd-summary-stack">
              <span
                class="sd-status"
                class:sd-status-good={lagTone(summaryData.runtime.lag_indicator) === 'good'}
                class:sd-status-warn={lagTone(summaryData.runtime.lag_indicator) === 'warn'}
                class:sd-status-bad={lagTone(summaryData.runtime.lag_indicator) === 'bad'}
              >
                {summaryData.runtime.lag_indicator ?? 'unknown'}
              </span>
              <span>Queue: {summaryData.runtime.queue_depth ?? '—'}</span>
              <span>Append p95: {formatLagMs(summaryData.runtime.append_to_visible_p95_ms)}</span>
            </div>
          </div>
          <div class="sd-summary-item">
            <div class="sd-summary-label">Watcher</div>
            <div class="sd-summary-stack">
              <span class="mono">{summaryData.runtime.watcher_backend ?? '—'}</span>
              <span>
                Errors: {summaryData.runtime.watcher_error_count ?? '—'} | Resets: {summaryData.runtime.watcher_reset_count ?? '—'}
              </span>
              <span class="mono">Last reset: {summaryData.runtime.watcher_last_reset_at ?? '—'}</span>
            </div>
          </div>
          <div class="sd-summary-item">
            <div class="sd-summary-label">Cadence</div>
            <div class="sd-summary-stack">
              <span>Heartbeat: {cadenceLabel(summaryData.runtime.heartbeat_cadence_seconds)}</span>
              <span>Reconcile: {cadenceLabel(summaryData.runtime.reconcile_cadence_seconds)}</span>
              <span>Watched files: {summaryData.runtime.files_watched ?? '—'}</span>
            </div>
          </div>
        </div>
      </div>
    {/if}

    {#if loading}
      <div class="mv-empty">Loading…</div>
    {:else if activeTab === 'files'}
      {#if filesError}
        <div class="mv-empty" role="status" aria-live="polite">{filesError}</div>
      {:else if filesData}
        {#if filesData.fs_error}
          <div class="sd-warn" role="status">Filesystem: {filesData.fs_error}</div>
        {/if}
        {#if filesData.query_error}
          <div class="sd-warn" role="status">Query: {filesData.query_error}</div>
        {/if}
        {#if filesData.files.length === 0}
          <div class="mv-empty">No files matched for this source.</div>
        {:else}
          <div class="sd-table-wrap">
            <table class="sd-table">
              <thead>
                <tr>
                  <th>Path</th>
                  <th>Size</th>
                  <th>Modified</th>
                  <th>Raw</th>
                  <th>Checkpoint</th>
                  <th>Status</th>
                </tr>
              </thead>
              <tbody>
                {#each filesData.files as file}
                  <tr>
                    <td class="sd-path" title={file.path}>{file.path}</td>
                    <td class="mono">{formatBytes(file.size_bytes)}</td>
                    <td class="mono">{file.modified_at ?? '—'}</td>
                    <td class="mono">{file.raw_event_count}</td>
                    <td class="mono">{file.checkpoint_offset ?? '—'}</td>
                    <td>
                      <span class="sd-status" class:sd-status-warn={file.checkpoint_status === 'error'}>
                        {file.checkpoint_status ?? '—'}
                      </span>
                    </td>
                  </tr>
                {/each}
              </tbody>
            </table>
          </div>
        {/if}
      {/if}
    {:else if activeTab === 'errors'}
      {#if errorsError}
        <div class="mv-empty" role="status" aria-live="polite">{errorsError}</div>
      {:else if errorsData}
        {#if errorsData.query_error}
          <div class="sd-warn" role="status">Query: {errorsData.query_error}</div>
        {/if}
        {#if errorsData.errors.length === 0}
          <div class="mv-empty">No errors recorded for this source.</div>
        {:else}
          <div class="sd-errors">
            {#each errorsData.errors as err}
              <div class="sd-error-row">
                <div class="sd-error-meta">
                  <span class="sd-error-time mono">{err.ingested_at}</span>
                  <span class="sd-error-kind">{err.error_kind}</span>
                  <span class="sd-error-file mono" title={err.source_file}>{err.source_file}</span>
                </div>
                <div class="sd-error-text">{err.error_text}</div>
                {#if err.raw_fragment}
                  <pre class="sd-error-raw">{err.raw_fragment}</pre>
                {/if}
              </div>
            {/each}
          </div>
        {/if}
      {/if}
    {/if}
  </section>
{/if}

<style>
  .sd-summary {
    display: grid;
    grid-template-columns: minmax(0, 1.5fr) minmax(16rem, 1fr);
    gap: 0.75rem;
    margin-bottom: 0.75rem;
  }

  .sd-summary-main,
  .sd-summary-side {
    display: grid;
    gap: 0.75rem;
  }

  .sd-summary-main {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }

  .sd-summary-item {
    min-width: 0;
    padding: 0.625rem 0.75rem;
    border: 1px solid var(--line);
    border-radius: 0.75rem;
    background: var(--panel-alt);
  }

  .sd-summary-wide {
    grid-column: 1 / -1;
  }

  .sd-summary-label {
    margin-bottom: 0.375rem;
    color: var(--subtle);
    font-size: 0.6875rem;
    font-family: 'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }

  .sd-summary-value,
  .sd-summary-stack {
    font-size: 0.8125rem;
    color: var(--text);
  }

  .sd-summary-stack {
    display: flex;
    flex-direction: column;
    gap: 0.25rem;
  }

  .sd-summary-path,
  .sd-summary-error-text {
    overflow-wrap: anywhere;
  }

  .sd-tabs {
    display: flex;
    gap: 0.25rem;
    margin-bottom: 0.75rem;
    border-bottom: 1px solid var(--line);
    padding-bottom: 0.25rem;
  }

  .sd-tab {
    appearance: none;
    border: 0;
    background: transparent;
    cursor: pointer;
    padding: 0.375rem 0.75rem;
    border-radius: 0.5rem;
    font-family: 'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.8125rem;
    color: var(--subtle);
    transition: background 0.12s ease, color 0.12s ease;
  }

  .sd-tab:hover {
    background: var(--active-bg);
    color: var(--text);
  }

  .sd-tab.is-active {
    background: var(--range-active-bg);
    color: var(--range-active-text);
    font-weight: 700;
  }

  .sd-warn {
    padding: 0.5rem 0.75rem;
    border-radius: 0.5rem;
    background: rgba(180, 83, 9, 0.08);
    color: var(--warn);
    font-size: 0.8125rem;
    margin-bottom: 0.75rem;
  }

  .sd-warning-list {
    display: grid;
    gap: 0.5rem;
    margin-bottom: 0.75rem;
  }

  .sd-warning-card {
    padding: 0.625rem 0.75rem;
    border: 1px solid var(--line);
    border-radius: 0.75rem;
    background: var(--panel-alt);
  }

  .sd-warning-copy {
    display: flex;
    flex-wrap: wrap;
    gap: 0.5rem;
    align-items: center;
    font-size: 0.8125rem;
  }

  .sd-table-wrap {
    overflow: auto;
    max-height: 420px;
    border: 1px solid var(--line);
    border-radius: 0.75rem;
  }

  .sd-table {
    width: 100%;
    border-collapse: collapse;
    font-size: 0.8125rem;
  }

  .sd-table thead {
    position: sticky;
    top: 0;
    background: var(--header-bg);
    color: var(--header-text);
    font-family: 'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.6875rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }

  .sd-table th,
  .sd-table td {
    padding: 0.5rem 0.625rem;
    text-align: left;
    border-bottom: 1px solid var(--line);
    white-space: nowrap;
  }

  .sd-table tbody tr:nth-child(even) {
    background: var(--row-stripe);
  }

  .sd-path {
    max-width: 360px;
    overflow: hidden;
    text-overflow: ellipsis;
    font-family: 'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace;
    font-size: 0.75rem;
  }

  .sd-status {
    display: inline-flex;
    padding: 0.125rem 0.375rem;
    border-radius: 0.375rem;
    font-size: 0.6875rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    background: var(--active-bg);
    color: var(--range-active-text);
  }

  .sd-status-warn {
    background: rgba(190, 18, 60, 0.1);
    color: var(--bad);
  }

  .sd-status-good {
    background: rgba(22, 163, 74, 0.12);
    color: var(--good);
  }

  .sd-status-bad {
    background: rgba(190, 18, 60, 0.1);
    color: var(--bad);
  }

  .sd-errors {
    display: flex;
    flex-direction: column;
    gap: 0.625rem;
    max-height: 480px;
    overflow: auto;
    padding-right: 0.25rem;
  }

  .sd-error-row {
    border: 1px solid var(--line);
    border-radius: 0.625rem;
    padding: 0.625rem 0.875rem;
    background: var(--panel-alt);
  }

  .sd-error-meta {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 0.375rem;
    font-size: 0.75rem;
  }

  .sd-error-time {
    color: var(--subtle);
  }

  .sd-error-kind {
    padding: 0.0625rem 0.375rem;
    border-radius: 0.375rem;
    background: rgba(190, 18, 60, 0.1);
    color: var(--bad);
    font-weight: 600;
    font-size: 0.6875rem;
    text-transform: uppercase;
    letter-spacing: 0.04em;
  }

  .sd-error-file {
    color: var(--subtle);
    max-width: 280px;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  .sd-error-text {
    font-size: 0.8125rem;
    color: var(--text);
    word-break: break-word;
  }

  .sd-error-raw {
    margin: 0.375rem 0 0;
    padding: 0.5rem 0.625rem;
    border-radius: 0.5rem;
    background: var(--panel);
    border: 1px solid var(--line);
    font-size: 0.75rem;
    font-family: 'IBM Plex Mono', ui-monospace, SFMono-Regular, Menlo, monospace;
    color: var(--subtle);
    overflow: auto;
    white-space: pre-wrap;
    word-break: break-all;
    max-height: 160px;
  }

  .mv-empty {
    padding: 1.5rem;
    text-align: center;
    color: var(--subtle);
    font-size: 0.875rem;
  }

  @media (max-width: 900px) {
    .sd-summary {
      grid-template-columns: 1fr;
    }

    .sd-summary-main {
      grid-template-columns: 1fr;
    }
  }
</style>
