<script lang="ts">
  import { onMount } from 'svelte';
  import { fetchSourceFiles, fetchSourceErrors } from '../api/client';
  import type { SourceFilesResponse, SourceErrorsResponse, SourceHealthStatus } from '../types/api';

  export let sourceName: string | null = null;

  let filesData: SourceFilesResponse | null = null;
  let filesError: string | null = null;
  let errorsData: SourceErrorsResponse | null = null;
  let errorsError: string | null = null;
  let activeTab: 'files' | 'errors' = 'files';
  let loading = false;

  async function load(source: string) {
    loading = true;
    filesError = null;
    errorsError = null;

    try {
      filesData = await fetchSourceFiles(source);
    } catch (err) {
      filesError = err instanceof Error ? err.message : String(err);
      filesData = null;
    }

    try {
      errorsData = await fetchSourceErrors(source, 50);
    } catch (err) {
      errorsError = err instanceof Error ? err.message : String(err);
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

  function close() {
    sourceName = null;
    filesData = null;
    errorsData = null;
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
</style>
