<script lang="ts">
  import type { SourceHealth, SourceHealthStatus } from '../types/api';

  export let sources: SourceHealth[] | null = null;
  export let error: string | null = null;
  export let queryError: string | null = null;

  interface Chip {
    name: string;
    status: SourceHealthStatus;
    ok: boolean;
    tone?: 'warn' | 'bad';
  }

  function buildChips(list: SourceHealth[] | null, err: string | null): Chip[] {
    if (err) {
      return [{ name: 'error', status: 'unknown', ok: false, tone: 'bad' }];
    }
    if (!list || list.length === 0) {
      return [{ name: 'none', status: 'unknown', ok: false }];
    }
    return list.map((s) => {
      const tone: Chip['tone'] =
        s.status === 'warning' ? 'warn' : s.status === 'error' ? 'bad' : undefined;
      return {
        name: s.name,
        status: s.status,
        ok: s.status === 'ok',
        tone,
      };
    });
  }

  $: chips = buildChips(sources, error);
</script>

<section class="panel status-strip" id="sourcesStrip">
  <div class="ss-group" id="sourcesGroup">
    <div class="ss-group-label">Sources</div>
    <div class="ss-chips">
      {#each chips as chip (chip.name)}
        <span
          class="ss-chip"
          class:ss-ok={chip.ok}
          class:ss-warn={chip.tone === 'warn'}
          class:ss-bad={chip.tone === 'bad'}
          title={chip.status}
        >
          {#if chip.ok}<span class="ss-dot"></span>{/if}
          <span class="ss-k">{chip.name}</span>
          <span class="ss-v">{chip.status}</span>
        </span>
      {/each}
    </div>
  </div>
  {#if queryError}
    <div class="ss-divider" aria-hidden="true"></div>
    <div class="ss-group">
      <div class="ss-group-label">Query</div>
      <div class="ss-chips">
        <span class="ss-chip ss-warn">
          <span class="ss-k">partial</span>
          <span class="ss-v">{queryError}</span>
        </span>
      </div>
    </div>
  {/if}
</section>
