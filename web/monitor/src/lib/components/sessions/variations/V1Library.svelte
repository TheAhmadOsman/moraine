<script lang="ts">
  import { onDestroy } from 'svelte';
  import { fetchSessionDetail } from '../../../api/sessions';
  import SessionCard from '../SessionCard.svelte';
  import SessionDetail from '../SessionDetail.svelte';
  import type { Session } from '../../../types/sessions';

  export let sessions: Session[];

  let openId: string | null = null;
  let open: Session | null = null;
  let detailLoading = false;
  let detailError: string | null = null;
  let detailToken = 0;

  async function handleOpen(s: Session): Promise<void> {
    openId = s.id;
    detailError = null;
    if (s.hasDetail) {
      open = s;
      detailLoading = false;
      return;
    }
    open = null;
    detailLoading = true;
    const token = ++detailToken;
    try {
      const detail = await fetchSessionDetail(s.id);
      if (token !== detailToken || openId !== s.id) return;
      if (!detail) {
        detailError = 'Session detail unavailable.';
        open = null;
        return;
      }
      open = detail;
    } catch (error) {
      if (token !== detailToken || openId !== s.id) return;
      detailError = error instanceof Error ? error.message : String(error);
      open = null;
    } finally {
      if (token === detailToken && openId === s.id) {
        detailLoading = false;
      }
    }
  }

  function handleClose(): void {
    openId = null;
    open = null;
    detailLoading = false;
    detailError = null;
  }

  function onKey(event: KeyboardEvent): void {
    if (event.key === 'Escape') {
      openId = null;
    }
  }

  $: if (open && typeof window !== 'undefined') {
    window.addEventListener('keydown', onKey);
  } else if (typeof window !== 'undefined') {
    window.removeEventListener('keydown', onKey);
  }

  onDestroy(() => {
    if (typeof window !== 'undefined') {
      window.removeEventListener('keydown', onKey);
    }
  });

  $: if (openId && !sessions.some((s) => s.id === openId)) {
    handleClose();
  }
</script>

<div class="mv-v1">
  <div class="mv-v1-list mv-list">
    {#if sessions.length === 0}
      <div class="mv-empty">No sessions match these filters.</div>
    {/if}
    {#each sessions as session (session.id)}
      <SessionCard {session} active={session.id === openId} variant="library" on:open={(e) => void handleOpen(e.detail)} />
    {/each}
  </div>
  {#if openId}
    <div class="mv-sidepanel-backdrop" role="presentation" on:click={handleClose}></div>
    <div class="mv-sidepanel" role="dialog" aria-label="Session detail" aria-modal="true">
      {#if detailLoading}
        <div class="mv-empty">Loading session detail…</div>
      {:else if detailError}
        <div class="mv-empty" role="status" aria-live="polite">{detailError}</div>
      {:else if open}
        <SessionDetail session={open} layout="sidepanel" closable on:close={handleClose} />
      {/if}
    </div>
  {/if}
</div>
