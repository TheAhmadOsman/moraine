<script lang="ts">
  import { onDestroy, tick } from 'svelte';
  import { isRequestAbortError } from '../../../api/client';
  import { fetchSessionDetail } from '../../../api/sessions';
  import SessionCard from '../SessionCard.svelte';
  import SessionDetail from '../SessionDetail.svelte';
  import type { Session } from '../../../types/sessions';

  export let sessions: Session[];

  const DETAIL_TURN_PAGE_SIZE = 50;

  let openId: string | null = null;
  let open: Session | null = null;
  let detailLoading = false;
  let detailPaging = false;
  let detailError: string | null = null;
  let detailToken = 0;
  let detailTurnCursor: string | null = null;
  let detailCursorHistory: Array<string | null> = [];
  let detailController: AbortController | null = null;
  let detailPanel: HTMLDivElement | null = null;

  function clearDetailController(): void {
    detailController?.abort();
    detailController = null;
  }

  function scrollDetailToTop(): void {
    const prefersReducedMotion =
      typeof window !== 'undefined' &&
      typeof window.matchMedia === 'function' &&
      window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    const behavior: ScrollBehavior = prefersReducedMotion ? 'auto' : 'smooth';
    detailPanel?.scrollTo({ top: 0, behavior });
    detailPanel?.querySelector<HTMLElement>('.mv-turns')?.scrollTo({ top: 0, behavior });
  }

  async function loadDetailPage(
    sessionId: string,
    turnCursor: string | null,
    nextHistory: Array<string | null>,
    preserveOpen = false,
  ): Promise<boolean> {
    clearDetailController();
    openId = sessionId;
    detailError = null;
    detailTurnCursor = turnCursor;
    detailCursorHistory = nextHistory;
    if (!preserveOpen) {
      open = null;
      detailLoading = true;
    } else {
      detailPaging = true;
    }
    const token = ++detailToken;
    detailController = new AbortController();
    const signal = detailController.signal;
    try {
      const detail = await fetchSessionDetail(sessionId, {
        signal,
        turnLimit: DETAIL_TURN_PAGE_SIZE,
        turnCursor,
      });
      if (token !== detailToken || openId !== sessionId) return false;
      if (!detail) {
        detailError = 'Session detail unavailable.';
        open = null;
        return false;
      }
      open = detail;
      await tick();
      scrollDetailToTop();
      return true;
    } catch (error) {
      if (token !== detailToken || openId !== sessionId) return false;
      if (isRequestAbortError(error)) {
        return false;
      }
      detailError = error instanceof Error ? error.message : String(error);
      if (!preserveOpen) {
        open = null;
      }
    } finally {
      if (token === detailToken && openId === sessionId) {
        detailLoading = false;
        detailPaging = false;
      }
      if (detailController?.signal === signal) {
        detailController = null;
      }
    }
    return false;
  }

  async function handleOpen(s: Session): Promise<void> {
    detailTurnCursor = null;
    detailCursorHistory = [];
    await loadDetailPage(s.id, null, [], false);
  }

  async function handleOlderTurns(): Promise<void> {
    if (!open?.detailMeta?.nextTurnCursor || detailLoading || detailPaging) {
      return;
    }
    await loadDetailPage(
      open.id,
      open.detailMeta.nextTurnCursor,
      [...detailCursorHistory, detailTurnCursor],
      true,
    );
  }

  async function handleNewerTurns(): Promise<void> {
    if (!open || detailLoading || detailPaging || detailCursorHistory.length === 0) {
      return;
    }
    const nextHistory = [...detailCursorHistory];
    const previousCursor = nextHistory.pop() ?? null;
    await loadDetailPage(open.id, previousCursor, nextHistory, true);
  }

  function handleClose(): void {
    clearDetailController();
    openId = null;
    open = null;
    detailLoading = false;
    detailPaging = false;
    detailError = null;
    detailTurnCursor = null;
    detailCursorHistory = [];
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
    clearDetailController();
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
    <div class="mv-sidepanel" role="dialog" aria-label="Session detail" aria-modal="true" bind:this={detailPanel}>
      {#if detailLoading}
        <div class="mv-empty">Loading session detail…</div>
      {:else if detailError}
        <div class="mv-empty" role="status" aria-live="polite">{detailError}</div>
      {:else if open}
        <SessionDetail
          session={open}
          layout="sidepanel"
          closable
          pagingTurns={detailPaging}
          on:close={handleClose}
          on:olderTurns={handleOlderTurns}
          on:newerTurns={handleNewerTurns}
        />
      {/if}
    </div>
  {/if}
</div>
