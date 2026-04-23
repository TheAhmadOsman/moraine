import type { Session, SessionsMeta, SessionsResponse, SessionsSinceKey } from '../types/sessions';
import { requestJson } from './client';
import { generateMockSessions } from './sessionsMock';

export interface FetchSessionsOptions {
  allowMock?: boolean;
  limit?: number;
  since?: SessionsSinceKey;
  cursor?: string | null;
}

export interface FetchSessionsResult {
  sessions: Session[];
  meta: SessionsMeta | null;
}

function normalizeMeta(data: SessionsResponse, sessions: Session[]): SessionsMeta | null {
  if (!data.meta) {
    return null;
  }
  return {
    requestedLimit: Number(data.meta.requested_limit ?? sessions.length),
    effectiveLimit: Number(data.meta.effective_limit ?? sessions.length),
    loadedCount: Number(data.meta.loaded_count ?? sessions.length),
    hasMore: Boolean(data.meta.has_more),
    sinceSeconds: Number(data.meta.since_seconds ?? 0),
    nextCursor: data.meta.next_cursor ?? null,
  };
}

export async function fetchSessions(options: FetchSessionsOptions = {}): Promise<FetchSessionsResult> {
  const { allowMock = true, limit, since, cursor } = options;

  try {
    const query = new URLSearchParams();
    if (typeof limit === 'number') {
      query.set('limit', String(limit));
    }
    if (since) {
      query.set('since', since);
    }
    if (cursor) {
      query.set('cursor', cursor);
    }
    const url = query.size > 0 ? `/api/sessions?${query.toString()}` : '/api/sessions';
    const data = await requestJson<SessionsResponse>(url, { timeoutMs: 15_000 });
    if (data.ok && Array.isArray(data.sessions)) {
      return { sessions: data.sessions, meta: normalizeMeta(data, data.sessions) };
    }
    if (data.error) {
      throw new Error(data.error);
    }
    throw new Error('sessions request failed');
  } catch (error) {
    if (!allowMock) {
      throw error;
    }
  }

  const sessions = generateMockSessions();
  return {
    sessions,
    meta: {
      requestedLimit: sessions.length,
      effectiveLimit: sessions.length,
      loadedCount: sessions.length,
      hasMore: false,
      sinceSeconds: 0,
      nextCursor: null,
    },
  };
}

export async function fetchSessionDetail(id: string): Promise<Session | null> {
  const controller = new AbortController();
  const timeoutId = globalThis.setTimeout(() => controller.abort(), 15_000);
  try {
    const response = await fetch(`/api/sessions/${encodeURIComponent(id)}`, {
      headers: { Accept: 'application/json' },
      signal: controller.signal,
    });
    if (!response.ok) {
      if (response.status === 404) {
        return null;
      }
      throw new Error(`session detail request failed (${response.status})`);
    }
    const data = (await response.json()) as { ok: boolean; session?: Session; error?: string };
    if (data.ok && data.session) {
      return data.session;
    }
    if (data.error) {
      throw new Error(data.error);
    }
  } catch (error) {
    if (controller.signal.aborted) {
      throw new Error('session detail request timed out');
    }
    throw error;
  } finally {
    globalThis.clearTimeout(timeoutId);
  }
  return null;
}
