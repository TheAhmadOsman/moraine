import type { Session, SessionsMeta, SessionsResponse, SessionsSinceKey } from '../types/sessions';
import { RequestAbortedError, RequestTimeoutError, requestJson } from './client';
import { generateMockSessions } from './sessionsMock';

export interface FetchSessionsOptions {
  allowMock?: boolean;
  limit?: number;
  since?: SessionsSinceKey;
  cursor?: string | null;
  signal?: AbortSignal;
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
  const { allowMock = true, limit, since, cursor, signal } = options;

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
    const data = await requestJson<SessionsResponse>(url, { timeoutMs: 15_000, signal });
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

export async function fetchSessionDetail(id: string, signal?: AbortSignal): Promise<Session | null> {
  try {
    const data = await requestJson<{ ok: boolean; session?: Session; error?: string }>(
      `/api/sessions/${encodeURIComponent(id)}`,
      { timeoutMs: 15_000, signal },
    );
    if (data.ok && data.session) {
      return data.session;
    }
    if (data.error) {
      throw new Error(data.error);
    }
  } catch (error) {
    if (error instanceof Error && error.message.includes('request failed (404)')) {
      return null;
    }
    if (error instanceof RequestTimeoutError || error instanceof RequestAbortedError) {
      throw error;
    }
    throw error;
  }
  return null;
}
