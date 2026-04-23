import type {
  AnalyticsRangeKey,
  AnalyticsResponse,
  HealthResponse,
  SourceDetailResponse,
  SourceErrorsResponse,
  SourceFilesResponse,
  SourcesResponse,
  StatusResponse,
} from '../types/api';

interface ErrorPayload {
  error?: string;
}

export interface JsonRequestOptions {
  timeoutMs?: number;
  signal?: AbortSignal;
}

export class RequestTimeoutError extends Error {
  constructor(message = 'request timed out') {
    super(message);
    this.name = 'RequestTimeoutError';
  }
}

export class RequestAbortedError extends Error {
  constructor(message = 'request cancelled') {
    super(message);
    this.name = 'RequestAbortedError';
  }
}

export function isRequestAbortError(error: unknown): error is RequestTimeoutError | RequestAbortedError {
  return error instanceof RequestTimeoutError || error instanceof RequestAbortedError;
}

export async function requestJson<T>(path: string, options: JsonRequestOptions = {}): Promise<T> {
  const controller = options.timeoutMs || options.signal ? new AbortController() : null;
  let timeoutTriggered = false;
  const abortFromSignal = () => controller?.abort();
  if (options.signal) {
    if (options.signal.aborted) {
      throw new RequestAbortedError();
    }
    options.signal.addEventListener('abort', abortFromSignal, { once: true });
  }
  const timeoutId =
    controller && options.timeoutMs
      ? globalThis.setTimeout(() => {
          timeoutTriggered = true;
          controller.abort();
        }, options.timeoutMs)
      : null;

  let response: Response;
  try {
    response = await fetch(path, {
      headers: {
        Accept: 'application/json',
      },
      ...(controller ? { signal: controller.signal } : {}),
    });
  } catch (error) {
    if (controller?.signal.aborted) {
      if (timeoutTriggered) {
        throw new RequestTimeoutError();
      }
      if (options.signal?.aborted) {
        throw new RequestAbortedError();
      }
    }
    throw error;
  } finally {
    if (options.signal) {
      options.signal.removeEventListener('abort', abortFromSignal);
    }
    if (timeoutId !== null) {
      globalThis.clearTimeout(timeoutId);
    }
  }

  if (!response.ok) {
    let errorMessage: string | undefined;
    const contentType = response.headers.get('content-type') ?? '';

    if (contentType.includes('application/json')) {
      try {
        const data = (await response.json()) as ErrorPayload;
        errorMessage = data.error;
      } catch {
        errorMessage = undefined;
      }
    }

    throw new Error(errorMessage || `request failed (${response.status})`);
  }

  return (await response.json()) as T;
}

export function fetchHealth(): Promise<HealthResponse> {
  return requestJson<HealthResponse>('/api/health');
}

export function fetchStatus(): Promise<StatusResponse> {
  return requestJson<StatusResponse>('/api/status');
}

export function fetchAnalytics(range: AnalyticsRangeKey, signal?: AbortSignal): Promise<AnalyticsResponse> {
  return requestJson<AnalyticsResponse>(`/api/analytics?range=${encodeURIComponent(range)}`, {
    timeoutMs: 15_000,
    signal,
  });
}

export function fetchSources(): Promise<SourcesResponse> {
  return requestJson<SourcesResponse>('/api/sources');
}

export function fetchSourceDetail(source: string): Promise<SourceDetailResponse> {
  return requestJson<SourceDetailResponse>(`/api/sources/${encodeURIComponent(source)}`);
}

export function fetchSourceFiles(source: string): Promise<SourceFilesResponse> {
  return requestJson<SourceFilesResponse>(`/api/sources/${encodeURIComponent(source)}/files`);
}

export function fetchSourceErrors(source: string, limit = 50): Promise<SourceErrorsResponse> {
  return requestJson<SourceErrorsResponse>(
    `/api/sources/${encodeURIComponent(source)}/errors?limit=${encodeURIComponent(limit)}`
  );
}
