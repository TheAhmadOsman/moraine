import type { AnalyticsRangeKey, AnalyticsResponse, HealthResponse, SourcesResponse, SourceFilesResponse, SourceErrorsResponse, StatusResponse } from '../types/api';

interface ErrorPayload {
  error?: string;
}

async function requestJson<T>(path: string): Promise<T> {
  const response = await fetch(path, {
    headers: {
      Accept: 'application/json',
    },
  });

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

export function fetchAnalytics(range: AnalyticsRangeKey): Promise<AnalyticsResponse> {
  return requestJson<AnalyticsResponse>(`/api/analytics?range=${encodeURIComponent(range)}`);
}

export function fetchSources(): Promise<SourcesResponse> {
  return requestJson<SourcesResponse>('/api/sources');
}

export function fetchSourceFiles(source: string): Promise<SourceFilesResponse> {
  return requestJson<SourceFilesResponse>(`/api/sources/${encodeURIComponent(source)}/files`);
}

export function fetchSourceErrors(source: string, limit = 50): Promise<SourceErrorsResponse> {
  return requestJson<SourceErrorsResponse>(
    `/api/sources/${encodeURIComponent(source)}/errors?limit=${encodeURIComponent(limit)}`
  );
}
