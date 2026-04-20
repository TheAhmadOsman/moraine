export type AnalyticsRangeKey = '15m' | '1h' | '6h' | '24h' | '7d' | '30d';

export interface ApiError {
  error?: string;
}

export interface ConnectionStats {
  total?: number | null;
  error?: string | null;
}

export interface HealthResponse {
  ok: boolean;
  url?: string;
  database?: string;
  version?: string | null;
  ping_ms?: number | null;
  connections?: ConnectionStats;
  error?: string;
}

export interface IngestorLatest {
  queue_depth?: number | null;
  files_active?: number | null;
  files_watched?: number | null;
  [key: string]: unknown;
}

export interface IngestorStatus {
  present: boolean;
  alive: boolean;
  latest: IngestorLatest | null;
  age_seconds: number | null;
}

export interface StatusResponse {
  ok: boolean;
  ingestor?: IngestorStatus;
  error?: string;
}

export interface AnalyticsRange {
  key: AnalyticsRangeKey;
  label: string;
  window_seconds: number;
  bucket_seconds: number;
  from_unix: number;
  to_unix: number;
}

export interface TokenPoint {
  bucket_unix: number;
  model: string;
  tokens: number;
}

export interface TurnPoint {
  bucket_unix: number;
  model: string;
  turns: number;
}

export interface ConcurrentSessionsPoint {
  bucket_unix: number;
  concurrent_sessions: number;
}

export interface AnalyticsSeries {
  tokens: TokenPoint[];
  turns: TurnPoint[];
  concurrent_sessions: ConcurrentSessionsPoint[];
}

export interface AnalyticsResponse {
  ok: boolean;
  range: AnalyticsRange;
  series: AnalyticsSeries;
  error?: string;
}

export type SourceHealthStatus = 'disabled' | 'ok' | 'warning' | 'error' | 'unknown';

export interface SourceHealth {
  name: string;
  harness: string;
  format: string;
  enabled: boolean;
  glob: string;
  watch_root: string;
  status: SourceHealthStatus;
  checkpoint_count: number;
  latest_checkpoint_at: string | null;
  raw_event_count: number;
  ingest_error_count: number;
  latest_error_at: string | null;
  latest_error_kind: string | null;
  latest_error_text: string | null;
}

export interface SourcesResponse {
  ok: boolean;
  sources: SourceHealth[];
  query_error?: string;
}
