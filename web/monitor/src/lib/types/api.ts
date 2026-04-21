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
  latest_checkpoint_age_seconds: number | null;
  raw_event_count: number;
  ingest_error_count: number;
  latest_error_at: string | null;
  latest_error_kind: string | null;
  latest_error_text: string | null;
}

export type SourceLagIndicator = 'healthy' | 'delayed' | 'stale' | 'unknown';

export interface SourceRuntime {
  latest_heartbeat_at: string | null;
  latest_heartbeat_age_seconds: number | null;
  queue_depth: number | null;
  files_active: number | null;
  files_watched: number | null;
  append_to_visible_p50_ms: number | null;
  append_to_visible_p95_ms: number | null;
  watcher_backend: string | null;
  watcher_error_count: number | null;
  watcher_reset_count: number | null;
  watcher_last_reset_at: string | null;
  heartbeat_cadence_seconds: number;
  reconcile_cadence_seconds: number;
  lag_indicator: SourceLagIndicator | null;
}

export type SourceWarningKind = 'file_state' | 'ingest_heartbeat' | 'watcher';
export type SourceWarningSeverity = 'warning' | 'error';

export interface SourceDetailWarning {
  kind: SourceWarningKind;
  severity: SourceWarningSeverity;
  summary: string;
}

export interface SourcesResponse {
  ok: boolean;
  sources: SourceHealth[];
  query_error?: string;
}

export interface SourceDetailResponse {
  ok: boolean;
  source: SourceHealth;
  runtime: SourceRuntime;
  runtime_query_error?: string;
  warnings: SourceDetailWarning[];
  query_error?: string;
}

export interface SourceFileRow {
  path: string;
  size_bytes: number;
  modified_at: string | null;
  checkpoint_offset: number | null;
  checkpoint_line_no: number | null;
  checkpoint_status: string | null;
  checkpoint_updated_at: string | null;
  raw_event_count: number;
  latest_error_at: string | null;
  latest_error_kind: string | null;
  latest_error_text: string | null;
}

export interface SourceFilesResponse {
  ok: boolean;
  source_name: string;
  watch_root: string;
  glob: string;
  files: SourceFileRow[];
  glob_match_count: number;
  fs_error?: string;
  query_error?: string;
}

export interface SourceErrorRow {
  ingested_at: string;
  source_file: string;
  source_line_no: number;
  source_offset: number;
  error_kind: string;
  error_text: string;
  raw_fragment: string;
}

export interface SourceErrorsResponse {
  ok: boolean;
  source_name: string;
  errors: SourceErrorRow[];
  query_error?: string;
}
