import type { AnalyticsRangeKey } from './types/api';

export interface AnalyticsRangeDetails {
  label: string;
  window_seconds: number;
  bucket_seconds: number;
}

export const ANALYTICS_RANGE_DETAILS: Record<AnalyticsRangeKey, AnalyticsRangeDetails> = {
  '15m': {
    label: 'Last 15m',
    window_seconds: 15 * 60,
    bucket_seconds: 60,
  },
  '1h': {
    label: 'Last 1h',
    window_seconds: 60 * 60,
    bucket_seconds: 5 * 60,
  },
  '6h': {
    label: 'Last 6h',
    window_seconds: 6 * 60 * 60,
    bucket_seconds: 15 * 60,
  },
  '24h': {
    label: 'Last 24h',
    window_seconds: 24 * 60 * 60,
    bucket_seconds: 60 * 60,
  },
  '7d': {
    label: 'Last 7d',
    window_seconds: 7 * 24 * 60 * 60,
    bucket_seconds: 6 * 60 * 60,
  },
  '30d': {
    label: 'Last 30d',
    window_seconds: 30 * 24 * 60 * 60,
    bucket_seconds: 24 * 60 * 60,
  },
  '90d': {
    label: 'Last 90d',
    window_seconds: 90 * 24 * 60 * 60,
    bucket_seconds: 3 * 24 * 60 * 60,
  },
  '180d': {
    label: 'Last 180d',
    window_seconds: 180 * 24 * 60 * 60,
    bucket_seconds: 7 * 24 * 60 * 60,
  },
  '365d': {
    label: 'Last 365d',
    window_seconds: 365 * 24 * 60 * 60,
    bucket_seconds: 14 * 24 * 60 * 60,
  },
};

export const ANALYTICS_RANGES: AnalyticsRangeKey[] = [
  '15m',
  '1h',
  '6h',
  '24h',
  '7d',
  '30d',
  '90d',
  '180d',
  '365d',
];

export const MODEL_COLORS = [
  '#155e75',
  '#3b82f6',
  '#0f766e',
  '#b45309',
  '#7c3aed',
  '#e11d48',
  '#2563eb',
  '#059669',
  '#4f46e5',
  '#0891b2',
];

export const THEME_STORAGE_KEY = 'moraine-monitor-theme';

export const FAST_POLL_INTERVAL_MS = 10_000;
export const SLOW_POLL_INTERVAL_MS = 60_000;

export const DEFAULT_ANALYTICS_META = 'Loading model analytics…';

export const ROW_LIMIT_OPTIONS = [10, 25, 50, 100];
