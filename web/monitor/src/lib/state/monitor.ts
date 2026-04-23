import { writable } from 'svelte/store';
import type { AnalyticsRangeKey } from '../types/api';

export const analyticsRangeStore = writable<AnalyticsRangeKey>('6h');
