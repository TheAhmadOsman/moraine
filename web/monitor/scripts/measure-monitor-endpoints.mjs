#!/usr/bin/env node

import { performance } from 'node:perf_hooks';

const rawBaseUrl = process.env.MONITOR_BASE_URL || 'http://127.0.0.1:8080';
const baseUrl = rawBaseUrl.replace(/\/+$/, '').replace(/\/monitor$/, '');
const sampleCount = Math.max(1, Number.parseInt(process.env.MONITOR_LATENCY_SAMPLES || '5', 10) || 5);
const sessionId = process.env.MONITOR_SESSION_ID || '';

const endpoints = [
  { label: 'status', path: '/api/status' },
  { label: 'sources', path: '/api/sources' },
  { label: 'analytics-6h', path: '/api/analytics?range=6h' },
  { label: 'analytics-24h', path: '/api/analytics?range=24h' },
  { label: 'sessions', path: '/api/sessions?since=30d&limit=25' },
];

if (sessionId) {
  endpoints.push({
    label: 'session-detail',
    path: `/api/sessions/${encodeURIComponent(sessionId)}?turn_limit=50`,
  });
}

function quantile(values, q) {
  if (values.length === 0) {
    return 0;
  }
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil(sorted.length * q) - 1));
  return sorted[index];
}

async function measureEndpoint(path) {
  const totals = [];
  let lastStatus = 0;
  let lastBytes = 0;

  for (let i = 0; i < sampleCount; i += 1) {
    const started = performance.now();
    const response = await fetch(`${baseUrl}${path}`, {
      headers: {
        accept: 'application/json, text/html;q=0.9, */*;q=0.8',
      },
    });
    const body = await response.arrayBuffer();
    const finished = performance.now();
    lastStatus = response.status;
    lastBytes = body.byteLength;
    totals.push(finished - started);
  }

  return {
    status: lastStatus,
    bytes: lastBytes,
    p50_ms: quantile(totals, 0.5),
    p95_ms: quantile(totals, 0.95),
    max_ms: Math.max(...totals),
  };
}

async function main() {
  console.log('moraine_monitor_endpoint_bench');
  console.log(`base_url=${baseUrl}`);
  console.log(`samples=${sampleCount}`);
  if (sessionId) {
    console.log(`session_id=${sessionId}`);
  }

  for (const endpoint of endpoints) {
    const result = await measureEndpoint(endpoint.path);
    console.log(
      [
        endpoint.label.padEnd(16, ' '),
        `status=${result.status}`,
        `p50_ms=${result.p50_ms.toFixed(1)}`,
        `p95_ms=${result.p95_ms.toFixed(1)}`,
        `max_ms=${result.max_ms.toFixed(1)}`,
        `bytes=${result.bytes}`,
        `path=${endpoint.path}`,
      ].join(' '),
    );
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exit(1);
});
