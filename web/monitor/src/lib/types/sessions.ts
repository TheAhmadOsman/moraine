export type SessionStatus = 'active' | 'completed' | 'cancelled' | 'error';

export type HarnessId =
  | 'claude-code'
  | 'codex'
  | 'hermes'
  | 'cursor'
  | 'aider'
  | 'cli'
  | 'custom';

export interface Harness {
  id: HarnessId | string;
  label: string;
  short: string;
  hue: number;
}

export interface UserStep {
  kind: 'user';
  at: number;
  text: string;
}

export interface AssistantStep {
  kind: 'assistant';
  at: number;
  text: string;
  tokens?: number;
}

export interface ThinkingStep {
  kind: 'thinking';
  at: number;
  text: string;
  durationMs?: number;
}

export interface ToolCallStep {
  kind: 'tool_call';
  at: number;
  tool: string;
  args: Record<string, unknown>;
  latencyMs: number;
  result: string;
  resultAt: number;
  status: 'ok' | 'error';
  callId?: string;
}

export type Step = UserStep | AssistantStep | ThinkingStep | ToolCallStep;

export interface Turn {
  idx: number;
  model: string;
  startedAt: number;
  endedAt: number;
  durationMs: number;
  promptTokens: number;
  completionTokens: number;
  totalTokens: number;
  toolCalls: number;
  steps: Step[];
  finishReason?: string;
}

export interface Session {
  id: string;
  title: string;
  previewText: string;
  harness: Harness;
  startedAt: number;
  endedAt: number;
  durationMs: number;
  status: SessionStatus;
  models: string[];
  turnCount: number;
  turns: Turn[];
  totalTokens: number;
  totalToolCalls: number;
  tags: string[];
  traceId: string;
  hasDetail: boolean;
}

export type SessionsSinceKey = '1h' | '6h' | '24h' | '7d' | '30d' | '90d' | 'all';

export interface SessionsMeta {
  requestedLimit: number;
  effectiveLimit: number;
  loadedCount: number;
  hasMore: boolean;
  sinceSeconds: number;
  nextCursor: string | null;
}

export interface SessionsResponse {
  ok: boolean;
  sessions: Session[];
  meta?: {
    requested_limit?: number;
    effective_limit?: number;
    loaded_count?: number;
    has_more?: boolean;
    since_seconds?: number;
    next_cursor?: string | null;
  };
  models?: string[];
  harnesses?: Harness[];
  error?: string;
}

export type TurnVizVariant = 'chat' | 'timeline' | 'trace' | 'document';

export interface SessionsFilter {
  query: string;
  model: string;
  status: string;
  harness: string;
}
