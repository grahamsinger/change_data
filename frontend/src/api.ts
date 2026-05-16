// All requests go through the api service. In dev we point at localhost:8000
// directly (the container exposes it).
export const API_BASE = (import.meta.env.VITE_API_URL as string) || "http://localhost:8000";

export async function jget<T>(path: string): Promise<T> {
  const r = await fetch(`${API_BASE}${path}`);
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json();
}

export async function jpost<T>(path: string, body: unknown): Promise<T> {
  const r = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!r.ok) throw new Error(`${r.status} ${await r.text()}`);
  return r.json();
}

export function eventsWebSocketURL(): string {
  const u = new URL(API_BASE);
  u.protocol = u.protocol === "https:" ? "wss:" : "ws:";
  u.pathname = "/ws/events";
  return u.toString();
}

export type TraceEvent = {
  ts?: number;
  service: "api" | "mysql" | "search" | "analytics" | string;
  type: string;
  payload?: Record<string, unknown>;
  correlation_id?: string | number | null;
};

export type Article = {
  id: number;
  title: string;
  body: string;
  kind: "poem" | "article";
  published_at: string;
  author: string;
};

export type Stats = {
  total_articles: number;
  unique_authors: number;
  kinds: Record<string, number>;
  top_authors: { author: string; count: number }[];
  rate_last_60s: number;
  rate_last_300s: number;
  // Batch metrics from the analytics consumer (post-pipelining):
  events_processed_total?: number;
  state_writes_total?: number;
  state_write_failures?: number;
  events_per_sec?: number;
  state_writes_per_sec?: number;
  trace_batch_size?: number;
  trace_batch_age_ms?: number;
  state_flush_interval_ms?: number;
};

export type SearchHit = {
  id: number;
  title: string;
  author: string;
  kind: string;
  published_at?: string;
};

export type SearchResp = {
  hits: SearchHit[];
  query: string;
  processing_ms?: number;
};

export type SimStatus = {
  enabled: boolean;
  interval_seconds: number;
  workers: number;
  // Diagnostics from the queue+flusher (added when bulk-publish landed):
  target_rate_per_sec?: number;
  actual_rate_per_sec?: number;
  batches_per_sec?: number;
  queue_depth?: number;
  queue_maxsize?: number;
  last_batch_size?: number;
  last_batch_ms?: number;
  inserts_total?: number;
  batches_total?: number;
  dropped_total?: number;
  flush_interval_ms?: number;
};

export type TopicSizes = Record<string, { offsets_written: number; records_kept: number }>;

export type LagBreakdown = {
  mysql_count: number;
  kafka_end_offset: number;
  consumer_committed_offset: number;
  meili_docs_count: number;
  debezium_lag: number;
  consumer_lag: number;
  meili_lag: number;
  total_lag: number;
};

export type SearchStats = {
  is_indexing: boolean;
  documents_count: number;
  pending_tasks: number;
  mysql_articles_count: number;
  lag: number;
  search_reachable: boolean;
  // Per-stage lag breakdown derived from the same snapshot as `lag`,
  // so the topline pill and the stage bar can never disagree.
  kafka_end_offset?: number;
  consumer_committed_offset?: number;
  debezium_lag?: number;
  consumer_lag?: number;
  meili_lag?: number;
  total_lag?: number;
  // Batching metrics from the search consumer:
  pending_docs?: number;
  last_batch_size?: number;
  last_batch_ms?: number;
  batches_per_sec?: number;
  docs_indexed_per_sec?: number;
  avg_batch_size?: number;
  max_batch_size?: number;
  max_batch_age_ms?: number;
};
