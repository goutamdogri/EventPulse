// Data shapes of the S2-F7 audit.

export interface PartitionOffsets {
  partition: number;
  low: number; // log start offset (moves when retention deletes data)
  high: number; // log end offset (next offset to be written)
}

export interface ApiStats {
  url: string;
  uptime_s: number;
  acked: number; // ep_kafka_messages_acked_total
  failed: number;
  http_202: number;
  by503: Record<string, number>;
  /** S3-F1/F3: ep_idem_requests_total by result (absent on a Stage 2 API). */
  idem?: IdemCounts;
}

export interface IdemCounts {
  new: number;
  replayed: number;
  in_progress: number;
  payload_mismatch: number;
  fail_open: number;
  fail_closed: number;
  disabled: number;
}

export interface ConsumerStats {
  url: string;
  uptime_s: number;
  consumed: number;
  attempted: number;
  inserted: number;
  duplicates: number;
  dlq: number;
  /** S3-F3: duplicates by cause (absent on a Stage 2 sink). reprocessed + redelivered + unknown = duplicates. */
  dup_reprocessed?: number;
  dup_redelivered?: number;
  dup_unknown?: number;
  conflicts?: number;
  /** S3-F5: records sent to a retry tier (consumed = inserted + duplicates + dlq + retried). */
  retried?: number;
}

export interface Snapshot {
  taken_at: string;
  topic: string;
  dlq_topic: string;
  group: string;
  raw: PartitionOffsets[];
  dlq: PartitionOffsets[];
  committed: { partition: number; offset: number }[]; // -1 = never committed
  /** dlq_meta_rows: dead-lettered SOURCE records (one per record, duplicates excluded); null before migration 0006. */
  db: { rows_total: number; dlq_meta_rows?: number | null };
  /** S3-F5: lag of the retry-tier group summed over the tier topics (null when there are no tier topics). */
  retry_lag?: number | null;
  api: ApiStats | null;
  consumers: ConsumerStats[];
  notes: string[];
}

export interface K6Summary {
  run_id?: string;
  sent: number;
  by_status: { '202': number; '4xx': number; '503': number; other: number };
  network_errors: number;
  retries_sent?: number;
  /** Stage 3: distinct events (iterations) that got at least one 202, and the resend counters. */
  unique_202?: number;
  replayed_202?: number;
  status_409?: number;
  status_422?: number;
  resends_sent?: number;
  dropped_iterations?: number;
  latency_ms?: { p50: number | null; p99: number | null; max: number | null };
}

/** Facts measured once at the end of the run. */
export interface RunFacts {
  run_id: string;
  rows_distinct: number; // distinct rows with this run's event_id prefix
  freshness_p50_s: number | null;
  freshness_p99_s: number | null;
  dlq_new_total: number; // DLQ records added during the run (any origin, e.g. injected poison)
  dlq_run: number; // ... of which carry this run's event_id prefix (API-acked events that were dead-lettered)
  lag_end: number; // group lag summed over partitions after the drain wait
  drained: boolean;
  drain_waited_s: number;
}

export type CheckStatus = 'ok' | 'fail' | 'finding' | 'skipped' | 'not_drained';

export interface Check {
  id: string; // A..I
  title: string;
  status: CheckStatus;
  critical: boolean; // a failed critical check means acknowledged data may be lost (or the numbers are invalid)
  detail: string;
}

export interface AuditResult {
  run_id: string;
  result: 'RECONCILED' | 'NOT RECONCILED' | 'NOT DRAINED';
  exit_code: 0 | 1 | 2;
  numbers: Record<string, number | null>;
  checks: Check[];
  findings: string[];
}
