// Sink counters and metrics (S2-F2..F6). Every value is a Prometheus series (/metrics) AND a field of
// /debug/stats (snapshot()). Works without a registry too (unit tests), using a private one.
//
// Counting rule (S2-F6): consumed / attempted / inserted / dlq are added together when a chunk COMPLETES (rows
// committed in Postgres and dead letters acknowledged by the DLQ). So, exactly:
//     consumed = inserted + duplicates + dlq        (duplicates = attempted - inserted)
// A chunk attempt that fails (DB down, poison with the DLQ off, DLQ publish failure, revoked partition) counts in
// chunk_failures_by_reason instead and its messages are counted when the retry completes. A failed attempt's
// partial writes then show up as duplicates on the retry.
//
// Labels stay low-cardinality: partition numbers, error classes, reasons. Never ids or raw messages.

import type { CauseSplit } from '../sink/progress.js';
import { DUPLICATE_CAUSES } from '../sink/progress.js';
import { Counter, Gauge, Histogram, Registry } from '@prometheus-io/client';
import type { ErrorClass } from '../sink/errors.js';
import { histQuantile, round } from './hist-quantile.js';

const SECONDS_BUCKETS = [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30];
const FRESHNESS_BUCKETS = [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10, 30, 60, 300, 1800, 3600];
const SIZE_BUCKETS = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000];

export interface PartitionLag {
  partition: number;
  end: number;
  committed: number; // -1 = the group never committed this partition
  lag: number;
}

export type ChunkFailure = 'poison_blocked' | 'db_unavailable' | 'registry_unavailable' | 'dlq_publish' | 'commit' | 'other';

export interface ChunkCompletion {
  consumed: number;
  attempted: number;
  inserted: number;
  dlq: { errorClass: ErrorClass }[];
  /** S3-F3: the duplicates (attempted - inserted) split by cause; reprocessed + redelivered + unknown = duplicates. */
  causes?: CauseSplit;
  /** Messages whose offset was already stored once for this partition. */
  reprocessed?: number;
  partition?: number;
}

export class SinkStats {
  readonly startedAt = Date.now();
  consumed = 0;
  attempted = 0;
  inserted = 0;
  batches = 0;
  chunks = 0;
  batchErrors = 0;
  commits = 0;
  commitErrors = 0;
  lastCommitMs?: number;
  stoppedEarly = 0;
  rebalances = 0;
  private revokedAtMs: number | undefined;
  dlq = 0;
  dlqErrors = 0;
  isolations = 0;
  pauses = 0;
  // PERMANENT_PARSE / PERMANENT_DB / UNKNOWN: one per bad record (counted once, when dead-lettered or blocked).
  // TRANSIENT_DB / SYSTEMIC_DB: one per failed write attempt of a chunk (the DB was down, no record is at fault).
  readonly errorsByClass: Partial<Record<ErrorClass, number>> = {};
  readonly retriesByClass: Partial<Record<ErrorClass, number>> = {};
  readonly dlqByClass: Partial<Record<ErrorClass, number>> = {};
  readonly chunkFailures: Partial<Record<ChunkFailure, number>> = {};
  /** S3-F3: duplicates by cause, and reprocessed messages per partition. */
  readonly duplicatesByCause: CauseSplit = { reprocessed: 0, redelivered: 0, unknown: 0 };
  readonly reprocessedByPartition: Record<string, number> = {};
  /** S3-F7: Avro deserialization failures by reason; writer schema ids seen (id -> last seen ms). */
  readonly deserializeFailures: Record<string, number> = {};
  readonly schemaIdsSeen = new Map<number, number>();
  registryUnavailable = 0;
  reprocessedMessages = 0;
  /** S3-F2: duplicates whose content differs from the stored row (same key, other content). Never overwritten. */
  conflicts = 0;
  conflictCheckErrors = 0;
  lastConflict?: { at: string; tenant_id: string; event_id: string };
  readonly paused = new Set<string>();
  readonly assigned = new Set<string>();
  readonly localLag: Record<string, number> = {}; // per partition, from the last batch (cheap, this member only)
  lastBatchAt?: number;
  lastError?: { at: string; message: string };
  lag?: { at: string; total: number; byPartition: PartitionLag[]; error?: string };

  private readonly m: ReturnType<typeof createMetrics>;

  constructor(registry: Registry = new Registry(), live?: () => { pgTotal: number; pgIdle: number; pgWaiting: number }) {
    this.m = createMetrics(registry, this, live);
    // Every catalogue series exists from startup (value 0), so dashboards and the audit never miss a label.
    for (const cause of DUPLICATE_CAUSES) this.m.duplicatesByCause.inc({ cause }, 0);
  }

  /** Reprocessed messages absorbed by ON CONFLICT DO NOTHING. */
  get duplicates() {
    return this.attempted - this.inserted;
  }

  completeChunk(c: ChunkCompletion) {
    this.chunks++;
    this.consumed += c.consumed;
    this.attempted += c.attempted;
    this.inserted += c.inserted;
    this.m.consumed.inc(c.consumed);
    this.m.inserted.inc(c.inserted);
    this.m.duplicates.inc(c.attempted - c.inserted);
    this.m.rowsAttempted.inc(c.attempted);
    this.m.rowsInserted.inc(c.inserted);
    // Chunks without a split (dedup off, unit tests) count their duplicates as "unknown": the causes always add up.
    const causes = c.causes ?? { reprocessed: 0, redelivered: 0, unknown: c.attempted - c.inserted };
    for (const cause of DUPLICATE_CAUSES) {
      if (causes[cause] === 0) continue;
      this.duplicatesByCause[cause] += causes[cause];
      this.m.duplicatesByCause.inc({ cause }, causes[cause]);
    }
    if (c.reprocessed) {
      const p = String(c.partition ?? 'unknown');
      this.reprocessedMessages += c.reprocessed;
      this.reprocessedByPartition[p] = (this.reprocessedByPartition[p] ?? 0) + c.reprocessed;
      this.m.reprocessed.inc({ partition: p }, c.reprocessed);
    }
    for (const d of c.dlq) {
      this.dlq++;
      this.dlqByClass[d.errorClass] = (this.dlqByClass[d.errorClass] ?? 0) + 1;
      this.m.dlq.inc({ error_class: d.errorClass });
    }
  }

  observeRegistry(op: string, ok: boolean, seconds: number) {
    this.m.registryRequests.inc({ op, result: ok ? 'ok' : 'error' });
    this.m.registryLatency.observe({ op }, seconds);
  }

  countDeserializeFailure(reason: string) {
    this.deserializeFailures[reason] = (this.deserializeFailures[reason] ?? 0) + 1;
    this.m.deserializeFailures.inc({ reason });
  }

  noteSchemaId(id: number) {
    this.schemaIdsSeen.set(id, Date.now());
  }

  /** Writer schema ids seen in the last 10 minutes (ep_schema_ids_in_use). */
  schemaIdsInUse(windowMs = 600_000): number[] {
    const since = Date.now() - windowMs;
    return [...this.schemaIdsSeen.entries()].filter(([, t]) => t >= since).map(([id]) => id).sort((a, b) => a - b);
  }

  countConflicts(keys: { tenant_id: string; event_id: string }[]) {
    this.conflicts += keys.length;
    this.m.conflicts.inc(keys.length);
    const k = keys[keys.length - 1];
    this.lastConflict = { at: new Date().toISOString(), tenant_id: k.tenant_id, event_id: k.event_id };
  }

  failChunk(reason: ChunkFailure) {
    this.chunkFailures[reason] = (this.chunkFailures[reason] ?? 0) + 1;
    this.m.chunkFailures.inc({ reason });
  }

  observeBatch(size: number) {
    this.batches++;
    this.m.batchSize.observe({ kind: 'batch' }, size);
  }

  observeChunk(size: number, processingMs: number) {
    this.m.batchSize.observe({ kind: 'chunk' }, size);
    this.m.processing.observe(processingMs / 1000);
  }

  observeDbWrite(ms: number, connectMs: number, freshnessS: number[]) {
    this.m.dbWrite.observe(ms / 1000);
    this.m.pgConnectWait.observe(connectMs / 1000);
    for (const f of freshnessS) this.m.freshness.observe(f);
  }

  observeCommit(ms: number) {
    this.commits++;
    this.lastCommitMs = ms;
    this.m.commit.observe(ms / 1000);
  }

  countCommitError() {
    this.commitErrors++;
    this.m.commitErrors.inc();
  }

  countRebalance() {
    this.rebalances++;
    this.m.rebalances.inc();
  }

  /** K8: a revoke callback arrived. The gap timer runs until the next assign callback. */
  countRevoke(partitions: number, nowMs: number = Date.now()) {
    this.m.revokes.inc();
    this.m.revokedPartitions.inc(partitions);
    this.revokedAtMs ??= nowMs;
  }

  /** K8: an assign callback arrived; closes the gap opened by a revoke (nothing is recorded for the first join). */
  closeRebalanceGap(nowMs: number = Date.now()) {
    if (this.revokedAtMs === undefined) return;
    this.m.rebalanceGap.observe((nowMs - this.revokedAtMs) / 1000);
    this.revokedAtMs = undefined;
  }

  countError(c: ErrorClass, n = 1) {
    this.errorsByClass[c] = (this.errorsByClass[c] ?? 0) + n;
    this.m.errors.inc({ class: c }, n);
  }

  countRetry(c: ErrorClass) {
    this.retriesByClass[c] = (this.retriesByClass[c] ?? 0) + 1;
    this.m.retries.inc({ class: c });
  }

  setLag(byPartition: PartitionLag[], error?: string) {
    this.lag = { at: new Date().toISOString(), total: byPartition.reduce((a, p) => a + p.lag, 0), byPartition, error };
  }

  setLocalLag(partition: number, lag: number) {
    if (Number.isFinite(lag) && lag >= 0) this.localLag[String(partition)] = lag;
  }

  async snapshot() {
    return {
      uptime_s: Math.round((Date.now() - this.startedAt) / 1000),
      assigned_partitions: [...this.assigned].sort(),
      lag: this.lag
        ? {
            total: this.lag.total,
            by_partition: Object.fromEntries(this.lag.byPartition.map((p) => [p.partition, p.lag])),
            last_committed: Object.fromEntries(this.lag.byPartition.map((p) => [p.partition, p.committed])),
            polled_at: this.lag.at,
            error: this.lag.error ?? null,
          }
        : null,
      local_lag: { ...this.localLag },
      consumed: this.consumed,
      attempted: this.attempted,
      inserted: this.inserted,
      duplicates: this.duplicates,
      schema: {
        ids_in_use: this.schemaIdsInUse(),
        deserialize_failures: { ...this.deserializeFailures },
        registry_unavailable: this.registryUnavailable,
      },
      duplicates_by_cause: { ...this.duplicatesByCause },
      reprocessed_messages: this.reprocessedMessages,
      reprocessed_by_partition: { ...this.reprocessedByPartition },
      conflicting_duplicates: this.conflicts,
      conflict_check_errors: this.conflictCheckErrors,
      last_conflict: this.lastConflict ?? null,
      dlq: this.dlq,
      dlq_by_class: { ...this.dlqByClass },
      dlq_errors: this.dlqErrors,
      errors_by_class: { ...this.errorsByClass },
      retries_by_class: { ...this.retriesByClass },
      chunk_failures_by_reason: { ...this.chunkFailures },
      batches: this.batches,
      chunks: this.chunks,
      batch_errors: this.batchErrors,
      commits: this.commits,
      commit_errors: this.commitErrors,
      last_commit_ms: this.lastCommitMs ?? null,
      stopped_early: this.stoppedEarly,
      rebalances: this.rebalances,
      isolations: this.isolations,
      pauses: this.pauses,
      paused: this.paused.size > 0,
      paused_partitions: [...this.paused].sort(),
      batch: {
        avg_size: this.chunks ? round(this.consumed / this.chunks, 1) : 0,
        processing_p99_ms: ms(await histQuantile(this.m.processing, 0.99)),
        db_write_p99_ms: ms(await histQuantile(this.m.dbWrite, 0.99)),
        commit_p99_ms: ms(await histQuantile(this.m.commit, 0.99)),
      },
      freshness_p50_s: round(await histQuantile(this.m.freshness, 0.5)),
      freshness_p99_s: round(await histQuantile(this.m.freshness, 0.99)),
      last_batch_at: this.lastBatchAt ? new Date(this.lastBatchAt).toISOString() : null,
      last_error: this.lastError ?? null,
    };
  }
}

const ms = (s: number | null) => (s === null ? null : round(s * 1000, 2));

function createMetrics(registry: Registry, st: SinkStats, live?: () => { pgTotal: number; pgIdle: number; pgWaiting: number }) {
  const registers = [registry];
  const gauge = (name: string, help: string, read: () => number) =>
    new Gauge({ name, help, registers, collect() { this.set(read()); } });

  new Gauge({
    name: 'ep_consumer_lag',
    help: 'Log end offset - committed offset of the group, per partition (polled every LAG_POLL_MS; same value on every member).',
    labelNames: ['partition'],
    registers,
    collect() {
      this.reset();
      for (const p of st.lag?.byPartition ?? []) this.set({ partition: String(p.partition) }, p.lag);
    },
  });
  new Gauge({
    name: 'ep_consumer_last_committed_offset',
    help: 'Committed offset of the group per partition (-1 = never committed).',
    labelNames: ['partition'],
    registers,
    collect() {
      this.reset();
      for (const p of st.lag?.byPartition ?? []) this.set({ partition: String(p.partition) }, p.committed);
    },
  });
  gauge('ep_consumer_assigned_partitions', 'Partitions this member owns.', () => st.assigned.size);
  gauge('ep_consumer_paused', 'Partitions of this member paused because the database is unavailable.', () => st.paused.size);
  if (live) {
    gauge('ep_pg_pool_total', 'pg pool: connections open.', () => live().pgTotal);
    gauge('ep_pg_pool_idle', 'pg pool: idle connections.', () => live().pgIdle);
    gauge('ep_pg_pool_waiting', 'pg pool: callers waiting for a connection.', () => live().pgWaiting);
  }

  const metrics = {
    consumed: new Counter({ name: 'ep_consumer_messages_consumed_total', help: 'Messages of completed chunks (reprocessed ones included).', registers }),
    inserted: new Counter({ name: 'ep_consumer_rows_inserted_total', help: 'Rows actually inserted (rowCount).', registers }),
    duplicates: new Counter({ name: 'ep_consumer_duplicates_total', help: 'Rows absorbed by ON CONFLICT DO NOTHING (reprocessing made visible).', registers }),
    rowsAttempted: new Counter({ name: 'ep_sink_rows_attempted_total', help: 'Rows sent to INSERT (Stage 3 catalogue name; = inserted + duplicates).', registers }),
    rowsInserted: new Counter({ name: 'ep_sink_rows_inserted_total', help: 'Rows actually inserted (Stage 3 catalogue name).', registers }),
    duplicatesByCause: new Counter({
      name: 'ep_sink_duplicates_total',
      help: 'Duplicate rows absorbed by the (tenant_id, event_id) key, by cause: reprocessed (offset already stored: crash, rebalance, offset reset) | redelivered (new offset, existing key: client retry past the API, replay) | unknown.',
      labelNames: ['cause'],
      registers,
    }),
    registryRequests: new Counter({ name: 'ep_schema_registry_requests_total', help: 'Schema Registry requests by operation and result.', labelNames: ['op', 'result'], registers }),
    registryLatency: new Histogram({ name: 'ep_schema_registry_latency_seconds', help: 'Schema Registry request latency.', labelNames: ['op'], buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5], registers }),
    deserializeFailures: new Counter({
      name: 'ep_schema_deserialize_failures_total',
      help: 'Records whose value could not be deserialized, by reason: not_avro | unknown_schema_id | incompatible_schema | avro_decode | json_not_accepted.',
      labelNames: ['reason'],
      registers,
    }),
    schemaIdsInUse: new Gauge({
      name: 'ep_schema_ids_in_use',
      help: 'Distinct writer schema ids seen in the last 10 minutes.',
      registers,
      collect() {
        this.set(st.schemaIdsInUse().length);
      },
    }),
    reprocessed: new Counter({ name: 'ep_sink_reprocessed_messages_total', help: 'Messages whose offset was already stored once (processed again after a restart / rebalance / reset).', labelNames: ['partition'], registers }),
    conflicts: new Counter({ name: 'ep_sink_conflicting_duplicates_total', help: 'Duplicate keys whose content_hash differs from the stored row (S3-F2; never overwritten).', registers }),
    dlq: new Counter({ name: 'ep_consumer_dlq_total', help: 'Records published to the DLQ, by error class.', labelNames: ['error_class'], registers }),
    chunkFailures: new Counter({ name: 'ep_consumer_chunk_failures_total', help: 'Chunk attempts that failed and will be retried, by reason.', labelNames: ['reason'], registers }),
    errors: new Counter({ name: 'ep_consumer_errors_total', help: 'Bad records (permanent classes) / failed write attempts (transient, systemic), by class.', labelNames: ['class'], registers }),
    retries: new Counter({ name: 'ep_consumer_retries_total', help: 'Retries, by error class.', labelNames: ['class'], registers }),
    commitErrors: new Counter({ name: 'ep_consumer_commit_errors_total', help: 'Failed offset commit attempts.', registers }),
    rebalances: new Counter({ name: 'ep_consumer_rebalances_total', help: 'Partition assignments received (group joins).', registers }),
    // K8: what a rebalance takes away from this member and how long it was without partitions.
    revokes: new Counter({ name: 'ep_consumer_revokes_total', help: 'Revoke callbacks received (a rebalance took partitions away from this member).', registers }),
    revokedPartitions: new Counter({ name: 'ep_consumer_partitions_revoked_total', help: 'Partitions revoked from this member (eager revokes all it owns, cooperative only the moved ones).', registers }),
    rebalanceGap: new Histogram({ name: 'ep_consumer_rebalance_gap_seconds', help: 'Time from a revoke callback to the next assign callback of this member.', buckets: SECONDS_BUCKETS, registers }),
    batchSize: new Histogram({ name: 'ep_consumer_batch_size_messages', help: 'Messages per Kafka batch (kind=batch) and per chunk (kind=chunk).', labelNames: ['kind'], buckets: SIZE_BUCKETS, registers }),
    processing: new Histogram({ name: 'ep_consumer_batch_processing_seconds', help: 'Whole chunk: parse + write + DLQ + offset commit.', buckets: SECONDS_BUCKETS, registers }),
    dbWrite: new Histogram({ name: 'ep_consumer_db_write_seconds', help: 'One write transaction (BEGIN..COMMIT).', buckets: SECONDS_BUCKETS, registers }),
    pgConnectWait: new Histogram({ name: 'ep_pg_pool_connect_wait_seconds', help: 'Time to get a pool connection.', buckets: SECONDS_BUCKETS, registers }),
    commit: new Histogram({ name: 'ep_consumer_commit_seconds', help: 'Offset commit latency (successful commits).', buckets: SECONDS_BUCKETS, registers }),
    freshness: new Histogram({ name: 'ep_event_freshness_seconds', help: 'End-to-end visibility: DB commit time - received_at (API), per newly inserted row.', buckets: FRESHNESS_BUCKETS, registers }),
  };
  // Known label values start at 0, so a clean run shows 0 on the dashboard instead of "No data".
  const classes = ['PERMANENT_PARSE', 'PERMANENT_DB', 'TRANSIENT_DB', 'SYSTEMIC_DB', 'UNKNOWN'];
  for (const c of classes) {
    metrics.errors.inc({ class: c }, 0);
    metrics.retries.inc({ class: c }, 0);
  }
  for (const c of ['PERMANENT_PARSE', 'PERMANENT_DB', 'UNKNOWN']) metrics.dlq.inc({ error_class: c }, 0);
  for (const r of ['poison_blocked', 'db_unavailable', 'dlq_publish', 'commit', 'other']) metrics.chunkFailures.inc({ reason: r }, 0);
  return metrics;
}
