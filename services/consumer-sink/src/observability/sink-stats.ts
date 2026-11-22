// Plain in-process counters for the sink, shown on /debug/stats.
// S2-F6 turns these into Prometheus series and adds lag, latency histograms, freshness, etc.

import type { ErrorClass } from '../sink/errors.js';

export class SinkStats {
  readonly startedAt = Date.now();
  consumed = 0; // every message handed to us, reprocessed ones included
  attempted = 0; // rows sent to INSERT and committed (isolation sub-writes included)
  inserted = 0; // rows actually inserted (rowCount)
  batches = 0;
  chunks = 0;
  batchErrors = 0;
  commits = 0; // successful offset commits (one per chunk)
  commitErrors = 0;
  lastCommitMs?: number;
  stoppedEarly = 0; // batches cut short by shutdown or a revoked partition
  rebalances = 0;
  // S2-F4
  dlq = 0; // records published to the DLQ
  dlqErrors = 0; // failed DLQ publishes (chunk retried)
  isolations = 0; // chunks split to find the bad rows
  pauses = 0; // partition pauses because the DB was down
  // PERMANENT_PARSE / PERMANENT_DB / UNKNOWN: one per bad record (counted once, when dead-lettered or blocked).
  // TRANSIENT_DB / SYSTEMIC_DB: one per failed write attempt of a chunk (the DB was down, no record is at fault).
  readonly errorsByClass: Partial<Record<ErrorClass, number>> = {};
  readonly retriesByClass: Partial<Record<ErrorClass, number>> = {};
  readonly dlqByClass: Partial<Record<ErrorClass, number>> = {};
  readonly paused = new Set<string>(); // "topic:partition" currently paused
  lastBatchAt?: number;
  lastError?: { at: string; message: string };
  readonly assigned = new Set<string>(); // "topic:partition"

  /** Reprocessed messages absorbed by ON CONFLICT DO NOTHING. */
  get duplicates() {
    return this.attempted - this.inserted;
  }

  countError(c: ErrorClass, n = 1) {
    this.errorsByClass[c] = (this.errorsByClass[c] ?? 0) + n;
  }

  countRetry(c: ErrorClass) {
    this.retriesByClass[c] = (this.retriesByClass[c] ?? 0) + 1;
  }

  countDlq(c: ErrorClass) {
    this.dlq++;
    this.dlqByClass[c] = (this.dlqByClass[c] ?? 0) + 1;
  }

  snapshot() {
    return {
      uptime_s: Math.round((Date.now() - this.startedAt) / 1000),
      assigned_partitions: [...this.assigned].sort(),
      consumed: this.consumed,
      attempted: this.attempted,
      inserted: this.inserted,
      duplicates: this.duplicates,
      batches: this.batches,
      chunks: this.chunks,
      batch_errors: this.batchErrors,
      commits: this.commits,
      commit_errors: this.commitErrors,
      last_commit_ms: this.lastCommitMs ?? null,
      stopped_early: this.stoppedEarly,
      rebalances: this.rebalances,
      dlq: this.dlq,
      dlq_by_class: { ...this.dlqByClass },
      dlq_errors: this.dlqErrors,
      errors_by_class: { ...this.errorsByClass },
      retries_by_class: { ...this.retriesByClass },
      isolations: this.isolations,
      pauses: this.pauses,
      paused_partitions: [...this.paused].sort(),
      last_batch_at: this.lastBatchAt ? new Date(this.lastBatchAt).toISOString() : null,
      last_error: this.lastError ?? null,
    };
  }
}
