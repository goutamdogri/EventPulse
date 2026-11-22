// Plain in-process counters for the sink, shown on /debug/stats.
// S2-F6 turns these into Prometheus series and adds lag, latency histograms, freshness, etc.

export class SinkStats {
  readonly startedAt = Date.now();
  consumed = 0; // every message handed to us, reprocessed ones included
  attempted = 0; // rows sent to INSERT
  inserted = 0; // rows actually inserted (rowCount)
  batches = 0;
  chunks = 0;
  batchErrors = 0;
  rebalances = 0;
  lastBatchAt?: number;
  lastError?: { at: string; message: string };
  readonly assigned = new Set<string>(); // "topic:partition"

  /** Reprocessed messages absorbed by ON CONFLICT DO NOTHING. */
  get duplicates() {
    return this.attempted - this.inserted;
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
      rebalances: this.rebalances,
      last_batch_at: this.lastBatchAt ? new Date(this.lastBatchAt).toISOString() : null,
      last_error: this.lastError ?? null,
    };
  }
}
