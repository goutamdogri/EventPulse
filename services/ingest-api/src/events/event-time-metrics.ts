// S3-F8 metrics at the API: how late events arrive, and how many the skew bounds reject.
import { Counter, Histogram } from '@prometheus-io/client';
import type { Registry } from '@prometheus-io/client';
import { SKEW_REASONS } from './event-time.js';
import type { SkewReason } from './event-time.js';

/** 1 s .. 30 days. */
export const LATENESS_BUCKETS = [1, 5, 15, 60, 300, 900, 3600, 6 * 3600, 86_400, 3 * 86_400, 7 * 86_400, 30 * 86_400];

export class EventTimeMetrics {
  private readonly lateness: Histogram<string>;
  private readonly rejected: Counter<string>;
  private readonly ahead: Counter<string>;
  readonly rejectedByReason: Record<string, number> = {};

  constructor(registry: Registry) {
    const registers = [registry];
    this.lateness = new Histogram({
      name: 'ep_event_lateness_seconds',
      help: 'received_at - occurred_at of accepted events (client clock to API). Events ahead of the server count as 0 (see ep_event_ahead_total).',
      buckets: LATENESS_BUCKETS,
      registers,
    });
    this.rejected = new Counter({
      name: 'ep_event_rejected_total',
      help: 'Events rejected with 422 by the clock-skew bounds: future (OCCURRED_AT_MAX_FUTURE_S) | too_old (OCCURRED_AT_MAX_PAST_DAYS).',
      labelNames: ['reason'],
      registers,
    });
    for (const r of ['future', 'too_old']) this.rejected.labels(r).inc(0);
    this.ahead = new Counter({ name: 'ep_event_ahead_total', help: 'Accepted events whose occurred_at is after received_at (within the future bound): client clock ahead.', registers });
  }

  observe(latenessS: number) {
    if (latenessS < 0) this.ahead.inc();
    this.lateness.observe(Math.max(0, latenessS));
  }

  reject(reason: SkewReason) {
    const label = reason === 'occurred_at_in_future' ? 'future' : 'too_old';
    this.rejected.labels(label).inc();
    this.rejectedByReason[reason] = (this.rejectedByReason[reason] ?? 0) + 1;
  }
}

export { SKEW_REASONS };
