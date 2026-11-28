import { Counter, Gauge, Histogram, Registry, collectDefaultMetrics } from '@prometheus-io/client';
import type { PublishedRow, RelayObserver } from '@eventpulse/outbox';

export class RelayMetrics implements RelayObserver {
  readonly registry = new Registry();
  readonly unpublished: Gauge;
  readonly oldestAge: Gauge;
  private readonly publishedTotal: Counter;
  private readonly republishedTotal: Counter;
  private readonly latency: Histogram;
  private readonly isLeader: Gauge;
  private readonly cycleFailures: Counter;
  private readonly retentionDeleted: Counter;

  constructor() {
    collectDefaultMetrics({ register: this.registry });
    const registers = [this.registry];
    this.unpublished = new Gauge({ name: 'ep_outbox_unpublished', help: 'Outbox rows not published yet (count on the partial index, polled every OUTBOX_GAUGE_POLL_MS).', registers });
    this.oldestAge = new Gauge({ name: 'ep_outbox_oldest_unpublished_age_seconds', help: 'Age of the oldest unpublished outbox row (0 when none). Rising = the relay is stuck or Kafka is down.', registers });
    this.publishedTotal = new Counter({ name: 'ep_outbox_published_total', help: 'Outbox rows published (acked by Kafka and marked), by topic.', labelNames: ['topic'], registers });
    this.republishedTotal = new Counter({
      name: 'ep_outbox_republished_total',
      help: 'Rows published with attempts > 1 (an earlier attempt crashed, failed or timed out): an upper bound on duplicates sent.',
      labelNames: ['topic'],
      registers,
    });
    this.latency = new Histogram({
      name: 'ep_outbox_publish_latency_seconds',
      help: 'Outbox row created_at (commit side) -> published_at.',
      buckets: [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300],
      registers,
    });
    this.isLeader = new Gauge({ name: 'ep_outbox_relay_is_leader', help: '1 if this relay is the active one (holds the advisory lock, or RELAY_MODE=multi).', registers });
    this.cycleFailures = new Counter({ name: 'ep_outbox_relay_cycle_failures_total', help: 'Relay cycles that failed (Kafka or Postgres error) and were rolled back.', registers });
    this.retentionDeleted = new Counter({ name: 'ep_outbox_retention_deleted_total', help: 'Published rows deleted by retention.', registers });
  }

  published(rows: PublishedRow[], publishedAt: Date) {
    for (const r of rows) {
      this.publishedTotal.labels(r.topic).inc();
      if (r.attempts > 1) this.republishedTotal.labels(r.topic).inc();
      this.latency.observe(Math.max(0, (publishedAt.getTime() - new Date(r.createdAt).getTime()) / 1000));
    }
  }
  cycleFailed() {
    this.cycleFailures.inc();
  }
  leadership(isLeader: boolean) {
    this.isLeader.set(isLeader ? 1 : 0);
  }
  retention(deleted: number) {
    this.retentionDeleted.inc(deleted);
  }
}
