import { Injectable } from '@nestjs/common';
import { Counter, Histogram, Registry, collectDefaultMetrics } from '@prometheus-io/client';

export const PUT_RESULTS = ['ok', 'invalid', 'not_found', 'precondition_failed', 'unavailable', 'error'] as const;
export type PutResult = (typeof PUT_RESULTS)[number];

@Injectable()
export class AdminMetrics {
  readonly registry = new Registry();
  readonly puts: Counter;
  readonly putSeconds: Histogram;
  readonly enqueued: Counter;

  constructor() {
    collectDefaultMetrics({ register: this.registry });
    const registers = [this.registry];
    this.puts = new Counter({ name: 'ep_admin_config_puts_total', help: 'PUT /v1/admin/tenants/:id/config by result.', labelNames: ['result'], registers });
    for (const r of PUT_RESULTS) this.puts.labels(r).inc(0);
    this.putSeconds = new Histogram({
      name: 'ep_admin_config_put_seconds',
      help: 'Latency of a config PUT (one transaction: lock, upsert, outbox insert, commit).',
      buckets: [0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1],
      registers,
    });
    this.enqueued = new Counter({ name: 'ep_outbox_enqueued_total', help: 'Outbox rows committed by admin-api (one per config change).', labelNames: ['topic'], registers });
  }
}
