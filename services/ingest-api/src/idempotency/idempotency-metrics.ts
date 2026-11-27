// Prometheus series + /debug/stats fields for API idempotency (S3-F1; names are the Stage 3 step 3 catalogue).
import { Counter, Histogram } from '@prometheus-io/client';
import type { Registry } from '@prometheus-io/client';
import type { IdempotencyObserver, IdempotencyResult, StoreOp } from '@eventpulse/idempotency';

export class IdempotencyMetrics implements IdempotencyObserver {
  private readonly requests: Counter<string>;
  private readonly completeFailures: Counter<string>;
  private readonly redisLatency: Histogram<string>;
  private readonly redisErrors: Counter<string>;
  readonly byResult: Partial<Record<IdempotencyResult, number>> = {};
  readonly completeFailuresByReason: Record<string, number> = {};
  readonly redisErrorsByOp: Partial<Record<StoreOp, number>> = {};
  lastRedisError?: { at: string; op: StoreOp; message: string };

  constructor(registry: Registry) {
    const registers = [registry];
    this.requests = new Counter({
      name: 'ep_idem_requests_total',
      help: 'Ingest requests by idempotency outcome: new | replayed | in_progress | payload_mismatch | fail_open | fail_closed | disabled.',
      labelNames: ['result'],
      registers,
    });
    this.completeFailures = new Counter({
      name: 'ep_idem_complete_failures_total',
      help: 'Produced events whose key could not be marked done (error | lost_ownership); retries get 409 until the inflight TTL.',
      labelNames: ['reason'],
      registers,
    });
    this.redisLatency = new Histogram({
      name: 'ep_idem_redis_latency_seconds',
      help: 'Latency of one idempotency store operation (one Lua script).',
      labelNames: ['op', 'ok'],
      buckets: [0.0002, 0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25],
      registers,
    });
    this.redisErrors = new Counter({
      name: 'ep_idem_redis_errors_total',
      help: 'Failed idempotency store operations (Redis down, timeout, corrupt value).',
      labelNames: ['op'],
      registers,
    });
  }

  result(r: IdempotencyResult) {
    this.byResult[r] = (this.byResult[r] ?? 0) + 1;
    this.requests.inc({ result: r });
  }

  latency(op: StoreOp, seconds: number, ok: boolean) {
    this.redisLatency.observe({ op, ok: String(ok) }, seconds);
  }

  completeFailed(reason: 'error' | 'lost_ownership') {
    this.completeFailuresByReason[reason] = (this.completeFailuresByReason[reason] ?? 0) + 1;
    this.completeFailures.inc({ reason });
  }

  storeError(op: StoreOp, err: unknown) {
    this.redisErrorsByOp[op] = (this.redisErrorsByOp[op] ?? 0) + 1;
    this.redisErrors.inc({ op });
    this.lastRedisError = { at: new Date().toISOString(), op, message: String((err as Error)?.message ?? err).slice(0, 300) };
  }

  snapshot() {
    return {
      by_result: { ...this.byResult },
      complete_failures: { ...this.completeFailuresByReason },
      redis_errors: { ...this.redisErrorsByOp },
      last_redis_error: this.lastRedisError ?? null,
    };
  }
}
