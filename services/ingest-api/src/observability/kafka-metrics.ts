// S2-F6: batcher + producer metrics of the ingest API. Every metric is a Prometheus series (/metrics) AND a field
// of /debug/stats (snapshot()). A plain class taking the registry, so it is unit-tested without Nest.
//
// Label cardinality stays low on purpose: reasons, error types and broker names only. Never event ids, tenant ids
// or raw error messages.

import { Counter, Gauge, Histogram } from '@prometheus-io/client';
import type { Registry } from '@prometheus-io/client';
import { errorType } from '../kafka/errors.js';
import type { BreakerState } from '../kafka/breaker.js';
import type { FlushResult } from '../kafka/micro-batcher.js';
import { histQuantile, round } from './hist-quantile.js';

const SECONDS_BUCKETS = [0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30];
const SIZE_BUCKETS = [1, 2, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000];
const BYTES_BUCKETS = [256, 1024, 4096, 16384, 65536, 262144, 524288, 900000];
const BREAKER_VALUE: Record<BreakerState, number> = { closed: 0, open: 1, 'half-open': 2 };

export interface BatcherGaugeSource {
  queueDepth: number;
  queuedBytes: number;
  inFlight: number;
}

export interface LiveSources {
  batcher: () => BatcherGaugeSource;
  breaker: () => BreakerState;
  connected: () => boolean;
  healthy: () => boolean;
}

export class KafkaMetrics {
  readonly flushTotal: Counter<string>;
  readonly batchSizeMessages: Histogram<string>;
  readonly batchSizeBytes: Histogram<string>;
  readonly queueWait: Histogram<string>;
  readonly rejected: Counter<string>;
  readonly sendSeconds: Histogram<string>;
  readonly sendErrors: Counter<string>;
  readonly acked: Counter<string>;
  readonly failed: Counter<string>;
  readonly http503: Counter<string>;
  readonly clientTxBytes: Counter<string>;

  // Plain mirrors for a cheap, synchronous-ish /debug/stats.
  private readonly flushes: Record<string, number> = {};
  private readonly rejects: Record<string, number> = {};
  private readonly errors: Record<string, number> = {};
  private readonly by503: Record<string, number> = {};
  private ackedN = 0;
  private failedN = 0;
  private batches = 0;
  private batchedMessages = 0;
  private client: { queueMessages: number; queueBytes: number; brokers: Record<string, { rttP50Ms: number; rttP99Ms: number; inFlight: number }> } = {
    queueMessages: 0,
    queueBytes: 0,
    brokers: {},
  };
  private lastTxBytes = 0; // librdkafka's tx_bytes is cumulative; the counter gets the delta
  private live?: LiveSources;

  constructor(registry: Registry) {
    const registers = [registry];
    // Arrow getters capture the class instance; inside collect() `this` is the gauge.
    const getLive = () => this.live;
    const getClient = () => this.client;
    const liveGauge = (name: string, help: string, read: (l: LiveSources) => number) =>
      new Gauge({
        name,
        help,
        registers,
        collect() {
          const live = getLive();
          if (live) this.set(read(live));
        },
      });

    liveGauge('ep_batcher_queue_depth', 'Messages waiting in the micro-batcher queue.', (l) => l.batcher().queueDepth);
    liveGauge('ep_batcher_queue_bytes', 'Bytes waiting in the micro-batcher queue.', (l) => l.batcher().queuedBytes);
    liveGauge('ep_batcher_in_flight_batches', 'Batches being sent to Kafka right now.', (l) => l.batcher().inFlight);
    liveGauge('ep_kafka_breaker_state', 'Circuit breaker: 0 closed, 1 open, 2 half-open.', (l) => BREAKER_VALUE[l.breaker()]);
    liveGauge('ep_kafka_producer_connected', 'Producer connected (1) or not (0).', (l) => (l.connected() ? 1 : 0));
    liveGauge('ep_kafka_health_ok', 'Last background Kafka health check passed and is recent (1) or not (0).', (l) => (l.healthy() ? 1 : 0));

    this.flushTotal = new Counter({ name: 'ep_batcher_flush_total', help: 'Batches flushed, by reason (size, bytes, time, drain).', labelNames: ['reason'], registers });
    this.batchSizeMessages = new Histogram({ name: 'ep_batcher_batch_size_messages', help: 'Messages per flushed batch.', buckets: SIZE_BUCKETS, registers });
    this.batchSizeBytes = new Histogram({ name: 'ep_batcher_batch_size_bytes', help: 'Bytes per flushed batch.', buckets: BYTES_BUCKETS, registers });
    this.queueWait = new Histogram({ name: 'ep_batcher_queue_wait_seconds', help: 'Per message: enqueue -> its batch is sent.', buckets: SECONDS_BUCKETS, registers });
    this.rejected = new Counter({ name: 'ep_batcher_rejected_total', help: 'Messages refused before queueing, by reason.', labelNames: ['reason'], registers });
    this.sendSeconds = new Histogram({ name: 'ep_kafka_send_seconds', help: 'producer.send duration per batch (ack latency).', buckets: SECONDS_BUCKETS, registers });
    this.sendErrors = new Counter({ name: 'ep_kafka_send_errors_total', help: 'Failed batches, by low-cardinality error type.', labelNames: ['type'], registers });
    this.acked = new Counter({ name: 'ep_kafka_messages_acked_total', help: 'Messages acknowledged by Kafka (used by the S2-F7 audit).', registers });
    this.failed = new Counter({ name: 'ep_kafka_messages_failed_total', help: 'Messages in batches that failed or timed out.', registers });
    this.http503 = new Counter({ name: 'ep_http_503_total', help: '503 responses, by reason.', labelNames: ['reason'], registers });

    this.clientTxBytes = new Counter({ name: 'ep_kafka_client_tx_bytes_total', help: 'Bytes the producer sent to the brokers on the wire, after compression (client statistics tx_bytes, updated every stats interval).', registers });

    // Known label values start at 0, so a clean run shows 0 on the dashboard instead of "No data".
    for (const reason of ['overloaded', 'breaker_open', 'too_large', 'closed', 'queue_timeout']) this.rejected.inc({ reason }, 0);
    for (const reason of ['overloaded', 'kafka_unavailable', 'shutting_down']) this.http503.inc({ reason }, 0);
    for (const reason of ['size', 'bytes', 'time', 'drain']) this.flushTotal.inc({ reason }, 0);

    const clientGauge = (name: string, help: string, read: () => number) =>
      new Gauge({ name, help, registers, collect() { this.set(read()); } });
    clientGauge('ep_kafka_client_queue_messages', "Messages in librdkafka's own producer queue (client statistics).", () => this.client.queueMessages);
    clientGauge('ep_kafka_client_queue_bytes', "Bytes in librdkafka's own producer queue (client statistics).", () => this.client.queueBytes);
    new Gauge({
      name: 'ep_kafka_broker_rtt_seconds',
      help: 'Produce request round-trip time per broker from client statistics (p50, p99 over the stats window).',
      labelNames: ['broker', 'quantile'],
      registers,
      collect() {
        this.reset();
        for (const [b, s] of Object.entries(getClient().brokers)) {
          this.set({ broker: b, quantile: '0.5' }, s.rttP50Ms / 1000);
          this.set({ broker: b, quantile: '0.99' }, s.rttP99Ms / 1000);
        }
      },
    });
    new Gauge({
      name: 'ep_kafka_broker_in_flight_requests',
      help: 'Requests sent to each broker and waiting for a response (client statistics).',
      labelNames: ['broker'],
      registers,
      collect() {
        this.reset();
        for (const [b, s] of Object.entries(getClient().brokers)) this.set({ broker: b }, s.inFlight);
      },
    });
  }

  /** Live values (queue gauges, breaker, connection, health) are read at scrape / snapshot time. */
  bind(live: LiveSources) {
    this.live = live;
  }

  onFlush(r: FlushResult) {
    this.flushTotal.inc({ reason: r.reason });
    this.flushes[r.reason] = (this.flushes[r.reason] ?? 0) + 1;
    this.batchSizeMessages.observe(r.messages);
    this.batchSizeBytes.observe(r.bytes);
    for (const w of r.waitsMs ?? [r.oldestWaitMs]) this.queueWait.observe(w / 1000);
    this.sendSeconds.observe(r.durationMs / 1000);
    this.batches++;
    this.batchedMessages += r.messages;
    if (r.error === undefined) {
      this.acked.inc(r.messages);
      this.ackedN += r.messages;
    } else {
      const type = errorType(r.error);
      this.sendErrors.inc({ type });
      this.errors[type] = (this.errors[type] ?? 0) + 1;
      this.failed.inc(r.messages);
      this.failedN += r.messages;
    }
  }

  onReject(reason: string) {
    this.rejected.inc({ reason });
    this.rejects[reason] = (this.rejects[reason] ?? 0) + 1;
  }

  count503(reason: string) {
    this.http503.inc({ reason });
    this.by503[reason] = (this.by503[reason] ?? 0) + 1;
  }

  /** librdkafka statistics JSON (stats_cb, every KAFKA_STATS_INTERVAL_MS). Never throws. */
  onClientStats(json: string) {
    try {
      const s = JSON.parse(json);
      const brokers: typeof this.client.brokers = {};
      for (const b of Object.values<any>(s.brokers ?? {})) {
        if (typeof b?.nodeid !== 'number' || b.nodeid < 0) continue; // skip bootstrap / internal entries
        brokers[String(b.nodeid)] = {
          rttP50Ms: (b.rtt?.p50 ?? 0) / 1000, // librdkafka reports microseconds
          rttP99Ms: (b.rtt?.p99 ?? 0) / 1000,
          inFlight: b.waitresp_cnt ?? 0,
        };
      }
      const tx = typeof s.tx_bytes === 'number' ? s.tx_bytes : 0;
      if (tx >= this.lastTxBytes) this.clientTxBytes.inc(tx - this.lastTxBytes); // a new client after a reconnect restarts at 0
      else this.clientTxBytes.inc(tx);
      this.lastTxBytes = tx;
      this.client = { queueMessages: s.msg_cnt ?? 0, queueBytes: s.msg_size ?? 0, brokers };
    } catch {
      /* malformed stats: keep the previous values */
    }
  }

  async snapshot() {
    const b = this.live?.batcher();
    return {
      batcher: {
        queue_depth: b?.queueDepth ?? 0,
        queue_bytes: b?.queuedBytes ?? 0,
        in_flight: b?.inFlight ?? 0,
        flushes_by_reason: { ...this.flushes },
        avg_batch_size: this.batches ? round(this.batchedMessages / this.batches, 1) : 0,
        batch_size_p50: round(await histQuantile(this.batchSizeMessages, 0.5), 1),
        batch_size_p99: round(await histQuantile(this.batchSizeMessages, 0.99), 1),
        queue_wait_p99_ms: msq(await histQuantile(this.queueWait, 0.99)),
        rejected: { ...this.rejects },
      },
      producer: {
        connected: this.live?.connected() ?? false,
        breaker: this.live?.breaker() ?? 'closed',
        acked: this.ackedN,
        failed: this.failedN,
        send_p50_ms: msq(await histQuantile(this.sendSeconds, 0.5)),
        send_p99_ms: msq(await histQuantile(this.sendSeconds, 0.99)),
        errors_by_type: { ...this.errors },
        client_queue_messages: this.client.queueMessages,
        client_queue_bytes: this.client.queueBytes,
        brokers: this.client.brokers,
      },
      http_503_by_reason: { ...this.by503 },
    };
  }
}

const msq = (s: number | null) => (s === null ? null : round(s * 1000, 2));
