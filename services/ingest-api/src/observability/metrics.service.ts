import { Inject, Injectable } from '@nestjs/common';
import { monitorEventLoopDelay } from 'node:perf_hooks';
import type { NextFunction, Request, Response } from 'express';
import {
  Counter,
  Gauge,
  Histogram,
  Registry,
  collectDefaultMetrics,
} from '@prometheus-io/client';
import { BatcherService } from '../kafka/batcher.service.js';
import { KafkaHealthService } from '../kafka/kafka-health.service.js';
import { ProducerService } from '../kafka/producer.service.js';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import { KafkaMetrics } from './kafka-metrics.js';
import { IDEM_BREAKER, REDIS } from '../idempotency/idempotency.module.js';
import { IdempotencyMetrics } from '../idempotency/idempotency-metrics.js';
import type { CircuitBreaker } from '../kafka/breaker.js';
import type { Redis } from 'ioredis';
import { SchemaService } from '../schema/schema.service.js';

@Injectable()
export class MetricsService {
  private readonly startedAt = Date.now();
  private readonly loop = monitorEventLoopDelay({ resolution: 10 });

  readonly httpRequests: Counter<string>;
  readonly httpDuration: Histogram<string>;
  readonly inFlight: Gauge<string>;
  readonly connOpen: Gauge<string>;
  readonly connTotal: Counter<string>;

  constructor(
    readonly registry: Registry,
    private readonly kafka: KafkaMetrics,
    private readonly batcher: BatcherService,
    private readonly producer: ProducerService,
    private readonly health: KafkaHealthService,
    @Inject(CONFIG) private readonly cfg: AppConfig,
    private readonly idem: IdempotencyMetrics,
    @Inject(REDIS) private readonly redis: Redis,
    @Inject(IDEM_BREAKER) private readonly idemBreaker: CircuitBreaker,
    private readonly schema: SchemaService,
  ) {
    const registers = [this.registry];
    // Live values (queue gauges, breaker, connection, health) are read at scrape / snapshot time.
    kafka.bind({
      batcher: () => batcher.stats(),
      breaker: () => batcher.breaker.state(),
      connected: () => producer.isConnected(),
      healthy: () => health.healthy(),
    });
    collectDefaultMetrics({ register: this.registry });
    this.loop.enable();

    this.httpRequests = new Counter({
      name: 'http_requests_total',
      help: 'Total number of HTTP requests.',
      labelNames: ['method', 'route', 'status'],
      registers,
    });

    this.httpDuration = new Histogram({
      name: 'http_request_duration_seconds',
      help: 'HTTP request duration.',
      labelNames: ['method', 'route', 'status'],
      buckets: [
        0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10,
      ],
      registers,
    });

    this.inFlight = new Gauge({
      name: 'http_requests_in_flight',
      help: "Requests currently being handled (Little's Law: rate x latency).",
      registers,
    });

    this.connOpen = new Gauge({
      name: 'http_connections_open',
      help: 'Open TCP connections',
      registers,
    });

    this.connTotal = new Counter({
      name: 'http_connections_total',
      help: 'Accepted connections',
      registers,
    });

    const loop = this.loop;
    new Gauge({
      name: 'eventpulse_event_loop_delay_seconds',
      help: 'Event loop delay (10 ms resolution histogram) since last reset.',
      labelNames: ['quantile'],
      registers,
      collect() {
        this.set({ quantile: '0.5' }, loop.percentile(50) / 1e9);
        this.set({ quantile: '0.99' }, loop.percentile(99) / 1e9);
        this.set({ quantile: 'max' }, loop.max / 1e9);
        loop.reset();
      },
    });
  }

  /** 503s by reason (overloaded | kafka_unavailable | shutting_down): ep_http_503_total + /debug/stats. */
  count503(reason: string) {
    this.kafka.count503(reason);
  }

  middleware() {
    return (req: Request, res: Response, next: NextFunction) => {
      const start = process.hrtime.bigint();
      this.inFlight.inc();
      res.once('close', () => {
        this.inFlight.dec();
        const labels = {
          method: req.method,
          route: req.route?.path ?? 'unmatched',
          status: String(res.statusCode),
        };
        this.httpRequests.inc(labels);
        this.httpDuration.observe(
          labels,
          Number(process.hrtime.bigint() - start) / 1e9,
        );
      });
      next();
    };
  }

  trackConnections(server: import('node:http').Server) {
    server.on('connection', (socket) => {
      this.connTotal.inc();
      this.connOpen.inc();
      socket.once('close', () => this.connOpen.dec());
    });
  }

  /** /debug/stats (S2-F6 shape). Includes the effective config, so every result records its settings. */
  async snapshot() {
    const byStatus: Record<string, number> = {};
    let requests = 0;
    for (const v of (await this.httpRequests.get()).values) {
      const k = String(v.labels.status);
      byStatus[k] = (byStatus[k] ?? 0) + v.value;
      requests += v.value;
    }
    const inFlight = (await this.inFlight.get()).values[0]?.value ?? 0;
    const k = await this.kafka.snapshot();
    const mem = process.memoryUsage();
    return {
      uptime_s: Math.round((Date.now() - this.startedAt) / 1000),
      memory: { rss: mem.rss, heapUsed: mem.heapUsed, heapTotal: mem.heapTotal, external: mem.external },
      // This histogram is reset on every Prometheus scrape, so these are "since last scrape".
      event_loop_delay_ms: {
        p50: this.loop.percentile(50) / 1e6,
        p99: this.loop.percentile(99) / 1e6,
        max: this.loop.max / 1e6,
      },
      http: { requests, in_flight: inFlight, by_status: byStatus, '503_by_reason': k.http_503_by_reason },
      batcher: k.batcher,
      producer: k.producer,
      kafka_health: this.health.snapshot(),
      idempotency: {
        ...this.idem.snapshot(),
        redis_status: this.redis.status,
        breaker: this.idemBreaker.enabled ? this.idemBreaker.state() : 'disabled',
      },
      schema: this.schema.snapshot(),
      config: {
        topic: this.cfg.topic,
        acks: this.cfg.producer.acks,
        compression: this.cfg.producer.compression,
        idempotent: this.cfg.producer.idempotent,
        linger_ms: this.cfg.producer.lingerMs,
        delivery_timeout_ms: this.cfg.producer.deliveryTimeoutMs,
        batch_max_messages: this.cfg.batcher.maxMessages,
        batch_max_wait_ms: this.cfg.batcher.maxWaitMs,
        batch_max_bytes: this.cfg.batcher.maxBytes,
        batch_max_queue: this.cfg.batcher.maxQueue,
        batch_max_queue_bytes: this.cfg.batcher.maxQueueBytes,
        batch_max_in_flight: this.cfg.batcher.maxInFlight,
        send_timeout_ms: this.cfg.batcher.sendTimeoutMs,
        breaker_failures: this.cfg.breaker.failures,
        breaker_cooldown_ms: this.cfg.breaker.cooldownMs,
        key_mode: this.cfg.keyMode,
        idempotency: {
          enabled: this.cfg.idempotency.enabled,
          redis: new URL(this.cfg.idempotency.redisUrl).host,
          ttl_s: this.cfg.idempotency.ttlS,
          inflight_ttl_s: this.cfg.idempotency.inflightTtlS,
          failure_policy: this.cfg.idempotency.policy,
          command_timeout_ms: this.cfg.idempotency.commandTimeoutMs,
        },
        brokers: this.cfg.kafka.brokers,
      },
    };
  }
}
