import { Controller, Get, Global, Header, Inject, Injectable, Module } from '@nestjs/common';
import type { Pool } from 'pg';
import { Registry, collectDefaultMetrics } from '@prometheus-io/client';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import { PG_POOL } from '../db/db.module.js';
import { SinkStats } from './sink-stats.js';

/** The Prometheus registry of the process: Node process metrics + every sink series (SinkStats, S2-F6). */
@Injectable()
export class MetricsRegistry {
  readonly registry = new Registry();
  constructor() {
    collectDefaultMetrics({ register: this.registry });
  }
}

@Controller()
export class ObservabilityController {
  constructor(
    private readonly metrics: MetricsRegistry,
    private readonly stats: SinkStats,
    @Inject(PG_POOL) private readonly pool: Pool,
    @Inject(CONFIG) private readonly cfg: AppConfig,
  ) {}

  @Get('metrics')
  @Header('Content-Type', 'text/plain; version=0.0.4')
  metrics_() {
    return this.metrics.registry.metrics();
  }

  /** /debug/stats (S2-F6 shape). Cached values only: no Kafka call inside this handler. */
  @Get('debug/stats')
  async debugStats() {
    const mem = process.memoryUsage();
    return {
      ...(await this.stats.snapshot()),
      group: this.cfg.consumer.groupId,
      topic: this.cfg.consumer.topic,
      memory: { rss: mem.rss, heapUsed: mem.heapUsed, heapTotal: mem.heapTotal, external: mem.external },
      pg_pool: { max: this.cfg.pgPoolMax, total: this.pool.totalCount, idle: this.pool.idleCount, waiting: this.pool.waitingCount },
      config: {
        consumer: this.cfg.consumer,
        sink: this.cfg.sink,
        retry: this.cfg.retry,
        schema: this.cfg.schema,
        lag_poll_ms: this.cfg.lagPollMs,
        brokers: this.cfg.kafka.brokers,
      },
    };
  }
}

@Global()
@Module({
  providers: [
    MetricsRegistry,
    {
      provide: SinkStats,
      inject: [MetricsRegistry, PG_POOL],
      useFactory: (m: MetricsRegistry, pool: Pool) =>
        new SinkStats(m.registry, () => ({ pgTotal: pool.totalCount, pgIdle: pool.idleCount, pgWaiting: pool.waitingCount })),
    },
  ],
  controllers: [ObservabilityController],
  exports: [MetricsRegistry, SinkStats],
})
export class ObservabilityModule {}
