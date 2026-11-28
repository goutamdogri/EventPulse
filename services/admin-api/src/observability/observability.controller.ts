import { Controller, Get, Header, HttpException, Inject } from '@nestjs/common';
import type { Pool } from 'pg';
import { FaultInjector } from '@eventpulse/fault-injector';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import { PG_POOL } from '../db.module.js';
import { ConfigSerdeService } from '../tenant-config/config-serde.service.js';
import { FAULT_INJECTOR } from '../tenant-config/tenant-config.service.js';
import { AdminMetrics } from './metrics.js';

@Controller()
export class ObservabilityController {
  constructor(
    private readonly metrics: AdminMetrics,
    private readonly serde: ConfigSerdeService,
    @Inject(PG_POOL) private readonly pool: Pool,
    @Inject(CONFIG) private readonly cfg: AppConfig,
    @Inject(FAULT_INJECTOR) private readonly faults: FaultInjector,
  ) {}

  @Get('metrics')
  @Header('Content-Type', 'text/plain; version=0.0.4')
  metrics_() {
    return this.metrics.registry.metrics();
  }

  @Get('healthz')
  healthz() {
    return { status: 'ok' };
  }

  /** Ready = Postgres answers and the config.changed schema id is known (a PUT can succeed). */
  @Get('readyz')
  async readyz() {
    const checks = { postgres: false, schema: this.serde.serde.ready };
    try {
      await this.pool.query('SELECT 1');
      checks.postgres = true;
    } catch {
      /* not ready */
    }
    if (!checks.postgres || !checks.schema) throw new HttpException({ status: 'not_ready', checks, schema_error: this.serde.lastError }, 503);
    return { status: 'ready', checks };
  }

  @Get('debug/stats')
  stats() {
    return {
      config_topic: this.cfg.configTopic,
      schema_id: this.serde.serde.serde.writerId ?? null,
      pg_pool: { max: this.cfg.pgPoolMax, total: this.pool.totalCount, idle: this.pool.idleCount, waiting: this.pool.waitingCount },
      faults: this.faults.describe(),
    };
  }
}
