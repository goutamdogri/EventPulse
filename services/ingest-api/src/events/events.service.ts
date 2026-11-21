import { Inject, Injectable } from '@nestjs/common';
import { Pool } from 'pg';
import { PG_POOL } from '../db/db.module.js';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import { MetricsService } from '../observability/metrics.service.js';
import { CreateEventDto } from './create-event.dto.js';

const INSERT_SQL = `
  INSERT INTO events (event_id, tenant_id, type, occurred_at, schema_version, payload)
  VALUES ($1, $2, $3, $4, $5, $6)
  ON CONFLICT (event_id) DO NOTHING`;

// B4: same transaction as the INSERT. The row lock on the tenant's counter row is held until COMMIT.
const COUNTER_SQL = `UPDATE tenant_counters SET n = n + 1 WHERE tenant_id = $1`;

@Injectable()
export class EventsService {
  constructor(
    @Inject(PG_POOL)
    private readonly pool: Pool,

    private readonly m: MetricsService,

    @Inject(CONFIG)
    private readonly cfg: AppConfig,
  ) {}

  /** Returns true if a row was inserted, false if it was a duplicate. */
  async insert(tenantId: string, e: CreateEventDto): Promise<boolean> {
    const t0 = process.hrtime.bigint();
    const client = await this.pool.connect(); // time spent waiting for the pool
    const t1 = process.hrtime.bigint();
    this.m.poolWait.observe(Number(t1 - t0) / 1e9);
    try {
      const params = [
        e.event_id,
        tenantId,
        e.type,
        e.occurred_at,
        e.schema_version,
        e.payload,
      ];
      let r;
      if (this.cfg.counterMode === 'hot') {
        await client.query('BEGIN');
        try {
          r = await client.query(INSERT_SQL, params);
          const tc = process.hrtime.bigint();
          await client.query(COUNTER_SQL, [tenantId]);
          this.m.counterUpdate.observe(Number(process.hrtime.bigint() - tc) / 1e9);
          await client.query('COMMIT');
        } catch (err) {
          await client.query('ROLLBACK').catch(() => undefined);
          throw err;
        }
      } else {
        r = await client.query(INSERT_SQL, params);
      }
      this.m.pgQuery.observe(Number(process.hrtime.bigint() - t1) / 1e9);
      const inserted = r.rowCount === 1;
      (inserted ? this.m.eventsInserted : this.m.eventsDuplicate).inc();
      return inserted;
    } finally {
      client.release();
    }
  }
}
