// The config change and its config.changed event in ONE transaction (S3-F4):
//
//   BEGIN
//     SELECT ... FROM tenants LEFT JOIN tenant_config ... FOR UPDATE OF tenants   (404 unknown tenant; serializes
//                                                                                  changes of one tenant, also the
//                                                                                  first one, when no config row exists)
//     If-Match check against the current version                                  (412)
//     INSERT ... ON CONFLICT (tenant_id) DO UPDATE SET version = version + 1 ... RETURNING version, updated_at
//     enqueue(tx, config.changed: full snapshot, event_id, version)               (packages/outbox)
//     admin:after-update-before-commit
//   COMMIT
//
// Either both the change and the outbox row commit, or neither does: nothing is published for a rolled-back
// change, and a committed change is published (at least once) by services/outbox-relay.
//
// No-op PUT (same config as stored): still a new version and a new event. Simpler than comparing documents, and
// harmless: consumers apply the same snapshot again. The version then counts accepted PUTs, not distinct configs.
import { randomUUID } from 'node:crypto';
import { Inject, Injectable, NotFoundException, PreconditionFailedException, ServiceUnavailableException } from '@nestjs/common';
import type { Pool } from 'pg';
import { FaultInjector } from '@eventpulse/fault-injector';
import { CONTENT_TYPE_CONFIG_AVRO, HEADER_EVENT_ID, HEADER_VERSION, enqueue } from '@eventpulse/outbox';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import { PG_POOL } from '../db.module.js';
import { AdminMetrics } from '../observability/metrics.js';
import { ConfigSerdeService } from './config-serde.service.js';
import { ifMatchOk } from './if-match.js';
import type { IfMatch } from './if-match.js';

export const FAULT_INJECTOR = Symbol('FAULT_INJECTOR');

export interface TenantConfigView {
  tenant_id: string;
  config: Record<string, unknown>;
  version: number;
  updated_at: string | null;
}

export interface PutOutcome extends TenantConfigView {
  event_id: string;
  outbox_id: string;
}

export const LOCK_TENANT_SQL = `
  SELECT c.version FROM tenants t LEFT JOIN tenant_config c USING (tenant_id) WHERE t.tenant_id = $1 FOR UPDATE OF t`;
export const UPSERT_SQL = `
  INSERT INTO tenant_config (tenant_id, config, version) VALUES ($1, $2, 1)
  ON CONFLICT (tenant_id) DO UPDATE SET config = EXCLUDED.config, version = tenant_config.version + 1, updated_at = now()
  RETURNING version, updated_at`;

@Injectable()
export class TenantConfigService {
  constructor(
    @Inject(PG_POOL) private readonly pool: Pool,
    @Inject(CONFIG) private readonly cfg: AppConfig,
    @Inject(FAULT_INJECTOR) private readonly faults: FaultInjector,
    private readonly serde: ConfigSerdeService,
    private readonly metrics: AdminMetrics,
  ) {}

  async get(tenantId: string): Promise<TenantConfigView> {
    const r = await this.pool.query(
      'SELECT t.tenant_id, c.config, c.version, c.updated_at FROM tenants t LEFT JOIN tenant_config c USING (tenant_id) WHERE t.tenant_id = $1',
      [tenantId],
    );
    if (r.rowCount === 0) throw new NotFoundException(`unknown tenant ${tenantId}`);
    const row = r.rows[0];
    return { tenant_id: tenantId, config: row.config ?? {}, version: Number(row.version ?? 0), updated_at: row.updated_at?.toISOString() ?? null };
  }

  async put(tenantId: string, config: Record<string, unknown>, ifMatch: IfMatch): Promise<PutOutcome> {
    if (!this.serde.serde.ready) throw new ServiceUnavailableException('config.changed schema id not known yet (schema registry unreachable at startup?)');
    const c = await this.pool.connect();
    let broken = false;
    try {
      await c.query('BEGIN');
      const cur = await c.query(LOCK_TENANT_SQL, [tenantId]);
      if (cur.rowCount === 0) throw new NotFoundException(`unknown tenant ${tenantId}`);
      const current = Number(cur.rows[0].version ?? 0);
      if (!ifMatchOk(ifMatch, current)) {
        throw new PreconditionFailedException({ message: `If-Match does not match the current version ${current}`, current_version: current });
      }
      const up = await c.query(UPSERT_SQL, [tenantId, JSON.stringify(config)]);
      const version = Number(up.rows[0].version);
      const updatedAt: Date = up.rows[0].updated_at;
      const eventId = randomUUID();
      const payload = this.serde.serde.encode({ eventId, tenantId, version, changedAt: updatedAt, config });
      const outboxId = await enqueue(
        c,
        {
          aggregateId: tenantId,
          topic: this.cfg.configTopic,
          key: tenantId,
          payload,
          headers: { [HEADER_EVENT_ID]: eventId, [HEADER_VERSION]: String(version), 'content-type': CONTENT_TYPE_CONFIG_AVRO },
        },
        { notify: this.cfg.outboxNotify },
      );
      this.faults.hit('admin:after-update-before-commit');
      this.faults.maybeFail('admin:after-update-before-commit');
      await c.query('COMMIT');
      this.metrics.enqueued.labels(this.cfg.configTopic).inc();
      return { tenant_id: tenantId, config, version, updated_at: updatedAt.toISOString(), event_id: eventId, outbox_id: outboxId };
    } catch (e) {
      try {
        await c.query('ROLLBACK');
      } catch {
        broken = true;
      }
      throw e;
    } finally {
      c.release(broken);
    }
  }
}
