// Postgres side of tools/dlq-replay: dlq_meta (read, mark, reindex) and dlq_replays (bookkeeping).
import type { Pool } from 'pg';
import { META_COLUMNS, selectSql, whereClause } from './filters.ts';
import type { Filters } from './filters.ts';
import type { MetaRow as NewMetaRow } from './reindex.ts';

export interface MetaRow {
  id: string;
  source_topic: string;
  source_partition: number;
  source_offset: string;
  failed_topic: string;
  dlq_topic: string;
  dlq_partition: number;
  dlq_offset: string;
  tenant_id: string | null;
  event_id: string | null;
  error_class: string;
  error_code: string | null;
  error_message: string | null;
  attempts: number;
  first_failed_at: Date;
  last_failed_at: Date;
  replayed_at: Date | null;
  replay_count: number;
  last_replay_id: string | null;
}

export interface ReplayCounts {
  selected: number;
  replayed: number;
  skipped: number;
  failed: number;
}

export type ReplayStatus = 'running' | 'done' | 'dry_run' | 'aborted' | 'failed';

export class Store {
  readonly pool: Pick<Pool, 'query'>;
  constructor(pool: Pick<Pool, 'query'>) {
    this.pool = pool;
  }

  async select(f: Filters): Promise<MetaRow[]> {
    const q = selectSql(f);
    return (await this.pool.query(q.text, q.values)).rows;
  }

  async get(id: number): Promise<MetaRow | undefined> {
    return (await this.pool.query(`SELECT ${META_COLUMNS} FROM dlq_meta WHERE id = $1`, [id])).rows[0];
  }

  /** Triage counts over the filtered rows. */
  async stats(f: Filters) {
    const w = whereClause(f);
    const by = async (expr: string) =>
      (await this.pool.query(`SELECT ${expr} AS k, count(*)::int AS n FROM dlq_meta ${w.text} GROUP BY 1 ORDER BY 2 DESC, 1`, w.values)).rows as { k: string | null; n: number }[];
    const age = `CASE WHEN now() - first_failed_at < interval '1 hour' THEN '1: < 1h'
                      WHEN now() - first_failed_at < interval '1 day' THEN '2: 1h-1d'
                      WHEN now() - first_failed_at < interval '7 days' THEN '3: 1d-7d' ELSE '4: > 7d' END`;
    const total = (await this.pool.query(`SELECT count(*)::int AS n, min(first_failed_at) AS oldest FROM dlq_meta ${w.text}`, w.values)).rows[0];
    return {
      total: total.n as number,
      oldest: (total.oldest as Date | null)?.toISOString() ?? null,
      byClass: await by('error_class'),
      byCode: await by(`error_class || ' ' || coalesce(error_code, '-')`),
      byTenant: await by(`coalesce(tenant_id, '(unparsed)')`),
      byFailedTopic: await by('failed_topic'),
      byAge: (await by(age)).sort((a, b) => String(a.k).localeCompare(String(b.k))).map((r) => ({ k: String(r.k).slice(3), n: r.n })),
    };
  }

  /** Called only after Kafka acknowledged the re-injected record. */
  async markReplayed(id: string, replayId: string): Promise<void> {
    await this.pool.query('UPDATE dlq_meta SET replayed_at = now(), replay_count = replay_count + 1, last_replay_id = $2 WHERE id = $1', [id, replayId]);
  }

  async startReplay(replayId: string, operator: string, filters: Record<string, unknown>, dryRun: boolean, status: ReplayStatus, c: ReplayCounts): Promise<void> {
    await this.pool.query(
      `INSERT INTO dlq_replays (replay_id, operator, filters, dry_run, selected, replayed, skipped, failed, status, finished_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, CASE WHEN $9 = 'running' THEN NULL ELSE now() END)`,
      [replayId, operator, JSON.stringify(filters), dryRun, c.selected, c.replayed, c.skipped, c.failed, status],
    );
  }

  async finishReplay(replayId: string, status: ReplayStatus, c: ReplayCounts): Promise<void> {
    await this.pool.query('UPDATE dlq_replays SET finished_at = now(), status = $2, selected = $3, replayed = $4, skipped = $5, failed = $6 WHERE replay_id = $1', [
      replayId,
      status,
      c.selected,
      c.replayed,
      c.skipped,
      c.failed,
    ]);
  }

  /** Reindex: insert one rebuilt row; false if the source record already has a row. */
  async insertMeta(r: NewMetaRow): Promise<boolean> {
    const res = await this.pool.query(
      `INSERT INTO dlq_meta (source_topic, source_partition, source_offset, failed_topic, dlq_topic, dlq_partition, dlq_offset,
                             tenant_id, event_id, error_class, error_code, error_message, attempts, first_failed_at, last_failed_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
       ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING`,
      [r.sourceTopic, r.sourcePartition, r.sourceOffset, r.failedTopic, r.dlqTopic, r.dlqPartition, r.dlqOffset, r.tenantId, r.eventId, r.errorClass, r.errorCode, r.errorMessage, r.attempts, r.firstFailedAt, r.lastFailedAt],
    );
    return (res.rowCount ?? 0) > 0;
  }
}
