// Writes rows to Postgres: one multi-row INSERT ... SELECT FROM unnest(...) per chunk, one transaction per chunk.
//
// ON CONFLICT (event_id) DO NOTHING absorbs duplicates (reprocessing after a crash, a rebalance, or a client
// retry). They are NOT hidden: every write returns attempted vs inserted, and attempted - inserted is counted
// as duplicates.

import type { Pool } from 'pg';
import type { EventRow } from './parse.js';

export const INSERT_SQL = `
  INSERT INTO events (event_id, tenant_id, type, occurred_at, schema_version, payload, received_at)
  SELECT * FROM unnest(
    $1::uuid[], $2::text[], $3::text[], $4::timestamptz[], $5::int[], $6::jsonb[], $7::timestamptz[]
  )
  ON CONFLICT (event_id) DO NOTHING
  RETURNING received_at`;

export interface WriteResult {
  attempted: number;
  inserted: number;
  durationMs: number;
  /** Time to get a pool connection. */
  connectMs?: number;
  /** Per newly inserted row: DB commit time - received_at, seconds (duplicates excluded). */
  freshnessS?: number[];
}

export function chunk<T>(items: T[], size: number): T[][] {
  if (size < 1) throw new Error('chunk size must be >= 1');
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

export function toColumns(rows: EventRow[]) {
  return [
    rows.map((r) => r.event_id),
    rows.map((r) => r.tenant_id),
    rows.map((r) => r.type),
    rows.map((r) => r.occurred_at),
    rows.map((r) => r.schema_version),
    rows.map((r) => r.payload),
    rows.map((r) => r.received_at),
  ];
}

export class BatchWriter {
  constructor(private readonly pool: Pool) {}

  /** One transaction: BEGIN, one INSERT for all rows, COMMIT. Resolves only after COMMIT returned. */
  async writeChunk(rows: EventRow[]): Promise<WriteResult> {
    if (rows.length === 0) return { attempted: 0, inserted: 0, durationMs: 0, connectMs: 0, freshnessS: [] };
    const c0 = performance.now();
    const client = await this.pool.connect();
    const t0 = performance.now();
    try {
      await client.query('BEGIN');
      try {
        // RETURNING lists only the rows actually inserted (not the ones ON CONFLICT skipped), so freshness is
        // measured for real first writes, never for reprocessed duplicates.
        const r = await client.query<{ received_at: Date }>(INSERT_SQL, toColumns(rows));
        await client.query('COMMIT');
        const committedAt = Date.now();
        return {
          attempted: rows.length,
          inserted: r.rowCount ?? r.rows?.length ?? 0,
          durationMs: performance.now() - t0,
          connectMs: t0 - c0,
          freshnessS: (r.rows ?? []).map((x) => (committedAt - new Date(x.received_at).getTime()) / 1000),
        };
      } catch (e) {
        await client.query('ROLLBACK').catch(() => undefined);
        throw e;
      }
    } finally {
      client.release();
    }
  }
}
