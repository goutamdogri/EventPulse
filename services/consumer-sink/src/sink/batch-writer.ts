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
  ON CONFLICT (event_id) DO NOTHING`;

export interface WriteResult {
  attempted: number;
  inserted: number;
  durationMs: number;
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
    const t0 = performance.now();
    if (rows.length === 0) return { attempted: 0, inserted: 0, durationMs: 0 };
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      try {
        const r = await client.query(INSERT_SQL, toColumns(rows));
        await client.query('COMMIT');
        return { attempted: rows.length, inserted: r.rowCount ?? 0, durationMs: performance.now() - t0 };
      } catch (e) {
        await client.query('ROLLBACK').catch(() => undefined);
        throw e;
      }
    } finally {
      client.release();
    }
  }
}
