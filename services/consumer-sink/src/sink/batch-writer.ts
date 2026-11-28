// Writes rows to Postgres: one multi-row INSERT ... SELECT FROM unnest(...) per chunk, one transaction per chunk.
//
// S3-F2: the sink is idempotent on (tenant_id, event_id).
//   - ON CONFLICT (tenant_id, event_id) DO NOTHING absorbs every duplicate (reprocessing after a crash or
//     rebalance, an offset reset, a client retry, a replay). DO NOTHING, never DO UPDATE: the first committed copy
//     wins and a duplicate changes nothing (DO UPDATE would also fail on a key repeated inside one statement).
//   - Duplicates are NOT hidden: RETURNING lists the inserted keys, so the writer reports attempted vs inserted
//     and WHICH keys were duplicates.
//   - When a chunk has duplicates, their stored content_hash is compared with the incoming one. A mismatch is a
//     "conflicting duplicate" (same key, different content: a producer bug); it is counted and reported, and the
//     stored row is never overwritten. This costs one extra query, only for chunks that have duplicates.
//   - sink_offsets (highest stored Kafka offset per group/topic/partition) is upserted in the SAME transaction.
//
// Nothing else in the transaction may have a side effect for a duplicate row.

import type { Pool, PoolClient } from 'pg';
import type { CrashPointName, FaultInjector } from '@eventpulse/fault-injector';
import type { EventRow } from './parse.js';

export function insertSql(table = 'events', dedup = true): string {
  if (!dedup) {
    // K5 / C14 "dedup off" arm (debug-only): plain INSERT into a copy of the table with no key.
    return `
  INSERT INTO ${table} (event_id, tenant_id, type, occurred_at, schema_version, payload, received_at)
  SELECT * FROM unnest(
    $1::uuid[], $2::text[], $3::text[], $4::timestamptz[], $5::int[], $6::jsonb[], $7::timestamptz[]
  )
  RETURNING tenant_id, event_id, received_at, occurred_at`;
  }
  return `
  INSERT INTO ${table} (event_id, tenant_id, type, occurred_at, schema_version, payload, received_at, content_hash)
  SELECT * FROM unnest(
    $1::uuid[], $2::text[], $3::text[], $4::timestamptz[], $5::int[], $6::jsonb[], $7::timestamptz[], $8::bytea[]
  )
  ON CONFLICT (tenant_id, event_id) DO NOTHING
  RETURNING tenant_id, event_id, received_at, occurred_at`;
}
export const INSERT_SQL = insertSql();

/** Highest stored offset per partition; GREATEST so a smaller (bisected / reprocessed) write never moves it back. */
export const SINK_OFFSETS_SQL = `
  INSERT INTO sink_offsets (consumer_group, topic, partition, max_offset)
  SELECT $1, $2, p, o FROM unnest($3::int[], $4::bigint[]) AS t(p, o)
  ON CONFLICT (consumer_group, topic, partition)
  DO UPDATE SET max_offset = GREATEST(sink_offsets.max_offset, EXCLUDED.max_offset), updated_at = now()`;

export const STORED_HASHES_SQL = `
  SELECT e.tenant_id, e.event_id::text AS event_id, e.content_hash
    FROM events e
    JOIN unnest($1::text[], $2::uuid[]) AS k(tenant_id, event_id)
      ON e.tenant_id = k.tenant_id AND e.event_id = k.event_id`;

export interface EventKey {
  tenant_id: string;
  event_id: string;
}

/** A row that was NOT inserted, with where it came from (S3-F3 splits duplicates by cause from its offset). */
export interface DuplicateRow extends EventKey {
  partition: number;
  offset: string;
}

export interface WriteResult {
  attempted: number;
  inserted: number;
  durationMs: number;
  /** Time to get a pool connection. */
  connectMs?: number;
  /** Per newly inserted row: DB commit time - received_at, seconds (duplicates excluded). */
  freshnessS?: number[];
  /** Per newly inserted row: received_at - occurred_at, seconds (S3-F8 lateness; negative = client clock ahead). */
  latenessS?: number[];
  /** The rows that were NOT inserted (attempted - inserted of them). */
  duplicateKeys?: DuplicateRow[];
  /** Duplicates whose stored content differs from the incoming one (never overwritten). */
  conflicts?: EventKey[];
  /** The conflict check itself failed (the rows ARE committed; only the comparison is missing). */
  conflictCheckFailed?: boolean;
}

export function chunk<T>(items: T[], size: number): T[][] {
  if (size < 1) throw new Error('chunk size must be >= 1');
  const out: T[][] = [];
  for (let i = 0; i < items.length; i += size) out.push(items.slice(i, i + size));
  return out;
}

export function toColumns(rows: EventRow[], withHash = true) {
  const cols: unknown[][] = [
    rows.map((r) => r.event_id),
    rows.map((r) => r.tenant_id),
    rows.map((r) => r.type),
    rows.map((r) => r.occurred_at),
    rows.map((r) => r.schema_version),
    rows.map((r) => r.payload),
    rows.map((r) => r.received_at),
  ];
  if (withHash) cols.push(rows.map((r) => r.content_hash ?? null));
  return cols;
}

const keyOf = (tenant: string, eventId: string) => `${tenant}\u0000${eventId.toLowerCase()}`;

/**
 * Keys sent minus keys returned. A key repeated inside the chunk counts once per extra copy, so
 * duplicateKeys.length === attempted - inserted.
 */
export function duplicateKeysOf(rows: EventRow[], inserted: EventKey[]): DuplicateRow[] {
  const left = new Map<string, number>();
  for (const k of inserted) left.set(keyOf(k.tenant_id, k.event_id), (left.get(keyOf(k.tenant_id, k.event_id)) ?? 0) + 1);
  const dups: DuplicateRow[] = [];
  for (const r of rows) {
    const k = keyOf(r.tenant_id, r.event_id);
    const n = left.get(k) ?? 0;
    if (n > 0) left.set(k, n - 1);
    else dups.push({ tenant_id: r.tenant_id, event_id: r.event_id, partition: r.partition, offset: r.offset });
  }
  return dups;
}

/** Highest offset per partition in the chunk. */
export function maxOffsets(rows: EventRow[]): { partitions: number[]; offsets: string[] } {
  const max = new Map<number, bigint>();
  for (const r of rows) {
    const o = BigInt(r.offset);
    const cur = max.get(r.partition);
    if (cur === undefined || o > cur) max.set(r.partition, o);
  }
  return { partitions: [...max.keys()], offsets: [...max.values()].map(String) };
}

/**
 * Duplicate rows whose incoming content_hash differs from the stored one. Rows without a hash on either side
 * (Stage 2 rows, dedup-off table) cannot be compared and are not conflicts.
 */
export function findConflicts(rows: EventRow[], dupKeys: EventKey[], stored: { tenant_id: string; event_id: string; content_hash: Buffer | null }[]): EventKey[] {
  if (dupKeys.length === 0) return [];
  const storedHash = new Map(stored.map((s) => [keyOf(s.tenant_id, s.event_id), s.content_hash]));
  const isDup = new Set(dupKeys.map((k) => keyOf(k.tenant_id, k.event_id)));
  const out: EventKey[] = [];
  const seen = new Set<string>();
  for (const r of rows) {
    const k = keyOf(r.tenant_id, r.event_id);
    if (!isDup.has(k)) continue;
    const s = storedHash.get(k);
    if (!s || !r.content_hash) continue;
    // A row that was inserted by THIS chunk is compared too: a key repeated inside one chunk with two contents.
    if (!s.equals(r.content_hash) && !seen.has(`${k}\u0000${r.content_hash.toString('hex')}`)) {
      seen.add(`${k}\u0000${r.content_hash.toString('hex')}`);
      out.push({ tenant_id: r.tenant_id, event_id: r.event_id });
    }
  }
  return out;
}

export interface BatchWriterOptions {
  /** K5 (debug-only): dedup=false = plain INSERT into `table`, which has no primary key. */
  table?: string;
  dedup?: boolean;
  /** Where sink_offsets rows go (S3-F2). Omitted = not tracked (unit tests, ad hoc writers). */
  track?: { group: string; topic: string };
  /** Crash points consumer:before-insert / consumer:after-insert-before-commit and injected errors. */
  faults?: Pick<FaultInjector, 'hit' | 'maybeFail'>;
  /** Which points this writer hits (the retry-tier consumer's writer uses retry:before-insert). */
  points?: { beforeInsert?: CrashPointName; afterInsert?: CrashPointName };
  /** Called when the conflict check fails after the commit (logged by the caller). */
  onConflictCheckError?: (e: unknown) => void;
}

export class BatchWriter {
  private readonly sql: string;
  private readonly dedup: boolean;

  constructor(private readonly pool: Pool, private readonly opts: BatchWriterOptions = {}) {
    this.dedup = opts.dedup !== false;
    this.sql = this.dedup ? INSERT_SQL : insertSql(opts.table ?? 'events_nodedup', false);
  }

  /** One transaction: BEGIN, INSERT, sink_offsets upsert, COMMIT. Resolves only after COMMIT returned. */
  async writeChunk(rows: EventRow[]): Promise<WriteResult> {
    if (rows.length === 0) return { attempted: 0, inserted: 0, durationMs: 0, connectMs: 0, freshnessS: [], latenessS: [], duplicateKeys: [], conflicts: [] };
    const c0 = performance.now();
    const client = await this.pool.connect();
    const t0 = performance.now();
    try {
      let inserted: { tenant_id: string; event_id: string; received_at: Date; occurred_at: Date }[];
      await client.query('BEGIN');
      try {
        const before = this.opts.points ? this.opts.points.beforeInsert : 'consumer:before-insert';
        const after = this.opts.points ? this.opts.points.afterInsert : 'consumer:after-insert-before-commit';
        if (before) {
          this.opts.faults?.hit(before, { rows: rows.length });
          this.opts.faults?.maybeFail(before, () => rows.map((r) => r.payload));
        }
        // RETURNING lists only the rows actually inserted (not the ones ON CONFLICT skipped), so freshness is
        // measured for real first writes, never for duplicates.
        const r = await client.query<{ tenant_id: string; event_id: string; received_at: Date; occurred_at: Date }>(this.sql, toColumns(rows, this.dedup));
        inserted = r.rows ?? [];
        if (this.dedup && this.opts.track) {
          const m = maxOffsets(rows);
          await client.query(SINK_OFFSETS_SQL, [this.opts.track.group, this.opts.track.topic, m.partitions, m.offsets]);
        }
        if (after) this.opts.faults?.hit(after, { rows: rows.length });
        await client.query('COMMIT');
      } catch (e) {
        await client.query('ROLLBACK').catch(() => undefined);
        throw e;
      }
      const committedAt = Date.now();
      const durationMs = performance.now() - t0;
      const insertedCount = inserted.length;
      const duplicateKeys = insertedCount === rows.length ? [] : duplicateKeysOf(rows, inserted);
      const result: WriteResult = {
        attempted: rows.length,
        inserted: insertedCount,
        durationMs,
        connectMs: t0 - c0,
        freshnessS: inserted.map((x) => (committedAt - new Date(x.received_at).getTime()) / 1000),
        latenessS: inserted.map((x) => (new Date(x.received_at).getTime() - new Date(x.occurred_at).getTime()) / 1000),
        duplicateKeys,
        conflicts: [],
      };
      if (this.dedup && duplicateKeys.length > 0) await this.checkConflicts(client, rows, duplicateKeys, result);
      return result;
    } finally {
      client.release();
    }
  }

  /** After the commit: the rows are stored whatever happens here, so a failure only loses the comparison. */
  private async checkConflicts(client: PoolClient, rows: EventRow[], dupKeys: EventKey[], result: WriteResult) {
    try {
      const uniq = new Map(dupKeys.map((k) => [keyOf(k.tenant_id, k.event_id), k]));
      const keys = [...uniq.values()];
      const stored = await client.query<{ tenant_id: string; event_id: string; content_hash: Buffer | null }>(STORED_HASHES_SQL, [
        keys.map((k) => k.tenant_id),
        keys.map((k) => k.event_id),
      ]);
      result.conflicts = findConflicts(rows, dupKeys, stored.rows);
    } catch (e) {
      result.conflictCheckFailed = true;
      this.opts.onConflictCheckError?.(e);
    }
  }
}
