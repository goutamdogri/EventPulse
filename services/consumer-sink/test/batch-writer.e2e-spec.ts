// BatchWriter against the real Postgres (migrations through 0005 applied: `pnpm migrate:test`).
// S3-F2: the sink is idempotent on (tenant_id, event_id); duplicates and conflicts are reported, never hidden.
import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { eventContentHash } from '@eventpulse/idempotency';
import { BatchWriter } from '../src/sink/batch-writer.js';
import type { EventRow } from '../src/sink/parse.js';

const TENANT = `writer_e2e_${randomUUID().slice(0, 8)}`;

function row(o: Partial<Omit<EventRow, 'payload'>> & { payload?: Record<string, unknown> } = {}): EventRow {
  const base = {
    event_id: randomUUID(),
    tenant_id: TENANT,
    type: 'order.created',
    occurred_at: '2026-11-12T10:00:00Z',
    schema_version: 1,
    payload: { a: 1, nested: { quote: 'he said "hi"', list: [1, 2] } } as Record<string, unknown>,
    received_at: '2026-11-22T05:00:00.123Z',
    partition: 0,
    offset: '0',
    ...o,
  };
  return { ...base, payload: JSON.stringify(base.payload), content_hash: eventContentHash(base) };
}

describe('BatchWriter (real Postgres)', () => {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 2 });
  const writer = new BatchWriter(pool);
  const count = async (ids: string[], tenant = TENANT) =>
    Number((await pool.query('select count(*) from events where tenant_id = $1 and event_id = any($2::uuid[])', [tenant, ids])).rows[0].count);
  /** Order-independent fingerprint of the stored rows of a tenant (S3-F2: "stored data unchanged"). */
  const checksum = async (tenant = TENANT) =>
    (
      await pool.query(
        `select count(*)::int as n,
                md5(coalesce(string_agg(event_id::text || '|' || encode(coalesce(content_hash, ''), 'hex') || '|' || payload::text
                                        || '|' || stored_at::text, ',' order by event_id), '')) as md5
           from events where tenant_id = $1`,
        [tenant],
      )
    ).rows[0] as { n: number; md5: string };

  afterAll(async () => {
    await pool.query('delete from events where tenant_id like $1', [`${TENANT}%`]);
    await pool.query("delete from sink_offsets where consumer_group like 'writer-e2e-%'");
    await pool.end();
  });

  it('inserts 100 rows in one statement', async () => {
    const rows = Array.from({ length: 100 }, () => row());
    const r = await writer.writeChunk(rows);
    expect(r).toMatchObject({ attempted: 100, inserted: 100, duplicateKeys: [], conflicts: [] });
    expect(await count(rows.map((x) => x.event_id))).toBe(100);
  });

  it('[G2.same-chunk-twice] writing the same chunk again inserts 0, reports every key as a duplicate, table unchanged', async () => {
    const rows = Array.from({ length: 100 }, () => row());
    await writer.writeChunk(rows);
    const before = await checksum();
    const r = await writer.writeChunk(rows);
    expect(r).toMatchObject({ attempted: 100, inserted: 0, conflicts: [] });
    expect(r.duplicateKeys).toHaveLength(100);
    expect(await checksum()).toEqual(before); // nothing changed, not even stored_at
  });

  it('a duplicate inside one chunk is absorbed in the same statement (DO NOTHING, not DO UPDATE)', async () => {
    const a = row();
    const r = await writer.writeChunk([a, row(), { ...a }]);
    expect(r.attempted - r.inserted).toBe(1);
    expect(r.duplicateKeys).toEqual([{ tenant_id: TENANT, event_id: a.event_id }]);
    expect(r.conflicts).toEqual([]);
    expect(await count([a.event_id])).toBe(1);
  });

  it('[G2.tenant-scope] the same event_id under two tenants is stored twice', async () => {
    const id = randomUUID();
    const r = await writer.writeChunk([row({ event_id: id }), row({ event_id: id, tenant_id: `${TENANT}_b` })]);
    expect(r).toMatchObject({ attempted: 2, inserted: 2 });
    expect(await count([id])).toBe(1);
    expect(await count([id], `${TENANT}_b`)).toBe(1);
  });

  it('[G2.conflict] same key, different content: first stored copy wins, the conflict is reported', async () => {
    const original = row({ payload: { v: 'original' } });
    await writer.writeChunk([original]);
    const changed = row({ event_id: original.event_id, payload: { v: 'changed' } });
    const r = await writer.writeChunk([changed]);
    expect(r).toMatchObject({ attempted: 1, inserted: 0, conflicts: [{ tenant_id: TENANT, event_id: original.event_id }] });
    const stored = (await pool.query('select payload from events where tenant_id = $1 and event_id = $2', [TENANT, original.event_id])).rows[0];
    expect(stored.payload).toEqual({ v: 'original' });
  });

  it('a duplicate with the same content in another form (key order, offset time zone) is NOT a conflict', async () => {
    const a = row({ payload: { x: 1, y: 2 } });
    await writer.writeChunk([a]);
    const same = row({ event_id: a.event_id, payload: { y: 2, x: 1 }, occurred_at: '2026-11-12T12:00:00+02:00' });
    expect(await writer.writeChunk([same])).toMatchObject({ inserted: 0, conflicts: [] });
  });

  it('stores received_at from the header, payload as jsonb, content_hash, and stored_at after received_at', async () => {
    const a = row({ received_at: new Date(Date.now() - 1000).toISOString() });
    await writer.writeChunk([a]);
    const r = (
      await pool.query(
        `select tenant_id, payload, received_at, content_hash, stored_at >= received_at as fresh_ok
           from events where tenant_id = $1 and event_id = $2`,
        [TENANT, a.event_id],
      )
    ).rows[0];
    expect(r.tenant_id).toBe(TENANT);
    expect(r.payload).toEqual(JSON.parse(a.payload));
    expect(r.received_at.toISOString()).toBe(a.received_at);
    expect(Buffer.compare(r.content_hash, a.content_hash)).toBe(0);
    expect(r.fresh_ok).toBe(true);
  });

  it('a failing chunk leaves nothing behind (transaction rolled back)', async () => {
    const good = row();
    const bad = { ...row(), occurred_at: 'not-a-time' };
    await expect(writer.writeChunk([good, bad])).rejects.toThrow();
    expect(await count([good.event_id])).toBe(0);
  });

  describe('sink_offsets (same transaction as the rows)', () => {
    const group = `writer-e2e-${randomUUID()}`;
    const tracked = new BatchWriter(pool, { track: { group, topic: 'topic-a' } });
    const offsets = async () =>
      Object.fromEntries(
        (await pool.query('select partition, max_offset from sink_offsets where consumer_group = $1 order by partition', [group])).rows.map(
          (r) => [r.partition, Number(r.max_offset)],
        ),
      );

    it('records the highest offset per partition and never moves it back (GREATEST)', async () => {
      await tracked.writeChunk([row({ partition: 0, offset: '10' }), row({ partition: 0, offset: '12' }), row({ partition: 3, offset: '7' })]);
      expect(await offsets()).toEqual({ 0: 12, 3: 7 });
      await tracked.writeChunk([row({ partition: 0, offset: '5' })]); // a reprocessed / bisected smaller write
      expect(await offsets()).toEqual({ 0: 12, 3: 7 });
      await tracked.writeChunk([row({ partition: 0, offset: '9000000000' })]); // bigint offsets
      expect(await offsets()).toEqual({ 0: 9_000_000_000, 3: 7 });
    });

    it('is rolled back together with the rows when the chunk fails', async () => {
      const before = await offsets();
      await expect(tracked.writeChunk([row({ partition: 0, offset: '9999999999' }), { ...row(), occurred_at: 'bad' }])).rejects.toThrow();
      expect(await offsets()).toEqual(before);
    });

    it('is advanced even when every row was a duplicate (the offset WAS processed)', async () => {
      const r = row({ partition: 5, offset: '1' });
      await tracked.writeChunk([r]);
      await tracked.writeChunk([{ ...r, offset: '2' }]);
      expect((await offsets())[5]).toBe(2);
    });
  });
});
