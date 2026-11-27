// BatchWriter against the real Postgres (migrations through 0004 applied).
import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { BatchWriter } from '../src/sink/batch-writer.js';
import type { EventRow } from '../src/sink/parse.js';

const row = (o: Partial<EventRow> = {}): EventRow => ({
  event_id: randomUUID(),
  tenant_id: 'writer_e2e',
  type: 'order.created',
  occurred_at: '2026-11-12T10:00:00Z',
  schema_version: 1,
  payload: JSON.stringify({ a: 1, nested: { quote: 'he said "hi"', list: [1, 2] } }),
  received_at: '2026-11-22T05:00:00.123Z',
  partition: 0,
  offset: '0',
  ...o,
});

describe('BatchWriter (real Postgres)', () => {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 2 });
  const writer = new BatchWriter(pool);
  const count = async (ids: string[]) =>
    Number((await pool.query('select count(*) from events where event_id = any($1::uuid[])', [ids])).rows[0].count);

  afterAll(() => pool.end());

  it('inserts 100 rows in one statement', async () => {
    const rows = Array.from({ length: 100 }, () => row());
    const r = await writer.writeChunk(rows);
    expect(r).toMatchObject({ attempted: 100, inserted: 100 });
    expect(await count(rows.map((x) => x.event_id))).toBe(100);
  });

  it('writing the same 100 again inserts 0 and keeps 100 rows (duplicates visible)', async () => {
    const rows = Array.from({ length: 100 }, () => row());
    await writer.writeChunk(rows);
    const r = await writer.writeChunk(rows);
    expect(r).toMatchObject({ attempted: 100, inserted: 0 });
    expect(await count(rows.map((x) => x.event_id))).toBe(100);
  });

  it('a duplicate inside one chunk is absorbed in the same statement', async () => {
    const a = row();
    const r = await writer.writeChunk([a, row(), { ...a }]);
    expect(r.attempted - r.inserted).toBe(1);
    expect(await count([a.event_id])).toBe(1);
  });

  it('stores received_at from the header, payload as jsonb, and stored_at after received_at', async () => {
    const a = row({ received_at: new Date(Date.now() - 1000).toISOString() });
    await writer.writeChunk([a]);
    const r = (
      await pool.query(
        `select tenant_id, payload, received_at, stored_at >= received_at as fresh_ok from events where event_id = $1`,
        [a.event_id],
      )
    ).rows[0];
    expect(r.tenant_id).toBe('writer_e2e');
    expect(r.payload).toEqual(JSON.parse(a.payload));
    expect(r.received_at.toISOString()).toBe(a.received_at);
    expect(r.fresh_ok).toBe(true);
  });

  it('a failing chunk leaves nothing behind (transaction rolled back)', async () => {
    const good = row();
    await expect(writer.writeChunk([good, row({ occurred_at: 'not-a-time' })])).rejects.toThrow();
    expect(await count([good.event_id])).toBe(0);
  });
});
