import { BatchWriter, chunk, insertSql, toColumns } from './batch-writer.js';
import type { EventRow } from './parse.js';

const row = (i: number): EventRow => ({
  event_id: `00000000-0000-4000-8000-${String(i).padStart(12, '0')}`,
  tenant_id: 't',
  type: 'x',
  occurred_at: '2026-11-12T10:00:00Z',
  schema_version: 1,
  payload: '{}',
  received_at: '2026-11-12T10:00:01Z',
  partition: 0,
  offset: String(i),
});

describe('chunk', () => {
  it('splits 2500 rows into 1000/1000/500', () =>
    expect(chunk(Array.from({ length: 2500 }, (_, i) => i), 1000).map((c) => c.length)).toEqual([1000, 1000, 500]));

  it('empty input gives no chunks', () => expect(chunk([], 10)).toEqual([]));
});

describe('toColumns', () => {
  it('builds one array per column, in INSERT order', () => {
    const cols = toColumns([row(1), row(2)]);
    expect(cols).toHaveLength(7);
    expect(cols[0]).toEqual([row(1).event_id, row(2).event_id]);
    expect(cols[6]).toEqual(['2026-11-12T10:00:01Z', '2026-11-12T10:00:01Z']);
  });
});

describe('BatchWriter.writeChunk (fake pool)', () => {
  function fakePool(insertResult: () => Promise<any>) {
    const queries: string[] = [];
    const client = {
      query: vi.fn(async (sql: string) => {
        queries.push(sql.trim().split(/\s+/)[0]);
        return sql.includes('INSERT') ? insertResult() : {};
      }),
      release: vi.fn(),
    };
    return { pool: { connect: vi.fn(async () => client) } as any, client, queries };
  }

  it('BEGIN, INSERT, COMMIT and reports attempted vs inserted', async () => {
    const f = fakePool(async () => ({ rowCount: 2 }));
    const r = await new BatchWriter(f.pool).writeChunk([row(1), row(2), row(3)]);
    expect(f.queries).toEqual(['BEGIN', 'INSERT', 'COMMIT']);
    expect(r).toMatchObject({ attempted: 3, inserted: 2 });
    expect(f.client.release).toHaveBeenCalled();
  });

  it('rolls back and rethrows when the INSERT fails, and releases the client', async () => {
    const f = fakePool(async () => {
      throw new Error('boom');
    });
    await expect(new BatchWriter(f.pool).writeChunk([row(1)])).rejects.toThrow('boom');
    expect(f.queries).toEqual(['BEGIN', 'INSERT', 'ROLLBACK']);
    expect(f.client.release).toHaveBeenCalled();
  });

  it('does not touch the pool for an empty chunk', async () => {
    const f = fakePool(async () => ({ rowCount: 0 }));
    expect(await new BatchWriter(f.pool).writeChunk([])).toMatchObject({ attempted: 0, inserted: 0 });
    expect(f.pool.connect).not.toHaveBeenCalled();
  });
});

describe('insertSql (K5 dedup switch)', () => {
  it('dedup on keeps ON CONFLICT; off writes plainly into the named table', () => {
    expect(insertSql()).toContain('ON CONFLICT (event_id) DO NOTHING');
    const off = insertSql('events_nodedup', false);
    expect(off).toContain('INSERT INTO events_nodedup');
    expect(off).not.toContain('ON CONFLICT');
  });
});
