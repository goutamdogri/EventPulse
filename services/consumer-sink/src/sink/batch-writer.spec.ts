import { FaultInjector, loadFaultConfig } from '@eventpulse/fault-injector';
import { BatchWriter, chunk, duplicateKeysOf, findConflicts, insertSql, maxOffsets, toColumns } from './batch-writer.js';
import type { EventRow } from './parse.js';

const hash = (s: string) => Buffer.from(s.padEnd(32, '.'));

const row = (i: number, o: Partial<EventRow> = {}): EventRow => ({
  event_id: `00000000-0000-4000-8000-${String(i).padStart(12, '0')}`,
  tenant_id: 't',
  type: 'x',
  occurred_at: '2026-11-12T10:00:00Z',
  schema_version: 1,
  payload: '{}',
  received_at: '2026-11-12T10:00:01Z',
  content_hash: hash(`h${i}`),
  partition: 0,
  offset: String(i),
  ...o,
});
const key = (r: EventRow) => ({ tenant_id: r.tenant_id, event_id: r.event_id });
const dup = (r: EventRow) => ({ ...key(r), partition: r.partition, offset: r.offset });

describe('chunk', () => {
  it('splits 2500 rows into 1000/1000/500', () =>
    expect(chunk(Array.from({ length: 2500 }, (_, i) => i), 1000).map((c) => c.length)).toEqual([1000, 1000, 500]));

  it('empty input gives no chunks', () => expect(chunk([], 10)).toEqual([]));
});

describe('toColumns', () => {
  it('builds one array per column, in INSERT order, content_hash last', () => {
    const cols = toColumns([row(1), row(2)]);
    expect(cols).toHaveLength(8);
    expect(cols[0]).toEqual([row(1).event_id, row(2).event_id]);
    expect(cols[6]).toEqual(['2026-11-12T10:00:01Z', '2026-11-12T10:00:01Z']);
    expect(cols[7]).toEqual([hash('h1'), hash('h2')]);
  });

  it('the dedup-off table gets the 7 Stage 2 columns', () => {
    expect(toColumns([row(1)], false)).toHaveLength(7);
  });
});

describe('insertSql', () => {
  it('dedup on: idempotent on (tenant_id, event_id), DO NOTHING, returns the inserted keys', () => {
    const sql = insertSql();
    expect(sql).toContain('ON CONFLICT (tenant_id, event_id) DO NOTHING');
    expect(sql).not.toContain('DO UPDATE');
    expect(sql).toContain('content_hash');
    expect(sql).toMatch(/RETURNING tenant_id, event_id, received_at, occurred_at/);
  });

  it('dedup off (K5, debug-only): plain INSERT into the named table', () => {
    const off = insertSql('events_nodedup', false);
    expect(off).toContain('INSERT INTO events_nodedup');
    expect(off).not.toContain('ON CONFLICT');
  });
});

describe('duplicateKeysOf', () => {
  it('keys sent minus keys returned', () => {
    const rows = [row(1), row(2), row(3)];
    expect(duplicateKeysOf(rows, [key(rows[0]), key(rows[2])])).toEqual([dup(rows[1])]);
  });

  it('a key repeated inside the chunk counts once per extra copy', () => {
    const rows = [row(1), row(2), row(1, { offset: '7' })];
    expect(duplicateKeysOf(rows, [key(rows[0]), key(rows[1])])).toEqual([dup(rows[2])]); // the later copy, with its own offset
  });

  it('the same event_id under two tenants is two different keys', () => {
    const a = row(1, { tenant_id: 'a' });
    const b = row(1, { tenant_id: 'b' });
    expect(duplicateKeysOf([a, b], [key(a), key(b)])).toEqual([]);
    expect(duplicateKeysOf([a, b], [key(a)])).toEqual([dup(b)]);
  });

  it('UUID case does not matter (Postgres returns lower case)', () => {
    const r = row(1, { event_id: 'ABCDEF00-0000-4000-8000-000000000001' });
    expect(duplicateKeysOf([r], [{ tenant_id: 't', event_id: 'abcdef00-0000-4000-8000-000000000001' }])).toEqual([]);
  });
});

describe('maxOffsets', () => {
  it('highest offset per partition, compared as numbers (not strings)', () => {
    const m = maxOffsets([row(1, { offset: '9' }), row(2, { offset: '10' }), row(3, { partition: 2, offset: '5' })]);
    expect(Object.fromEntries(m.partitions.map((p, i) => [p, m.offsets[i]]))).toEqual({ 0: '10', 2: '5' });
  });
});

describe('findConflicts', () => {
  it('a duplicate with the same hash is a true duplicate, a different hash is a conflict', () => {
    const same = row(1);
    const other = row(2, { content_hash: hash('changed') });
    const stored = [
      { ...key(same), content_hash: hash('h1') },
      { ...key(other), content_hash: hash('h2') },
    ];
    expect(findConflicts([same, other], [key(same), key(other)], stored)).toEqual([key(other)]);
  });

  it('rows without a stored hash (Stage 2 rows) cannot be compared and are not conflicts', () => {
    const r = row(1);
    expect(findConflicts([r], [key(r)], [{ ...key(r), content_hash: null }])).toEqual([]);
  });

  it('a key repeated inside one chunk with two contents is one conflict', () => {
    const a = row(1);
    const b = row(1, { content_hash: hash('other') });
    expect(findConflicts([a, b], [key(b)], [{ ...key(a), content_hash: hash('h1') }])).toEqual([key(b)]);
  });
});

describe('BatchWriter.writeChunk (fake pool)', () => {
  function fakePool(insert: (rows: EventRow[], params: unknown[][]) => Promise<any>, storedHashes: any[] = []) {
    const queries: string[] = [];
    const params: unknown[][] = [];
    let sent: EventRow[] = [];
    const client = {
      query: vi.fn(async (sql: string, p?: unknown[]) => {
        const s = sql.trim();
        queries.push(s.startsWith('INSERT INTO sink_offsets') ? 'SINK_OFFSETS' : s.startsWith('SELECT e.tenant_id') ? 'HASHES' : s.split(/\s+/)[0]);
        params.push((p ?? []) as unknown[]);
        if (s.startsWith('INSERT INTO events')) return insert(sent, (p ?? []) as unknown[][]);
        if (s.startsWith('SELECT e.tenant_id')) return { rows: storedHashes };
        return {};
      }),
      release: vi.fn(),
    };
    const pool = { connect: vi.fn(async () => client) } as any;
    return { pool, client, queries, params, setSent: (r: EventRow[]) => (sent = r) };
  }
  const returning = (rows: EventRow[]) => ({ rows: rows.map((r) => ({ ...key(r), received_at: new Date(r.received_at) })) });

  it('BEGIN, INSERT, sink_offsets, COMMIT; reports attempted vs inserted and which keys were duplicates', async () => {
    const rows = [row(1), row(2), row(3)];
    const f = fakePool(async () => returning([rows[0], rows[2]]), [{ ...key(rows[1]), content_hash: hash('h2') }]);
    f.setSent(rows);
    const r = await new BatchWriter(f.pool, { track: { group: 'g', topic: 't' } }).writeChunk(rows);
    expect(f.queries).toEqual(['BEGIN', 'INSERT', 'SINK_OFFSETS', 'COMMIT', 'HASHES']);
    expect(r).toMatchObject({ attempted: 3, inserted: 2, duplicateKeys: [dup(rows[1])], conflicts: [] });
    expect(f.params[2]).toEqual(['g', 't', [0], ['3']]);
    expect(f.client.release).toHaveBeenCalled();
  });

  it('no duplicates: no conflict query (no cost on the happy path); no track: no sink_offsets', async () => {
    const rows = [row(1), row(2)];
    const f = fakePool(async () => returning(rows));
    const r = await new BatchWriter(f.pool).writeChunk(rows);
    expect(f.queries).toEqual(['BEGIN', 'INSERT', 'COMMIT']);
    expect(r.duplicateKeys).toEqual([]);
  });

  it('reports conflicting duplicates (stored hash differs) without failing the write', async () => {
    const rows = [row(1)];
    const f = fakePool(async () => ({ rows: [] }), [{ ...key(rows[0]), content_hash: hash('stored-other') }]);
    const r = await new BatchWriter(f.pool).writeChunk(rows);
    expect(r).toMatchObject({ inserted: 0, conflicts: [key(rows[0])] });
  });

  it('a failing conflict check does not fail the (already committed) write', async () => {
    const rows = [row(1)];
    const f = fakePool(async () => ({ rows: [] }));
    f.client.query.mockImplementation(async (sql: string) => {
      if (sql.trim().startsWith('SELECT e.tenant_id')) throw new Error('db gone');
      return sql.trim().startsWith('INSERT') ? { rows: [] } : {};
    });
    const onErr = vi.fn();
    const r = await new BatchWriter(f.pool, { onConflictCheckError: onErr }).writeChunk(rows);
    expect(r).toMatchObject({ attempted: 1, inserted: 0, conflictCheckFailed: true });
    expect(onErr).toHaveBeenCalled();
  });

  it('rolls back and rethrows when the INSERT fails, and releases the client', async () => {
    const f = fakePool(async () => {
      throw new Error('boom');
    });
    await expect(new BatchWriter(f.pool).writeChunk([row(1)])).rejects.toThrow('boom');
    expect(f.queries).toEqual(['BEGIN', 'INSERT', 'ROLLBACK']);
    expect(f.client.release).toHaveBeenCalled();
  });

  it('a failing sink_offsets upsert rolls the rows back too (same transaction)', async () => {
    const f = fakePool(async () => ({ rows: [] }));
    f.client.query.mockImplementation(async (sql: string) => {
      const s = sql.trim();
      f.queries.push(s.split(/\s+/).slice(0, 3).join(' '));
      if (s.startsWith('INSERT INTO sink_offsets')) throw new Error('offsets failed');
      return s.startsWith('INSERT') ? { rows: [] } : {};
    });
    await expect(new BatchWriter(f.pool, { track: { group: 'g', topic: 't' } }).writeChunk([row(1)])).rejects.toThrow('offsets failed');
    expect(f.queries.at(-1)).toBe('ROLLBACK');
  });

  it('does not touch the pool for an empty chunk', async () => {
    const f = fakePool(async () => ({ rows: [] }));
    expect(await new BatchWriter(f.pool).writeChunk([])).toMatchObject({ attempted: 0, inserted: 0 });
    expect(f.pool.connect).not.toHaveBeenCalled();
  });

  it('crash points inside the transaction: before-insert and after-insert-before-commit', async () => {
    for (const [point, expected] of [
      ['consumer:before-insert', ['BEGIN']],
      ['consumer:after-insert-before-commit', ['BEGIN', 'INSERT']],
    ] as const) {
      const f = fakePool(async () => ({ rows: [] }));
      const kill = vi.fn(() => {
        throw new Error('SIGKILL');
      });
      const faults = new FaultInjector(loadFaultConfig({ DEBUG_FAULTS: 'true', FAULTS: point }), kill, () => undefined);
      await expect(new BatchWriter(f.pool, { faults }).writeChunk([row(1)])).rejects.toThrow('SIGKILL');
      expect(f.queries.slice(0, expected.length)).toEqual(expected);
      expect(f.queries).not.toContain('COMMIT');
    }
  });

  it('injected errors only hit writes that contain the marker (so bisection can isolate them)', async () => {
    const faults = new FaultInjector(loadFaultConfig({ DEBUG_FAULTS: 'true', FAULT_ERRORS: 'consumer:before-insert=23502x*:marker=bad' }), () => undefined);
    const f = fakePool(async (_s, p) => ({ rows: (p[0] as string[]).map((id) => ({ tenant_id: 't', event_id: id, received_at: new Date() })) }));
    const w = new BatchWriter(f.pool, { faults });
    await expect(w.writeChunk([row(1), row(2, { payload: '{"tag":"bad"}' })])).rejects.toMatchObject({ code: '23502' });
    await expect(w.writeChunk([row(1)])).resolves.toMatchObject({ inserted: 1 });
  });
});
