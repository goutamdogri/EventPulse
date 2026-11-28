import { PartitionProgress, reprocessedMessages, splitByCause } from './progress.js';

const dup = (offset: string) => ({ tenant_id: 't', event_id: `e${offset}`, partition: 0, offset });

describe('splitByCause (S3-F3)', () => {
  it('offset <= stored max: reprocessed; above: redelivered (a new offset carrying an existing key)', () => {
    expect(splitByCause([dup('5'), dup('10'), dup('11'), dup('400')], 10n)).toEqual({ reprocessed: 2, redelivered: 2, unknown: 0 });
  });

  it('nothing stored yet for the partition: every duplicate is redelivered', () => {
    expect(splitByCause([dup('0'), dup('1')], undefined)).toEqual({ reprocessed: 0, redelivered: 2, unknown: 0 });
  });

  it('stored max unknown (lookup failed): counted as unknown, never dropped', () => {
    expect(splitByCause([dup('0'), dup('1')], null)).toEqual({ reprocessed: 0, redelivered: 0, unknown: 2 });
  });

  it('compares offsets as numbers, not strings', () => {
    expect(splitByCause([dup('9')], 10n).reprocessed).toBe(1);
    expect(splitByCause([dup('100')], 99n).redelivered).toBe(1);
    expect(splitByCause([dup('9000000001')], 9_000_000_000n).redelivered).toBe(1);
  });

  it('the causes always add up to the duplicates', () => {
    const d = [dup('1'), dup('2'), dup('3')];
    for (const m of [undefined, null, 0n, 2n, 3n]) {
      const s = splitByCause(d, m);
      expect(s.reprocessed + s.redelivered + s.unknown).toBe(d.length);
    }
  });
});

describe('reprocessedMessages', () => {
  it('counts messages at or below the stored max; none when nothing is known', () => {
    expect(reprocessedMessages(['8', '9', '10', '11'], 10n)).toBe(3);
    expect(reprocessedMessages(['8'], undefined)).toBe(0);
    expect(reprocessedMessages(['8'], null)).toBe(0);
  });
});

describe('PartitionProgress', () => {
  const pool = (rows: Record<number, string>, fail = false) => {
    const query = vi.fn(async (_sql: string, p: unknown[]) => {
      if (fail) throw new Error('db down');
      const v = rows[p[2] as number];
      return { rows: v === undefined ? [] : [{ max_offset: v }] };
    });
    return { query } as any;
  };

  it('reads sink_offsets once per partition, then answers from memory', async () => {
    const p = pool({ 0: '41' });
    const pr = new PartitionProgress(p, 'g', 't');
    expect(await pr.known(0)).toBe(41n);
    expect(await pr.known(0)).toBe(41n);
    expect(await pr.known(3)).toBeUndefined();
    expect(p.query).toHaveBeenCalledTimes(2);
    expect(p.query.mock.calls[0][1]).toEqual(['g', 't', 0]);
  });

  it('advance moves the cached max forward only, and only for loaded partitions', async () => {
    const pr = new PartitionProgress(pool({ 0: '41' }), 'g', 't');
    await pr.known(0);
    pr.advance(0, '60');
    pr.advance(0, '50');
    expect(await pr.known(0)).toBe(60n);
    pr.advance(7, '5'); // not loaded: the database is the truth for it
    expect(await pr.known(7)).toBeUndefined();
  });

  it('a partition with nothing stored becomes known after the first write', async () => {
    const pr = new PartitionProgress(pool({}), 'g', 't');
    expect(await pr.known(2)).toBeUndefined();
    pr.advance(2, '9');
    expect(await pr.known(2)).toBe(9n);
  });

  it('forget drops the cache (rebalance): the next lookup reads the database again', async () => {
    const p = pool({ 0: '41' });
    const pr = new PartitionProgress(p, 'g', 't');
    await pr.known(0);
    pr.forget([0]);
    await pr.known(0);
    expect(p.query).toHaveBeenCalledTimes(2);
  });

  it('a failed lookup returns null (cause unknown) and is not cached', async () => {
    const onError = vi.fn();
    const pr = new PartitionProgress(pool({}, true), 'g', 't', onError);
    expect(await pr.known(0)).toBeNull();
    expect(await pr.known(0)).toBeNull();
    expect(onError).toHaveBeenCalledTimes(2);
  });
});
