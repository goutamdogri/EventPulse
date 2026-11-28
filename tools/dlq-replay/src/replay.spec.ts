import { runReplay } from './replay.ts';
import type { DlqRecord, ReplayDeps, ReplayOptions } from './replay.ts';
import type { MetaRow } from './store.ts';

const row = (id: number, o: Partial<MetaRow> = {}): MetaRow => ({
  id: String(id),
  source_topic: 'events.raw',
  source_partition: 0,
  source_offset: String(100 + id),
  failed_topic: 'events.raw',
  dlq_topic: 'events.dlq',
  dlq_partition: 0,
  dlq_offset: String(id),
  tenant_id: 't1',
  event_id: null,
  error_class: 'PERMANENT_DB',
  error_code: '22001',
  error_message: 'too long',
  attempts: 0,
  first_failed_at: new Date('2026-11-28T10:00:00Z'),
  last_failed_at: new Date('2026-11-28T10:00:00Z'),
  replayed_at: null,
  replay_count: 0,
  last_replay_id: null,
  ...o,
});

function fakes(rows: MetaRow[], records: Record<string, DlqRecord>, o: { poison?: Set<string>; produceFailsAt?: number; confirm?: boolean } = {}) {
  const produced: { topic: string; m: any }[] = [];
  const marked: string[] = [];
  const replays: any[] = [];
  const hits: string[] = [];
  const sleeps: number[] = [];
  let clock = 0;
  const deps: ReplayDeps = {
    store: {
      select: async () => rows,
      markReplayed: async (id) => void marked.push(id),
      startReplay: async (...a) => void replays.push({ start: a }),
      finishReplay: async (...a) => void replays.push({ finish: a }),
    },
    read: async (_t, at) => new Map(at.map((a) => [`${a.partition}:${a.offset}`, records[`${a.partition}:${a.offset}`]]).filter(([, r]) => r) as [string, DlqRecord][]),
    produce: async (topic, m) => {
      if (o.produceFailsAt !== undefined && produced.length === o.produceFailsAt) throw new Error('broker down');
      produced.push({ topic, m });
    },
    check: {
      prepare: async () => undefined,
      check: (m) => (o.poison?.has(m.value!.toString()) ? { ok: false, errorClass: 'PERMANENT_PARSE', code: 'invalid_json', message: 'bad' } : { ok: true }),
    },
    confirm: async () => o.confirm ?? true,
    hit: (p) => void hits.push(p),
    log: () => undefined,
    sleep: async (ms) => {
      sleeps.push(ms);
      clock += ms;
    },
    now: () => clock,
  };
  return { deps, produced, marked, replays, hits, sleeps };
}

const rec = (v: string, headers: Record<string, string> = {}): DlqRecord => ({ key: Buffer.from(`k-${v}`), value: Buffer.from(v), headers: Object.fromEntries(Object.entries({ received_at: '2026-11-28T10:00:00Z', 'dlq.error_class': 'PERMANENT_DB', ...headers }).map(([k, x]) => [k, Buffer.from(x)])) });
const opts = (o: Partial<ReplayOptions> = {}): ReplayOptions => ({ execute: false, yes: true, includeFailing: false, rate: 0, maxReplays: 3, operator: 'test', replayId: 'r-1', ...o });

describe('runReplay', () => {
  const rows = [row(1), row(2), row(3), row(4, { tenant_id: null, error_class: 'PERMANENT_PARSE' })];
  const records = { '0:1': rec('ok1'), '0:2': rec('ok2'), '0:3': rec('bad3'), '0:4': rec('bad4') };
  const poison = new Set(['bad3', 'bad4']);

  it('dry run (default): nothing produced, nothing marked, a dry_run bookkeeping row, plan counts', async () => {
    const f = fakes(rows, records, { poison });
    const r = await runReplay(f.deps, {}, opts());
    expect(f.produced).toEqual([]);
    expect(f.marked).toEqual([]);
    expect(r).toMatchObject({ dryRun: true, status: 'dry_run', counts: { selected: 4, replayed: 0, skipped: 2, failed: 0 }, plan: { replay: 2, wouldFail: 2, loopGuard: 0, missing: 0 } });
    expect(r.byClass).toEqual({ PERMANENT_DB: 3, PERMANENT_PARSE: 1 });
    expect(r.byTenant).toEqual({ t1: 3, '(unparsed)': 1 });
    expect(f.replays).toEqual([{ start: ['r-1', 'test', {}, true, 'dry_run', r.counts] }]);
  });

  it('--execute: produces the good ones to source_topic with original key/value + replay headers, marks after each ack', async () => {
    const f = fakes(rows, records, { poison });
    const r = await runReplay(f.deps, {}, opts({ execute: true }));
    expect(f.produced.map((p) => [p.topic, p.m.value.toString(), p.m.key.toString()])).toEqual([
      ['events.raw', 'ok1', 'k-ok1'],
      ['events.raw', 'ok2', 'k-ok2'],
    ]);
    expect(f.produced[0].m.headers).toEqual({ received_at: Buffer.from('2026-11-28T10:00:00Z'), 'replay.id': 'r-1', 'replay.count': '1', 'replay.dlq_partition': '0', 'replay.dlq_offset': '1' });
    expect(f.marked).toEqual(['1', '2']);
    expect(f.hits).toEqual(['replay:after-produce-before-mark', 'replay:after-produce-before-mark']);
    expect(r).toMatchObject({ status: 'done', counts: { selected: 4, replayed: 2, skipped: 2, failed: 0 } });
    expect(f.replays.at(-1)).toEqual({ finish: ['r-1', 'done', r.counts] });
  });

  it('--include-failing replays the pre-check failures too (forced)', async () => {
    const f = fakes(rows, records, { poison });
    const r = await runReplay(f.deps, {}, opts({ execute: true, includeFailing: true }));
    expect(f.produced).toHaveLength(4);
    expect(r.plan).toMatchObject({ replay: 4, forced: 2 });
  });

  it('loop guard: a dead letter that already went through DLQ_MAX_REPLAYS rounds is skipped', async () => {
    const f = fakes([row(1)], { '0:1': rec('ok1', { 'replay.count': '3' }) });
    const r = await runReplay(f.deps, {}, opts({ execute: true }));
    expect(f.produced).toEqual([]);
    expect(r).toMatchObject({ counts: { skipped: 1, replayed: 0 }, plan: { loopGuard: 1 } });
  });

  it('a record missing from the DLQ topic is counted as failed, the others still replay', async () => {
    const f = fakes([row(1), row(9)], { '0:1': rec('ok1') });
    const r = await runReplay(f.deps, {}, opts({ execute: true }));
    expect(f.marked).toEqual(['1']);
    expect(r.counts).toEqual({ selected: 2, replayed: 1, skipped: 0, failed: 1 });
  });

  it('a produce error stops the run: the row is NOT marked (re-running replays it), status failed', async () => {
    const f = fakes([row(1), row(2), row(3)], { '0:1': rec('a'), '0:2': rec('b'), '0:3': rec('c') }, { produceFailsAt: 1 });
    const r = await runReplay(f.deps, {}, opts({ execute: true }));
    expect(f.marked).toEqual(['1']);
    expect(r).toMatchObject({ status: 'failed', error: 'broker down', counts: { replayed: 1, failed: 2 } });
  });

  it('not confirmed: aborted, nothing produced', async () => {
    const f = fakes([row(1)], { '0:1': rec('a') }, { confirm: false });
    const r = await runReplay(f.deps, {}, opts({ execute: true, yes: false }));
    expect(r.status).toBe('aborted');
    expect(f.produced).toEqual([]);
  });

  it('--rate paces the produces (10/s = 100 ms apart)', async () => {
    const f = fakes([row(1), row(2), row(3)], { '0:1': rec('a'), '0:2': rec('b'), '0:3': rec('c') });
    await runReplay(f.deps, {}, opts({ execute: true, rate: 10 }));
    expect(f.sleeps).toEqual([100, 100]);
  });
});
