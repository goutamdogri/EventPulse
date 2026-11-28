import { encodeEvent } from '@eventpulse/kafka-config';
import { FaultInjector } from '@eventpulse/fault-injector';
import { RETRY_HEADERS, originalHeaders, retryHeaders, route } from '@eventpulse/retry';
import { SinkStats } from '../observability/sink-stats.js';
import type { EventRow, RawMessage } from '../sink/parse.js';
import { DbUnavailableError } from '../consumer/batch-handler.js';
import { NotDueError, handleRetryBatch } from './retry-handler.js';
import { DLQ_HEADERS, DlqMetaStore, dlqMetaColumns, toDlqMessage } from '../sink/dlq-publisher.js';

const policy = { topics: ['r1', 'r2', 'r3'], delaysMs: [1000, 2000, 3000], jitter: 'equal' as const };
const origin = { topic: 'events.raw', partition: 4, offset: '77', firstFailedAt: '2026-11-28T10:00:00.000Z' };
const pgErr = (code: string) => Object.assign(new Error(`pg ${code}`), { code });

let n = 0;
/** A record as it sits on tier `attempt`, due at notBefore. */
function retryMsg(attempt: number, notBefore: number): RawMessage {
  const e = encodeEvent({
    tenantId: 't',
    event: { event_id: `00000000-0000-4000-8000-${String(++n).padStart(12, '0')}`, type: 'x', occurred_at: '2026-11-12T10:00:00Z', schema_version: 1, payload: {} },
    receivedAt: new Date('2026-11-12T10:00:01Z'),
    requestId: 'r',
    keyMode: 'tenant',
  });
  const r = { kind: 'retry' as const, tier: attempt, topic: policy.topics[attempt - 1], notBefore, attempt };
  return { partition: 0, offset: String(n), key: e.key, value: e.value, headers: retryHeaders(e.headers, r, origin, { errorClass: 'TRANSIENT_RECORD', errorCode: '40001', errorMessage: 'm' }) };
}

function setup(o: { write?: (rows: EventRow[]) => Promise<any>; now?: number; tier?: number } = {}) {
  const calls: string[] = [];
  const stats = new SinkStats();
  const retried: any[] = [];
  const dead: any[] = [];
  const ctx = (messages: RawMessage[]) => ({
    messages,
    resolveOffset: (off: string) => void calls.push(`resolve:${off}`),
    commitOffsetsIfNecessary: async () => void calls.push('commit'),
    heartbeat: async () => undefined,
    isRunning: () => true,
    isStale: () => false,
    pausePartition: (ms: number) => void calls.push(`pause:${ms}`),
  });
  const deps = {
    tier: o.tier ?? 1,
    topic: policy.topics[(o.tier ?? 1) - 1],
    policy,
    writer: { writeChunk: vi.fn(o.write ?? (async (rows: EventRow[]) => (calls.push('write'), { attempted: rows.length, inserted: rows.length, durationMs: 1 }))) },
    stats,
    dlq: { publish: vi.fn(async (ls: any[]) => (dead.push(...ls), calls.push(`dlq:${ls.length}`), ls.map((letter) => ({ letter, origin: letter.origin, failedTopic: letter.failedTopic, dlqTopic: 'dlq', dlqPartition: 0, dlqOffset: '1', failedAt: new Date() })))) },
    retryOut: { publish: vi.fn(async (ls: any[]) => void (retried.push(...ls), calls.push(`retry:${ls.length}`))) },
    faults: new FaultInjector(undefined, () => undefined),
    pauseMs: 5000,
    now: () => o.now ?? 10_000,
    rand: () => 1,
    commit: async (c: any) => c.commitOffsetsIfNecessary(),
  };
  return { calls, stats, retried, dead, ctx, deps };
}

describe('handleRetryBatch (S3-F5)', () => {
  it('not due yet: commits what is done, pauses the partition until not_before, never sleeps or writes', async () => {
    const s = setup({ now: 10_000 });
    const due = retryMsg(1, 9_000);
    const later = retryMsg(1, 12_500);
    await expect(handleRetryBatch(s.ctx([due, later]), s.deps)).rejects.toBeInstanceOf(NotDueError);
    expect(s.calls).toEqual(['write', `resolve:${due.offset}`, 'commit', 'pause:2500']);
    expect(s.stats.retryWaits).toBe(1);
  });

  it('due and the write works: stored, counted per tier with its lag, committed', async () => {
    const s = setup({ now: 10_000 });
    await handleRetryBatch(s.ctx([retryMsg(1, 9_000)]), s.deps);
    expect(s.stats.retryProcessedByTier['1']).toEqual({ stored: 1 });
    expect(s.stats.inserted).toBe(1);
    expect(s.calls.at(-1)).toBe('commit');
  });

  it('already stored by someone else: a duplicate (redelivered), not an error', async () => {
    const s = setup({ write: async (rows) => ({ attempted: rows.length, inserted: 0, durationMs: 1 }) });
    await handleRetryBatch(s.ctx([retryMsg(2, 0)]), { ...s.deps, tier: 2, topic: 'r2' });
    expect(s.stats.duplicatesByCause.redelivered).toBe(1);
    expect(s.stats.retryProcessedByTier['2']).toEqual({ duplicate: 1 });
  });

  it('fails again with a retriable class: next tier, attempt+1, ORIGIN and first failure preserved', async () => {
    const s = setup({ write: async () => { throw pgErr('40001'); }, now: 10_000 });
    await handleRetryBatch(s.ctx([retryMsg(1, 0)]), s.deps);
    expect(s.retried).toHaveLength(1);
    expect(s.retried[0]).toMatchObject({ route: { tier: 2, topic: 'r2', attempt: 2, notBefore: 12_000 }, origin });
    expect(s.dead).toEqual([]);
    expect(s.stats.retryProcessedByTier['1']).toEqual({ next_tier: 1 });
    expect(s.stats.consumed).toBe(s.stats.inserted + s.stats.duplicates + s.stats.dlq + s.stats.retried);
  });

  it('fails on the LAST tier: dead letter with attempts = tiers tried, origin = events.raw, failed_topic = the tier', async () => {
    const s = setup({ write: async () => { throw pgErr('40001'); }, tier: 3 });
    await handleRetryBatch(s.ctx([retryMsg(3, 0)]), s.deps);
    expect(s.retried).toEqual([]);
    expect(s.dead).toHaveLength(1);
    expect(s.dead[0]).toMatchObject({ errorClass: 'TRANSIENT_RECORD', errorCode: '40001', attempts: 3, origin, failedTopic: 'r3', tenantId: 't' });
  });

  it('a permanent failure on a tier: dead letter at once', async () => {
    const s = setup({ write: async () => { throw pgErr('23502'); } });
    await handleRetryBatch(s.ctx([retryMsg(1, 0)]), s.deps);
    expect(s.dead[0]).toMatchObject({ errorClass: 'PERMANENT_DB', attempts: 1, failedTopic: 'r1' });
  });

  it('the database down while retrying: pause and retry this record later, never DLQ, nothing committed', async () => {
    const s = setup({ write: async () => { throw pgErr('ECONNREFUSED'); } });
    await expect(handleRetryBatch(s.ctx([retryMsg(1, 0)]), s.deps)).rejects.toBeInstanceOf(DbUnavailableError);
    expect(s.calls).toEqual(['pause:5000']);
    expect(s.dead).toEqual([]);
    expect(s.retried).toEqual([]);
  });

  it('a record without retry headers is treated as attempt 1, due now, origin = where it is', async () => {
    const s = setup({ write: async () => { throw pgErr('40001'); } });
    const r = retryMsg(1, 0);
    const m = { ...r, headers: originalHeaders(r.headers) }; // the event's own headers, no retry.*
    await handleRetryBatch(s.ctx([m]), s.deps);
    expect(s.retried[0]).toMatchObject({ route: { tier: 2 }, origin: { topic: 'r1', partition: 0, offset: m.offset } });
  });
});

describe('dead letters and dlq_meta', () => {
  const raw = { partition: 1, offset: '9', key: 'k', value: Buffer.from('v'), headers: retryHeaders({ request_id: 'r' }, route(0, 'TRANSIENT_RECORD', policy, 0) as any, origin, { errorClass: 'TRANSIENT_RECORD', errorCode: '40001', errorMessage: 'm' }) };

  it('the DLQ record points at the ORIGIN, names the failed tier, keeps original headers, drops retry.*', () => {
    const m = toDlqMessage({ raw, errorClass: 'TRANSIENT_RECORD', errorCode: '40001', errorMessage: 'm', attempts: 3, origin, failedTopic: 'r3' }, { sourceTopic: 'events.raw', groupId: 'g' }, new Date('2026-11-28T11:00:00Z'));
    expect(m.headers).toMatchObject({
      request_id: 'r',
      [DLQ_HEADERS.originalTopic]: 'events.raw',
      [DLQ_HEADERS.originalPartition]: '4',
      [DLQ_HEADERS.originalOffset]: '77',
      [DLQ_HEADERS.failedTopic]: 'r3',
      [DLQ_HEADERS.attempts]: '3',
      [DLQ_HEADERS.firstFailedAt]: origin.firstFailedAt,
    });
    expect(Object.keys(m.headers).some((k) => k.startsWith('retry.'))).toBe(false);
    expect(m.headers[RETRY_HEADERS.attempt as keyof typeof m.headers]).toBeUndefined();
  });

  it('dlq_meta columns: origin as the unique source, the DLQ position, a non-UUID event id stored as null', () => {
    const cols = dlqMetaColumns([
      { letter: { raw, errorClass: 'PERMANENT_PARSE', errorCode: 'bad_event_id', errorMessage: 'x', attempts: 0, eventId: 'not-a-uuid' }, origin, failedTopic: 'events.raw', dlqTopic: 'events.dlq', dlqPartition: 2, dlqOffset: '55', failedAt: new Date('2026-11-28T11:00:00Z') },
    ]);
    expect(cols.map((c) => c[0])).toEqual(['events.raw', 4, '77', 'events.raw', 'events.dlq', 2, '55', null, null, 'PERMANENT_PARSE', 'bad_event_id', 'x', 0, origin.firstFailedAt, '2026-11-28T11:00:00.000Z']);
  });

  it('DlqMetaStore.record: rows that already existed are duplicates', async () => {
    const pool = { query: vi.fn(async () => ({ rowCount: 1, rows: [{ id: 1 }] })) };
    const store = new DlqMetaStore(pool as any);
    const p = { letter: { raw, errorClass: 'PERMANENT_PARSE' as const, errorCode: 'c', errorMessage: 'x', attempts: 0 }, origin, failedTopic: 't', dlqTopic: 'd', dlqPartition: 0, dlqOffset: '1', failedAt: new Date() };
    expect(await store.record([p, { ...p, origin: { ...origin, offset: '78' } }])).toEqual({ inserted: 1, duplicates: 1 });
    expect(await store.record([])).toEqual({ inserted: 0, duplicates: 0 });
  });
});
