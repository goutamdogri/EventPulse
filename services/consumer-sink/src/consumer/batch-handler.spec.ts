import { encodeEvent } from '@eventpulse/kafka-config';
import { SinkStats } from '../observability/sink-stats.js';
import type { EventRow, RawMessage } from '../sink/parse.js';
import { DbUnavailableError, DlqPublishError, OffsetCommitError, PoisonBatchError, handleBatch } from './batch-handler.js';
import type { RetryPolicy } from './batch-handler.js';
import type { DeadLetter } from '../sink/dlq-publisher.js';
import { FaultInjector } from './fault-injector.js';
import type { CrashPoint } from './fault-injector.js';

let n = 0;
function msg(): RawMessage {
  const e = encodeEvent({
    tenantId: 't',
    event: {
      event_id: `00000000-0000-4000-8000-${String(++n).padStart(12, '0')}`,
      type: 'x',
      occurred_at: '2026-11-12T10:00:00Z',
      schema_version: 1,
      payload: {},
    },
    receivedAt: new Date(),
    requestId: 'r',
    keyMode: 'tenant',
  });
  return { partition: 0, offset: String(n), key: e.key, value: e.value, headers: e.headers };
}
const msgs = (k: number) => Array.from({ length: k }, msg);
const poison = (): RawMessage => ({ ...msg(), value: Buffer.from('{oops') });

type WriteFn = (rows: EventRow[]) => Promise<{ attempted: number; inserted: number; durationMs: number }>;
const okWrite: WriteFn = async (rows) => ({ attempted: rows.length, inserted: rows.length, durationMs: 1 });

/** Records every call (writes, resolves, commits, heartbeats, crashes) in order. */
const pgErr = (code: string, message = `pg error ${code}`) => Object.assign(new Error(message), { code });

function setup(opts: { write?: WriteFn; maxRows?: number; crashAt?: CrashPoint; crashAfterBatches?: number; retry?: Partial<RetryPolicy>; dlqPublish?: (d: DeadLetter[]) => Promise<void> } = {}) {
  const calls: string[] = [];
  const stats = new SinkStats();
  const write = opts.write ?? okWrite;
  const writeChunk = vi.fn(async (rows: EventRow[]) => {
    calls.push(`write:${rows.length}`);
    const r = await write(rows);
    calls.push('db-commit');
    return r;
  });
  const kill = vi.fn(() => void calls.push('SIGKILL'));
  const faults = new FaultInjector({ crashAt: opts.crashAt ?? 'none', crashAfterBatches: opts.crashAfterBatches ?? 1 }, kill);
  let running = true;
  let stale = false;
  const dead: DeadLetter[] = [];
  const dlq = {
    publish: vi.fn(async (letters: DeadLetter[]) => {
      if (opts.dlqPublish) await opts.dlqPublish(letters);
      dead.push(...letters);
      calls.push(`dlq:${letters.length}`);
    }),
  };
  const sleeps: number[] = [];
  const sleep = vi.fn(async (ms: number) => void sleeps.push(ms));
  const retry: RetryPolicy = { dlqEnabled: true, maxRetries: 3, baseMs: 100, maxMs: 1000, transientWindowMs: 10_000, pauseMs: 5000, ...opts.retry };
  let clock = 0;
  const ctx = (messages: RawMessage[]) => ({
    messages,
    resolveOffset: vi.fn((o: string) => void calls.push(`resolve:${o}`)),
    commitOffsetsIfNecessary: vi.fn(async () => void calls.push('kafka-commit')),
    heartbeat: vi.fn(async () => void calls.push('heartbeat')),
    isRunning: () => running,
    isStale: () => stale,
    pausePartition: vi.fn((ms: number) => void calls.push(`pause:${ms}`)),
  });
  return {
    calls,
    stats,
    kill,
    writeChunk,
    ctx,
    dlq,
    dead,
    sleeps,
    deps: {
      writer: { writeChunk },
      stats,
      maxRows: opts.maxRows ?? 2,
      faults,
      processingDelayMs: 0,
      dlq,
      retry,
      // fake clock: every sleep advances it, so the transient window is deterministic
      sleep: async (ms: number) => {
        clock += ms;
        await sleep(ms);
      },
      now: () => clock,
      rand: () => 1, // jitter at its max: delay == cap
    },
    stop: () => (running = false),
    revoke: () => (stale = true),
  };
}

describe('handleBatch: offsets committed only after the DB commit', () => {
  it('per chunk: write -> DB commit -> resolve(last offset of chunk) -> Kafka commit -> heartbeat', async () => {
    const s = setup();
    const ms = msgs(3);
    await handleBatch(s.ctx(ms), s.deps);
    expect(s.calls).toEqual([
      'write:2', 'db-commit', `resolve:${ms[1].offset}`, 'kafka-commit', 'heartbeat',
      'write:1', 'db-commit', `resolve:${ms[2].offset}`, 'kafka-commit', 'heartbeat',
    ]);
    expect(s.stats.snapshot()).toMatchObject({ consumed: 3, inserted: 3, chunks: 2, commits: 2, batches: 1 });
  });

  it('writer rejects (DB down): that chunk is never resolved or committed', async () => {
    const s = setup({ write: async () => Promise.reject(pgErr('ECONNREFUSED')), retry: { transientWindowMs: 0 } });
    const c = s.ctx(msgs(2));
    await expect(handleBatch(c, s.deps)).rejects.toBeInstanceOf(DbUnavailableError);
    expect(c.resolveOffset).not.toHaveBeenCalled();
    expect(c.commitOffsetsIfNecessary).not.toHaveBeenCalled();
  });

  it('3 chunks, writer fails on chunk 2: only chunk 1 is resolved and committed', async () => {
    let i = 0;
    const s = setup({ write: async (rows) => (++i === 2 ? Promise.reject(pgErr('08006')) : okWrite(rows)), retry: { transientWindowMs: 0 } });
    const ms = msgs(6);
    const c = s.ctx(ms);
    await expect(handleBatch(c, s.deps)).rejects.toBeInstanceOf(DbUnavailableError);
    expect(c.resolveOffset.mock.calls).toEqual([[ms[1].offset]]);
    expect(c.commitOffsetsIfNecessary).toHaveBeenCalledTimes(1);
  });

  it('partition revoked after chunk 1: chunk 2 is not written or committed', async () => {
    const s = setup();
    const c = s.ctx(msgs(4));
    c.heartbeat.mockImplementationOnce(async () => void s.revoke());
    await handleBatch(c, s.deps);
    expect(s.writeChunk).toHaveBeenCalledTimes(1);
    expect(c.commitOffsetsIfNecessary).toHaveBeenCalledTimes(1);
    expect(s.stats.stoppedEarly).toBe(1);
  });

  it('partition revoked during the DB write: rows stored, offset NOT resolved or committed', async () => {
    const s = setup({
      write: async (rows) => {
        s.revoke();
        return okWrite(rows);
      },
    });
    const c = s.ctx(msgs(2));
    await handleBatch(c, s.deps);
    expect(s.stats.inserted).toBe(2);
    expect(c.resolveOffset).not.toHaveBeenCalled();
    expect(c.commitOffsetsIfNecessary).not.toHaveBeenCalled();
  });

  it('shutdown (isRunning false) after chunk 1: finishes chunk 1, then stops', async () => {
    const s = setup();
    const c = s.ctx(msgs(4));
    c.heartbeat.mockImplementationOnce(async () => void s.stop());
    await handleBatch(c, s.deps);
    expect(s.writeChunk).toHaveBeenCalledTimes(1);
    expect(c.commitOffsetsIfNecessary).toHaveBeenCalledTimes(1);
  });

  it('Kafka commit fails after a successful write: OffsetCommitError, counted, rows stay stored', async () => {
    const s = setup();
    const c = s.ctx(msgs(2));
    c.commitOffsetsIfNecessary.mockRejectedValueOnce(new Error('REBALANCE_IN_PROGRESS'));
    await expect(handleBatch(c, s.deps)).rejects.toBeInstanceOf(OffsetCommitError);
    expect(s.stats.commitErrors).toBe(1);
    expect(s.stats.inserted).toBe(2); // nothing "un-done"
  });

  it('DLQ off: poison in chunk 2 -> chunk 1 committed, the batch then throws at the poison chunk', async () => {
    const s = setup({ retry: { dlqEnabled: false } });
    const ms = [msg(), msg(), msg(), poison()];
    const c = s.ctx(ms);
    await expect(handleBatch(c, s.deps)).rejects.toBeInstanceOf(PoisonBatchError);
    expect(c.resolveOffset.mock.calls).toEqual([[ms[1].offset]]);
    expect(s.writeChunk).toHaveBeenCalledTimes(1);
  });

  it('counts duplicates absorbed by ON CONFLICT', async () => {
    const s = setup({ write: async (rows) => ({ attempted: rows.length, inserted: rows.length - 1, durationMs: 1 }) });
    await handleBatch(s.ctx(msgs(4)), s.deps);
    expect(s.stats.duplicates).toBe(2);
  });

  it('processing delay runs before the DB write', async () => {
    const s = setup();
    const sleep = vi.fn(async () => void s.calls.push('sleep'));
    await handleBatch(s.ctx(msgs(1)), { ...s.deps, processingDelayMs: 50, sleep });
    expect(sleep).toHaveBeenCalledWith(50);
    expect(s.calls.slice(0, 2)).toEqual(['sleep', 'write:1']);
  });

  it('empty batch is a no-op', async () => {
    const s = setup();
    await handleBatch(s.ctx([]), s.deps);
    expect(s.calls).toEqual([]);
    expect(s.stats.batches).toBe(0);
  });
});

describe('crash points', () => {
  // The fake kill does not stop the code, so we look at what happened BEFORE the SIGKILL.
  const before = (calls: string[]) => calls.slice(0, calls.indexOf('SIGKILL'));

  it('before_parse: killed before anything is written', async () => {
    const s = setup({ crashAt: 'before_parse' });
    await handleBatch(s.ctx(msgs(2)), s.deps);
    expect(before(s.calls)).toEqual([]);
  });

  it('after_parse: killed before the DB write', async () => {
    const s = setup({ crashAt: 'after_parse' });
    await handleBatch(s.ctx(msgs(2)), s.deps);
    expect(before(s.calls)).toEqual([]);
  });

  it('after_db_commit: killed after the DB commit, before the offset is resolved or committed', async () => {
    const s = setup({ crashAt: 'after_db_commit' });
    await handleBatch(s.ctx(msgs(2)), s.deps);
    expect(before(s.calls)).toEqual(['write:2', 'db-commit']);
  });

  it('fires only on the configured batch number, and only once', async () => {
    const s = setup({ crashAt: 'before_parse', crashAfterBatches: 2 });
    await handleBatch(s.ctx(msgs(2)), s.deps);
    expect(s.kill).not.toHaveBeenCalled();
    await handleBatch(s.ctx(msgs(4)), s.deps); // 2 chunks in batch 2: still one kill
    expect(s.kill).toHaveBeenCalledTimes(1);
    await handleBatch(s.ctx(msgs(2)), s.deps);
    expect(s.kill).toHaveBeenCalledTimes(1);
  });

  it('none: never kills', async () => {
    const s = setup();
    await handleBatch(s.ctx(msgs(4)), s.deps);
    expect(s.kill).not.toHaveBeenCalled();
  });
});

describe('S2-F4 poison handling', () => {
  it('1 invalid JSON among 99 valid: 99 inserted, 1 dead-lettered (PERMANENT_PARSE), offset past all 100', async () => {
    const s = setup({ maxRows: 1000 });
    const ms = [...msgs(50), poison(), ...msgs(49)];
    const c = s.ctx(ms);
    await handleBatch(c, s.deps);
    expect(s.stats.inserted).toBe(99);
    expect(s.dead).toHaveLength(1);
    expect(s.dead[0]).toMatchObject({ errorClass: 'PERMANENT_PARSE', errorCode: 'invalid_json', attempts: 1 });
    expect(s.dead[0].raw).toBe(ms[50]); // original bytes, untouched
    expect(c.resolveOffset).toHaveBeenCalledWith(ms[99].offset);
    expect(s.calls.indexOf('dlq:1')).toBeLessThan(s.calls.indexOf(`resolve:${ms[99].offset}`)); // DLQ ack before commit
    expect(s.stats.snapshot()).toMatchObject({ dlq: 1, dlq_by_class: { PERMANENT_PARSE: 1 } });
  });

  it('several poison records in one chunk: all dead-lettered, good ones stored', async () => {
    const s = setup({ maxRows: 1000 });
    await handleBatch(s.ctx([poison(), ...msgs(3), poison(), poison()]), s.deps);
    expect(s.dead).toHaveLength(3);
    expect(s.stats.inserted).toBe(3);
  });

  it('batch insert throws 22001: bisection isolates the bad row, good rows inserted, bad row retried maxRetries times then DLQ', async () => {
    const ms = msgs(8);
    const badId = JSON.parse(ms[5].value!.toString()).event_id;
    const s = setup({
      maxRows: 1000,
      write: async (rows) => {
        if (rows.some((r) => r.event_id === badId)) throw pgErr('22001', 'value too long for type character varying(100)');
        return okWrite(rows);
      },
    });
    const c = s.ctx(ms);
    await handleBatch(c, s.deps);
    expect(s.stats.inserted).toBe(7);
    expect(s.dead).toHaveLength(1);
    expect(s.dead[0]).toMatchObject({ errorClass: 'PERMANENT_DB', errorCode: '22001', attempts: 4 });
    expect(s.dead[0].raw).toBe(ms[5]);
    expect(s.stats.retriesByClass.PERMANENT_DB).toBe(3);
    expect(s.sleeps).toEqual([100, 200, 400]); // backoff between the retries of the bad row
    expect(c.resolveOffset).toHaveBeenCalledWith(ms[7].offset);
    expect(s.stats.isolations).toBe(1);
  });

  it('a row that fails once then succeeds on retry is stored, not dead-lettered', async () => {
    let failed = false;
    const s = setup({
      maxRows: 1,
      write: async (rows) => {
        if (!failed) {
          failed = true;
          throw pgErr('23514', 'check constraint');
        }
        return okWrite(rows);
      },
    });
    await handleBatch(s.ctx(msgs(1)), s.deps);
    expect(s.dead).toHaveLength(0);
    expect(s.stats.inserted).toBe(1);
  });

  it('ECONNREFUSED 5 times then success: no DLQ, growing backoff, committed only after success', async () => {
    let n = 0;
    const s = setup({ write: async (rows) => (++n <= 5 ? Promise.reject(pgErr('ECONNREFUSED')) : okWrite(rows)) });
    const c = s.ctx(msgs(2));
    await handleBatch(c, s.deps);
    expect(s.dlq.publish).not.toHaveBeenCalled();
    expect(s.sleeps).toEqual([100, 200, 400, 800, 1000]); // doubled, capped at maxMs
    expect(s.stats.retriesByClass.TRANSIENT_DB).toBe(5);
    expect(c.commitOffsetsIfNecessary).toHaveBeenCalledTimes(1);
    expect(s.calls.filter((x) => x.startsWith('resolve')).length).toBe(1);
  });

  it('DB down past the retry window: partition paused, DbUnavailableError, nothing resolved, nothing dead-lettered', async () => {
    const s = setup({ write: async () => Promise.reject(pgErr('57P01', 'terminating connection due to administrator command')), retry: { transientWindowMs: 1000 } });
    const c = s.ctx(msgs(2));
    await expect(handleBatch(c, s.deps)).rejects.toBeInstanceOf(DbUnavailableError);
    expect(c.pausePartition).toHaveBeenCalledWith(5000);
    expect(s.stats.pauses).toBe(1);
    expect(c.resolveOffset).not.toHaveBeenCalled();
    expect(s.dlq.publish).not.toHaveBeenCalled();
    expect(s.sleeps.reduce((a, b) => a + b, 0)).toBeLessThanOrEqual(1000);
  });

  it('DB going down DURING isolation: the whole chunk is retried, nothing dead-lettered', async () => {
    const ms = msgs(4);
    const badId = JSON.parse(ms[1].value!.toString()).event_id;
    let calls = 0;
    const s = setup({
      maxRows: 1000,
      retry: { transientWindowMs: 0 },
      write: async (rows) => {
        calls++;
        if (calls === 1 && rows.some((r) => r.event_id === badId)) throw pgErr('22P02');
        throw pgErr('ECONNRESET');
      },
    });
    await expect(handleBatch(s.ctx(ms), s.deps)).rejects.toBeInstanceOf(DbUnavailableError);
    expect(s.dlq.publish).not.toHaveBeenCalled();
  });

  it('schema broken (42703 undefined column): never dead-lettered, treated as blocking', async () => {
    const s = setup({ write: async () => Promise.reject(pgErr('42703', 'column "x" does not exist')), retry: { transientWindowMs: 0 } });
    await expect(handleBatch(s.ctx(msgs(2)), s.deps)).rejects.toBeInstanceOf(DbUnavailableError);
    expect(s.dlq.publish).not.toHaveBeenCalled();
    expect(s.stats.errorsByClass.SYSTEMIC_DB).toBe(1);
  });

  it('DLQ publish rejects: offset not resolved, not committed; rows already stored stay stored', async () => {
    const s = setup({ maxRows: 1000, dlqPublish: async () => Promise.reject(new Error('NOT_ENOUGH_REPLICAS')) });
    const c = s.ctx([...msgs(3), poison()]);
    await expect(handleBatch(c, s.deps)).rejects.toBeInstanceOf(DlqPublishError);
    expect(c.resolveOffset).not.toHaveBeenCalled();
    expect(c.commitOffsetsIfNecessary).not.toHaveBeenCalled();
    expect(s.stats.inserted).toBe(3);
    expect(s.stats.dlqErrors).toBe(1);
  });

  it('DLQ off: a row Postgres rejects blocks (the error propagates, nothing committed)', async () => {
    const s = setup({ write: async () => Promise.reject(pgErr('22P05')), retry: { dlqEnabled: false } });
    const c = s.ctx(msgs(2));
    await expect(handleBatch(c, s.deps)).rejects.toMatchObject({ code: '22P05' });
    expect(c.resolveOffset).not.toHaveBeenCalled();
    expect(s.dlq.publish).not.toHaveBeenCalled();
  });
});
