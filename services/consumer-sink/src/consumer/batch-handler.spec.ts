import { encodeEvent } from '@eventpulse/kafka-config';
import { SinkStats } from '../observability/sink-stats.js';
import type { EventRow, RawMessage } from '../sink/parse.js';
import { DbUnavailableError, DlqMetaError, DlqPublishError, OffsetCommitError, PoisonBatchError, RegistryUnavailableChunkError, RetryRateLimitedError, handleBatch } from './batch-handler.js';
import { EventSerde, RegistryClient, encodeWire } from '@eventpulse/schema';
import type { RetryPolicy } from './batch-handler.js';
import type { DeadLetter } from '../sink/dlq-publisher.js';
import { FaultInjector, loadFaultConfig } from '@eventpulse/fault-injector';

type CrashPoint = 'none' | 'before_parse' | 'after_parse' | 'after_db_commit';

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

function setup(opts: { write?: WriteFn; maxRows?: number; crashAt?: CrashPoint; crashAfterBatches?: number; faults?: string; retry?: Partial<RetryPolicy>; dlqPublish?: (d: DeadLetter[]) => Promise<void> } = {}) {
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
  // Stage 2 env form (CRASH_AT + CRASH_AFTER_BATCHES) or the Stage 3 form (FAULTS=<point>@N).
  const faults = new FaultInjector(
    loadFaultConfig({ DEBUG_FAULTS: 'true', CRASH_AT: opts.crashAt ?? 'none', CRASH_AFTER_BATCHES: String(opts.crashAfterBatches ?? 1), FAULTS: opts.faults }),
    kill,
    () => undefined,
  );
  let running = true;
  let stale = false;
  const dead: DeadLetter[] = [];
  const dlq = {
    publish: vi.fn(async (letters: DeadLetter[]) => {
      if (opts.dlqPublish) await opts.dlqPublish(letters);
      dead.push(...letters);
      calls.push(`dlq:${letters.length}`);
      return letters.map((letter, i) => ({ letter, origin: { topic: 't', partition: letter.raw.partition, offset: letter.raw.offset, firstFailedAt: 'x' }, failedTopic: 't', dlqTopic: 'dlq', dlqPartition: 0, dlqOffset: String(i), failedAt: new Date() }));
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
    expect(await s.stats.snapshot()).toMatchObject({ consumed: 3, inserted: 3, chunks: 2, commits: 2, batches: 1 });
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

  it('Kafka commit fails on every attempt: OffsetCommitError after 1 + 3 attempts, counted, rows stay stored', async () => {
    const s = setup();
    const c = s.ctx(msgs(2));
    c.commitOffsetsIfNecessary.mockRejectedValue(new Error('REBALANCE_IN_PROGRESS'));
    await expect(handleBatch(c, s.deps)).rejects.toBeInstanceOf(OffsetCommitError);
    expect(c.commitOffsetsIfNecessary).toHaveBeenCalledTimes(4);
    expect(s.stats.commitErrors).toBe(4);
    expect(s.stats.inserted).toBe(2); // nothing "un-done"
  });

  it('Kafka commit fails twice then succeeds: retried with backoff, no error, chunk committed', async () => {
    const s = setup();
    const c = s.ctx(msgs(2));
    c.commitOffsetsIfNecessary
      .mockRejectedValueOnce(new Error('COORDINATOR_LOAD_IN_PROGRESS'))
      .mockRejectedValueOnce(new Error('COORDINATOR_LOAD_IN_PROGRESS'));
    await handleBatch(c, s.deps);
    expect(c.commitOffsetsIfNecessary).toHaveBeenCalledTimes(3);
    expect(s.sleeps).toEqual([100, 200]);
    expect(s.stats).toMatchObject({ commitErrors: 2, commits: 1 });
  });

  it('commit retry stops at once if the partition was revoked meanwhile', async () => {
    const s = setup();
    const c = s.ctx(msgs(2));
    c.commitOffsetsIfNecessary.mockImplementation(async () => {
      s.revoke();
      throw new Error('ILLEGAL_GENERATION');
    });
    await expect(handleBatch(c, s.deps)).rejects.toBeInstanceOf(OffsetCommitError);
    expect(c.commitOffsetsIfNecessary).toHaveBeenCalledTimes(1);
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

  it('delay window: skips the first chunks, delays the next ones, then stops', async () => {
    const s = setup();
    const sleep = vi.fn(async () => undefined);
    const delayWindow = { skip: 1, left: 2 };
    for (let i = 0; i < 5; i++) await handleBatch(s.ctx(msgs(1)), { ...s.deps, processingDelayMs: 50, sleep, delayWindow });
    expect(sleep).toHaveBeenCalledTimes(2); // chunk 1 skipped, chunks 2 and 3 delayed, chunks 4 and 5 not
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

  it('consumer:after-offset-commit: killed only after the offset commit', async () => {
    const s = setup({ faults: 'consumer:after-offset-commit' });
    await handleBatch(s.ctx(msgs(2)), s.deps);
    const b = before(s.calls);
    expect(b.slice(0, 2)).toEqual(['write:2', 'db-commit']);
    expect(b.at(-1)).toBe('kafka-commit');
  });

  it('FAULTS=<point>@N counts hits (chunks), not batches', async () => {
    const s = setup({ faults: 'consumer:after-db-commit@2' });
    await handleBatch(s.ctx(msgs(4)), s.deps); // maxRows 2 -> 2 chunks in one batch
    expect(s.kill).toHaveBeenCalledTimes(1);
    expect(before(s.calls).filter((c) => c === 'db-commit')).toHaveLength(2);
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
    expect(s.dead[0]).toMatchObject({ errorClass: 'PERMANENT_PARSE', errorCode: 'invalid_json', attempts: 0 }); // no retry tier tried
    expect(s.dead[0].raw).toBe(ms[50]); // original bytes, untouched
    expect(c.resolveOffset).toHaveBeenCalledWith(ms[99].offset);
    expect(s.calls.indexOf('dlq:1')).toBeLessThan(s.calls.indexOf(`resolve:${ms[99].offset}`)); // DLQ ack before commit
    expect(await s.stats.snapshot()).toMatchObject({ dlq: 1, dlq_by_class: { PERMANENT_PARSE: 1 } });
  });

  it('several poison records in one chunk: all dead-lettered, good ones stored', async () => {
    const s = setup({ maxRows: 1000 });
    await handleBatch(s.ctx([poison(), ...msgs(3), poison(), poison()]), s.deps);
    expect(s.dead).toHaveLength(3);
    expect(s.stats.errorsByClass).toEqual({ PERMANENT_PARSE: 3 });
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
    // One bad record = one error, however many sub-writes failed during the bisection (8 -> 4 -> 2 -> 1).
    expect(s.stats.errorsByClass).toEqual({ PERMANENT_DB: 1 });
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
    expect(s.stats.retriesByClass.TRANSIENT_SYSTEMIC).toBe(5);
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
    expect(s.stats.errorsByClass.SYSTEMIC_CONFIG).toBe(1);
  });

  it('DLQ publish rejects: offset not resolved, not committed; the rows were written but the chunk is not "complete"', async () => {
    const s = setup({ maxRows: 1000, dlqPublish: async () => Promise.reject(new Error('NOT_ENOUGH_REPLICAS')) });
    const c = s.ctx([...msgs(3), poison()]);
    await expect(handleBatch(c, s.deps)).rejects.toBeInstanceOf(DlqPublishError);
    expect(c.resolveOffset).not.toHaveBeenCalled();
    expect(c.commitOffsetsIfNecessary).not.toHaveBeenCalled();
    expect(s.writeChunk).toHaveBeenCalledTimes(1); // the 3 good rows WERE written to Postgres
    // S2-F6 counting rule: only completed chunks count; this attempt shows up as a chunk failure, and its rows
    // will appear as duplicates when the retry completes.
    expect(s.stats).toMatchObject({ consumed: 0, inserted: 0, dlq: 0, dlqErrors: 1, chunkFailures: { dlq_publish: 1 } });
  });

  it('DLQ off: a row Postgres rejects blocks (the error propagates, nothing committed)', async () => {
    const s = setup({ write: async () => Promise.reject(pgErr('22P05')), retry: { dlqEnabled: false } });
    const c = s.ctx(msgs(2));
    await expect(handleBatch(c, s.deps)).rejects.toMatchObject({ code: '22P05' });
    expect(c.resolveOffset).not.toHaveBeenCalled();
    expect(s.dlq.publish).not.toHaveBeenCalled();
  });
});

describe('S3-F3 duplicates by cause', () => {
  /** A writer that reports every row whose event_id is in `stored` as a duplicate (like ON CONFLICT DO NOTHING). */
  function dedupWriter(stored: Set<string>) {
    return async (rows: EventRow[]) => {
      const dups = rows.filter((r) => stored.has(r.event_id));
      rows.forEach((r) => stored.add(r.event_id));
      return {
        attempted: rows.length,
        inserted: rows.length - dups.length,
        durationMs: 1,
        duplicateKeys: dups.map((r) => ({ tenant_id: r.tenant_id, event_id: r.event_id, partition: r.partition, offset: r.offset })),
      };
    };
  }
  const fakeProgress = (max: bigint | null | undefined) => {
    const advanced: string[] = [];
    return {
      advanced,
      known: vi.fn(async () => max),
      advance: vi.fn((_p: number, o: string) => void advanced.push(o)),
      forget: vi.fn(),
    };
  };

  it('a re-read chunk below the stored max is reprocessed; a new offset with a stored key is redelivered', async () => {
    const ms = msgs(4); // offsets n+1..n+4
    const stored = new Set<string>();
    const s = setup({ write: dedupWriter(stored), maxRows: 10 });
    await handleBatch(s.ctx(ms), { ...s.deps, progress: fakeProgress(undefined) }); // first time: all new
    // Same 4 records again (offset reset): their offsets are at or below the stored max -> reprocessed.
    const maxOffset = BigInt(ms[3].offset);
    const lines: string[] = [];
    await handleBatch(s.ctx(ms), { ...s.deps, progress: fakeProgress(maxOffset), log: (l) => lines.push(l) });
    expect(s.stats.duplicatesByCause).toMatchObject({ reprocessed: 4, redelivered: 0 });
    expect(s.stats.reprocessedMessages).toBe(4);
    // A client retry: the same event at a NEW offset -> redelivered.
    const retry = { ...ms[0], offset: String(Number(ms[3].offset) + 1) };
    await handleBatch(s.ctx([retry]), { ...s.deps, progress: fakeProgress(maxOffset) });
    expect(s.stats.duplicatesByCause).toMatchObject({ reprocessed: 4, redelivered: 1, unknown: 0 });
    expect(s.stats.duplicates).toBe(5); // the causes add up to attempted - inserted
    expect(JSON.parse(lines[0])).toMatchObject({ msg: 'duplicates', duplicates: 4, reprocessed: 4, redelivered: 0, reprocessed_messages: 4 });
  });

  it('advances the progress to the highest written offset, and asks only once per chunk', async () => {
    const ms = msgs(3);
    const s = setup({ maxRows: 10 });
    const p = fakeProgress(undefined);
    await handleBatch(s.ctx(ms), { ...s.deps, progress: p });
    expect(p.known).toHaveBeenCalledTimes(1);
    expect(p.advanced).toEqual([ms[2].offset]);
  });

  it('an unknown stored max counts the duplicates as unknown', async () => {
    const ms = msgs(2);
    const s = setup({ write: dedupWriter(new Set(ms.map((m) => JSON.parse(m.value!.toString()).event_id))), maxRows: 10 });
    await handleBatch(s.ctx(ms), { ...s.deps, progress: fakeProgress(null) });
    expect(s.stats.duplicatesByCause).toMatchObject({ reprocessed: 0, redelivered: 0, unknown: 2 });
  });

  it('no log line for a chunk without duplicates', async () => {
    const s = setup({ maxRows: 10 });
    const lines: string[] = [];
    await handleBatch(s.ctx(msgs(3)), { ...s.deps, progress: fakeProgress(undefined), log: (l) => lines.push(l) });
    expect(lines).toEqual([]);
  });
});

describe('S3-F7 schema registry unavailable', () => {
  it('a chunk with an Avro record whose writer schema cannot be fetched: pause, retry later, nothing written or dead-lettered', async () => {
    const down = new RegistryClient({ url: 'http://registry', fetch: (async () => { throw new Error('ECONNREFUSED'); }) as any });
    const serde = new EventSerde(down, { subject: 'events.raw-value', schema: '"string"' });
    const s = setup({ maxRows: 10 });
    const avro = { ...msg(), value: encodeWire(77, Buffer.from([2, 0x61])) };
    await expect(handleBatch(s.ctx([msg(), avro]), { ...s.deps, schema: { serde, acceptJson: true } })).rejects.toBeInstanceOf(RegistryUnavailableChunkError);
    expect(s.calls).toEqual(['pause:5000']); // no write, no DLQ, no resolve, no commit
    expect(s.dead).toEqual([]);
    expect(s.stats.registryUnavailable).toBe(1);
    expect(s.stats.chunkFailures.registry_unavailable).toBe(1);
  });

  it('Stage 2 JSON records need no registry at all', async () => {
    const down = new RegistryClient({ url: 'http://registry', fetch: (async () => { throw new Error('ECONNREFUSED'); }) as any });
    const serde = new EventSerde(down, { subject: 'events.raw-value', schema: '"string"' });
    const s = setup({ maxRows: 10 });
    await handleBatch(s.ctx(msgs(3)), { ...s.deps, schema: { serde, acceptJson: true } });
    expect(s.calls).toContain('kafka-commit');
  });
});

describe('S3-F5 retry tiers (main consumer)', () => {
  const policy = { topics: ['r1', 'r2', 'r3'], delaysMs: [5000, 60000, 600000], jitter: 'equal' as const };
  const idOf = (m: RawMessage) => JSON.parse(m.value!.toString()).event_id as string;
  /** Rows whose event_id is in `bad` fail with `code` (every time); the others are stored. */
  const failing = (bad: Set<string>, code: string): WriteFn => async (rows) => {
    if (rows.some((r) => bad.has(r.event_id))) throw pgErr(code);
    return { attempted: rows.length, inserted: rows.length, durationMs: 1 };
  };
  function tiered(o: { write: WriteFn; maxRows?: number; limiterOk?: boolean; dlqMeta?: any }) {
    const s = setup({ write: o.write, maxRows: o.maxRows ?? 10 });
    const published: any[] = [];
    const retryOut = { publish: vi.fn(async (ls: any[]) => void (published.push(...ls), s.calls.push(`retry:${ls.length}`))) };
    const deps = {
      ...s.deps,
      tiers: { policy, inProcessAttempts: 2 },
      retryOut,
      limiter: { tryTake: () => o.limiterOk ?? true },
      sourceTopic: 'events.raw',
      ...(o.dlqMeta ? { dlqMeta: o.dlqMeta } : {}),
    };
    return { ...s, deps, published, retryOut };
  }

  it('a row failing alone with TRANSIENT_RECORD: chunk retried in-process twice, then isolated, the row goes to tier 1; the rest is stored', async () => {
    const ms = msgs(4);
    const t = tiered({ write: failing(new Set([idOf(ms[2])]), '40001') });
    await handleBatch(t.ctx(ms), t.deps);
    expect(t.published).toHaveLength(1);
    expect(t.published[0]).toMatchObject({
      route: { kind: 'retry', tier: 1, topic: 'r1', attempt: 1, notBefore: expect.any(Number) },
      origin: { topic: 'events.raw', partition: 0, offset: ms[2].offset },
      failure: { errorClass: 'TRANSIENT_RECORD', errorCode: '40001' },
    });
    expect(t.calls.indexOf('retry:1')).toBeLessThan(t.calls.indexOf('kafka-commit')); // acked before the offset moves
    expect(t.stats.retried).toBe(1);
    expect(t.stats.consumed).toBe(t.stats.inserted + t.stats.duplicates + t.stats.dlq + t.stats.retried); // the invariant
    expect(t.stats.retriesByClass.TRANSIENT_RECORD).toBe(2); // the two in-process attempts
    expect(t.dead).toEqual([]);
  });

  it('a whole-chunk TRANSIENT_RECORD that goes away in-process: no retry-topic traffic at all', async () => {
    let fails = 2;
    const t = tiered({ write: async (rows) => { if (fails-- > 0) throw pgErr('40P01'); return { attempted: rows.length, inserted: rows.length, durationMs: 1 }; } });
    await handleBatch(t.ctx(msgs(5)), t.deps);
    expect(t.published).toEqual([]);
    expect(t.stats.inserted).toBe(5);
  });

  it('UNKNOWN errors also go to the retry tiers', async () => {
    const ms = msgs(2);
    const t = tiered({ write: async (rows) => { if (rows.some((r) => r.event_id === idOf(ms[0]))) throw new TypeError('weird'); return { attempted: rows.length, inserted: rows.length, durationMs: 1 }; } });
    await handleBatch(t.ctx(ms), t.deps);
    expect(t.published.map((p) => p.failure.errorClass)).toEqual(['UNKNOWN']);
  });

  it('PERMANENT_DB with tiers on: dead-lettered at once (no in-process retries, no retry tier), attempts 0, with tenant and event id', async () => {
    const ms = msgs(3);
    const t = tiered({ write: failing(new Set([idOf(ms[1])]), '23502') });
    await handleBatch(t.ctx(ms), t.deps);
    expect(t.published).toEqual([]);
    expect(t.dead).toHaveLength(1);
    expect(t.dead[0]).toMatchObject({ errorClass: 'PERMANENT_DB', errorCode: '23502', attempts: 0, tenantId: 't', eventId: idOf(ms[1]) });
    expect(t.stats.retriesByClass.PERMANENT_DB).toBeUndefined();
  });

  it('a dependency that is down never goes to a retry topic or the DLQ, tiers or not', async () => {
    const t = tiered({ write: failing(new Set(msgs(0).map(idOf)), '57P01') });
    t.deps.writer.writeChunk = vi.fn(async () => { throw pgErr('ECONNREFUSED'); }) as any;
    await expect(handleBatch(t.ctx(msgs(3)), { ...t.deps, retry: { ...t.deps.retry, transientWindowMs: 0 } })).rejects.toBeInstanceOf(DbUnavailableError);
    expect(t.published).toEqual([]);
    expect(t.dead).toEqual([]);
  });

  it('too many records for the retry tiers (rate limit): the partition is paused instead, nothing published or committed', async () => {
    const ms = msgs(3);
    const t = tiered({ write: failing(new Set(ms.map(idOf)), '40001'), limiterOk: false });
    await expect(handleBatch(t.ctx(ms), t.deps)).rejects.toBeInstanceOf(RetryRateLimitedError);
    expect(t.published).toEqual([]);
    expect(t.calls).toContain('pause:5000');
    expect(t.calls).not.toContain('kafka-commit');
    expect(t.stats.retryRateLimited).toBe(1);
    expect(t.stats.chunkFailures.retry_rate_limited).toBe(1);
  });

  it('dead letters are indexed in dlq_meta before the commit; re-published ones are counted as DLQ duplicates', async () => {
    const record = vi.fn(async (ps: any[]) => ({ inserted: ps.length - 1, duplicates: 1 }));
    const t = tiered({ write: okWrite, dlqMeta: { record } });
    await handleBatch(t.ctx([poison(), poison(), msg()]), t.deps);
    expect(record).toHaveBeenCalledTimes(1);
    expect(record.mock.calls[0][0]).toHaveLength(2);
    expect(t.stats.dlqDuplicates).toBe(1);
  });

  it('dlq_meta down (blocking): pause, nothing committed, the chunk is read again', async () => {
    const record = vi.fn(async () => { throw pgErr('ECONNREFUSED'); });
    const t = tiered({ write: okWrite, dlqMeta: { record } });
    await expect(handleBatch(t.ctx([poison(), msg()]), t.deps)).rejects.toBeInstanceOf(DlqMetaError);
    expect(t.calls).toContain('pause:5000');
    expect(t.calls).not.toContain('kafka-commit');
    expect(t.stats.chunkFailures.db_unavailable).toBe(1);
  });

  it('tiers OFF: TRANSIENT_RECORD blocks the partition like before (Stage 2 behavior)', async () => {
    const s = setup({ write: failing(new Set(), '40001'), maxRows: 10 });
    s.deps.writer.writeChunk = vi.fn(async () => { throw pgErr('40001'); }) as any;
    await expect(handleBatch(s.ctx(msgs(2)), { ...s.deps, retry: { ...s.deps.retry, transientWindowMs: 0 } })).rejects.toBeInstanceOf(DbUnavailableError);
  });
});
