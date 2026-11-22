import { encodeEvent } from '@eventpulse/kafka-config';
import { SinkStats } from '../observability/sink-stats.js';
import type { EventRow, RawMessage } from '../sink/parse.js';
import { OffsetCommitError, PoisonBatchError, handleBatch } from './batch-handler.js';
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
function setup(opts: { write?: WriteFn; maxRows?: number; crashAt?: CrashPoint; crashAfterBatches?: number } = {}) {
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
  const ctx = (messages: RawMessage[]) => ({
    messages,
    resolveOffset: vi.fn((o: string) => void calls.push(`resolve:${o}`)),
    commitOffsetsIfNecessary: vi.fn(async () => void calls.push('kafka-commit')),
    heartbeat: vi.fn(async () => void calls.push('heartbeat')),
    isRunning: () => running,
    isStale: () => stale,
  });
  return {
    calls,
    stats,
    kill,
    writeChunk,
    ctx,
    deps: { writer: { writeChunk }, stats, maxRows: opts.maxRows ?? 2, faults, processingDelayMs: 0 },
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

  it('writer rejects: that chunk is never resolved or committed', async () => {
    const s = setup({ write: async () => Promise.reject(new Error('db down')) });
    const c = s.ctx(msgs(2));
    await expect(handleBatch(c, s.deps)).rejects.toThrow('db down');
    expect(c.resolveOffset).not.toHaveBeenCalled();
    expect(c.commitOffsetsIfNecessary).not.toHaveBeenCalled();
  });

  it('3 chunks, writer fails on chunk 2: only chunk 1 is resolved and committed', async () => {
    let i = 0;
    const s = setup({ write: async (rows) => (++i === 2 ? Promise.reject(new Error('boom')) : okWrite(rows)) });
    const ms = msgs(6);
    const c = s.ctx(ms);
    await expect(handleBatch(c, s.deps)).rejects.toThrow('boom');
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

  it('poison in chunk 2: chunk 1 committed, the batch then throws at the poison chunk', async () => {
    const s = setup();
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
