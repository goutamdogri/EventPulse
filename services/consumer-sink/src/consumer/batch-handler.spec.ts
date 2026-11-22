import { encodeEvent } from '@eventpulse/kafka-config';
import { SinkStats } from '../observability/sink-stats.js';
import type { EventRow, RawMessage } from '../sink/parse.js';
import { PoisonBatchError, handleBatch } from './batch-handler.js';

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

function setup(writeChunk = vi.fn(async (rows: EventRow[]) => ({ attempted: rows.length, inserted: rows.length, durationMs: 1 }))) {
  const calls: string[] = [];
  const stats = new SinkStats();
  const wrapped = vi.fn(async (rows: EventRow[]) => {
    calls.push(`write:${rows.length}`);
    return writeChunk(rows);
  });
  const ctx = (messages: RawMessage[]) => ({
    messages,
    resolveOffset: vi.fn((o: string) => calls.push(`resolve:${o}`)),
    commitOffsetsIfNecessary: vi.fn(async () => void calls.push('commit')),
  });
  return { calls, stats, deps: { writer: { writeChunk: wrapped }, stats, maxRows: 2 }, ctx, writeChunk: wrapped };
}

describe('handleBatch', () => {
  it('writes in chunks of maxRows, then resolves the last offset and commits', async () => {
    const s = setup();
    const ms = [msg(), msg(), msg(), msg(), msg()];
    await handleBatch(s.ctx(ms), s.deps);
    expect(s.calls).toEqual(['write:2', 'write:2', 'write:1', `resolve:${ms[4].offset}`, 'commit']);
    expect(s.stats.snapshot()).toMatchObject({ consumed: 5, attempted: 5, inserted: 5, duplicates: 0, chunks: 3, batches: 1 });
  });

  it('counts duplicates absorbed by ON CONFLICT', async () => {
    const s = setup(vi.fn(async (rows: EventRow[]) => ({ attempted: rows.length, inserted: rows.length - 1, durationMs: 1 })));
    await handleBatch(s.ctx([msg(), msg(), msg(), msg()]), s.deps);
    expect(s.stats.duplicates).toBe(2); // 2 chunks, one duplicate each
  });

  it('poison record: throws, nothing written, nothing committed', async () => {
    const s = setup();
    const bad = { ...msg(), value: Buffer.from('{oops') };
    const c = s.ctx([msg(), bad, msg()]);
    await expect(handleBatch(c, s.deps)).rejects.toBeInstanceOf(PoisonBatchError);
    expect(s.writeChunk).not.toHaveBeenCalled();
    expect(c.resolveOffset).not.toHaveBeenCalled();
    expect(c.commitOffsetsIfNecessary).not.toHaveBeenCalled();
  });

  it('DB failure: no offset resolved or committed (the batch will be retried)', async () => {
    const s = setup(vi.fn(async () => {
      throw new Error('db down');
    }));
    const c = s.ctx([msg(), msg()]);
    await expect(handleBatch(c, s.deps)).rejects.toThrow('db down');
    expect(c.resolveOffset).not.toHaveBeenCalled();
    expect(c.commitOffsetsIfNecessary).not.toHaveBeenCalled();
  });

  it('empty batch is a no-op', async () => {
    const s = setup();
    const c = s.ctx([]);
    await handleBatch(c, s.deps);
    expect(s.calls).toEqual([]);
  });
});
