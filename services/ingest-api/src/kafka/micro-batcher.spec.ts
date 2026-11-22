import {
  BatcherClosedError,
  MessageTooLargeError,
  MicroBatcher,
  OverloadedError,
} from './micro-batcher.js';
import type { BatchMessage, FlushResult, MicroBatcherOptions } from './micro-batcher.js';

const opts = (o: Partial<MicroBatcherOptions> = {}): MicroBatcherOptions => ({
  maxMessages: 3,
  maxWaitMs: 10,
  maxBytes: 1000,
  maxQueue: 100,
  maxInFlight: 1,
  ...o,
});

const item = (id: number, sizeBytes = 10) => ({
  message: { key: `k${id}`, value: Buffer.from(String(id)), headers: {} },
  sizeBytes,
});

/** A fake sender whose sends stay pending until the test settles them. */
function controllableSender() {
  const calls: { messages: BatchMessage[]; resolve: () => void; reject: (e: unknown) => void }[] = [];
  const send = vi.fn(
    (messages: BatchMessage[]) =>
      new Promise<void>((resolve, reject) => calls.push({ messages, resolve, reject })),
  );
  const ids = (i: number) => calls[i].messages.map((m) => m.key);
  return { send, calls, ids };
}

const flushMicrotasks = () => new Promise<void>((r) => setImmediate(r));

beforeEach(() => vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] }));
afterEach(() => vi.useRealTimers());

describe('MicroBatcher', () => {
  it('flushes on size: maxMessages items go out in one send, timer cleared', async () => {
    const s = controllableSender();
    const b = new MicroBatcher(s.send, opts());
    const ps = [1, 2, 3].map((i) => b.enqueue(item(i)));

    expect(s.send).toHaveBeenCalledTimes(1);
    expect(s.ids(0)).toEqual(['k1', 'k2', 'k3']);
    expect(vi.getTimerCount()).toBe(0);

    s.calls[0].resolve();
    await expect(Promise.all(ps)).resolves.toBeDefined();
  });

  it('flushes on time: a single item goes out after maxWaitMs', async () => {
    const s = controllableSender();
    const b = new MicroBatcher(s.send, opts());
    const p = b.enqueue(item(1));

    vi.advanceTimersByTime(9);
    expect(s.send).not.toHaveBeenCalled();
    vi.advanceTimersByTime(1);
    expect(s.send).toHaveBeenCalledTimes(1);
    expect(s.ids(0)).toEqual(['k1']);

    s.calls[0].resolve();
    await expect(p).resolves.toBeUndefined();
  });

  it('measures the wait from the oldest message, not the newest', () => {
    const s = controllableSender();
    const b = new MicroBatcher(s.send, opts());
    void b.enqueue(item(1));
    vi.advanceTimersByTime(6);
    void b.enqueue(item(2));
    vi.advanceTimersByTime(4); // 10 ms after the first message
    expect(s.send).toHaveBeenCalledTimes(1);
    expect(s.ids(0)).toEqual(['k1', 'k2']);
  });

  it('flushes on bytes and never builds a batch above maxBytes', async () => {
    const results: FlushResult[] = [];
    const s = controllableSender();
    const b = new MicroBatcher(s.send, opts({ maxMessages: 100, maxBytes: 100, maxInFlight: 10 }), {
      onFlush: (r) => results.push(r),
    });
    for (let i = 1; i <= 5; i++) void b.enqueue(item(i, 40)); // 200 bytes total

    // 40+40 = 80 fits, a 3rd would make 120. The queue reached 120 bytes >= 100, so batch 1 left on "bytes".
    expect(s.ids(0)).toEqual(['k1', 'k2']);
    expect(s.ids(1)).toEqual(['k3', 'k4']);
    expect(s.calls).toHaveLength(2); // k5 alone (40 bytes) waits for its timer
    vi.advanceTimersByTime(10);
    expect(s.ids(2)).toEqual(['k5']);

    s.calls.forEach((c) => c.resolve());
    await flushMicrotasks();
    expect(results.map((r) => r.reason)).toEqual(['bytes', 'bytes', 'time']);
    expect(results.every((r) => r.bytes <= 100)).toBe(true);
  });

  it('rejects a single message larger than maxBytes', async () => {
    const s = controllableSender();
    const b = new MicroBatcher(s.send, opts({ maxBytes: 50 }));
    await expect(b.enqueue(item(1, 51))).rejects.toBeInstanceOf(MessageTooLargeError);
    expect(b.stats().queueDepth).toBe(0);
  });

  it('bounded queue: rejects with OverloadedError when full, queue unchanged', async () => {
    const s = controllableSender();
    const onReject = vi.fn();
    const b = new MicroBatcher(s.send, opts({ maxMessages: 100, maxQueue: 2 }), { onReject });
    void b.enqueue(item(1));
    void b.enqueue(item(2));

    await expect(b.enqueue(item(3))).rejects.toBeInstanceOf(OverloadedError);
    expect(b.stats().queueDepth).toBe(2);
    expect(onReject).toHaveBeenCalledWith('overloaded');
  });

  it('error fan-out: every waiter of the failed batch rejects with that error, other batches unaffected', async () => {
    const s = controllableSender();
    const b = new MicroBatcher(s.send, opts({ maxInFlight: 2 }));
    const batch1 = [1, 2, 3].map((i) => b.enqueue(item(i)));
    const batch2 = [4, 5, 6].map((i) => b.enqueue(item(i)));
    const boom = new Error('broker said no');

    s.calls[0].reject(boom);
    s.calls[1].resolve();

    const r1 = await Promise.allSettled(batch1);
    const r2 = await Promise.allSettled(batch2);
    expect(r1.every((r) => r.status === 'rejected' && r.reason === boom)).toBe(true);
    expect(r2.every((r) => r.status === 'fulfilled')).toBe(true);
  });

  it('partial failure: each message is in exactly one send, nothing is re-sent or dropped', async () => {
    const s = controllableSender();
    const b = new MicroBatcher(s.send, opts());
    const all = Promise.allSettled([1, 2, 3, 4, 5, 6, 7].map((i) => b.enqueue(item(i))));

    s.calls[0].reject(new Error('fail batch 1')); // k1..k3
    await flushMicrotasks(); // slot freed -> batch 2 (k4..k6) is sent
    s.calls[1].resolve();
    await flushMicrotasks();
    vi.advanceTimersByTime(10); // k7 alone, by time
    s.calls[2].resolve();

    const settled = await all;
    const sentKeys = s.calls.flatMap((c) => c.messages.map((m) => m.key));
    expect(sentKeys).toEqual(['k1', 'k2', 'k3', 'k4', 'k5', 'k6', 'k7']); // no duplicates, none missing
    expect(settled.map((r) => r.status)).toEqual([
      'rejected', 'rejected', 'rejected', 'fulfilled', 'fulfilled', 'fulfilled', 'fulfilled',
    ]);
  });

  it('in-flight limit: the next batch is not sent until the current one settles', async () => {
    const s = controllableSender();
    const b = new MicroBatcher(s.send, opts({ maxInFlight: 1 }));
    [1, 2, 3, 4, 5, 6].forEach((i) => void b.enqueue(item(i)));

    expect(s.send).toHaveBeenCalledTimes(1);
    expect(b.stats()).toMatchObject({ queueDepth: 3, inFlight: 1 });

    s.calls[0].resolve();
    await flushMicrotasks();
    expect(s.send).toHaveBeenCalledTimes(2);
    expect(s.ids(1)).toEqual(['k4', 'k5', 'k6']);
  });

  it('a partial batch whose wait expired while the slot was busy goes out as soon as the slot frees', async () => {
    const s = controllableSender();
    const b = new MicroBatcher(s.send, opts({ maxInFlight: 1 }));
    [1, 2, 3].forEach((i) => void b.enqueue(item(i))); // batch 1 in flight
    void b.enqueue(item(4));
    vi.advanceTimersByTime(50); // k4 waited past maxWaitMs, no slot
    expect(s.send).toHaveBeenCalledTimes(1);

    s.calls[0].resolve();
    await flushMicrotasks();
    expect(s.send).toHaveBeenCalledTimes(2);
    expect(s.ids(1)).toEqual(['k4']);
  });

  it('a sender that throws synchronously rejects the batch and frees the slot', async () => {
    let n = 0;
    const send = vi.fn((_m: BatchMessage[]) => {
      if (n++ === 0) throw new Error('sync boom');
      return Promise.resolve();
    });
    const b = new MicroBatcher(send, opts());
    const first = [1, 2, 3].map((i) => b.enqueue(item(i)));
    const second = [4, 5, 6].map((i) => b.enqueue(item(i)));

    expect((await Promise.allSettled(first)).every((r) => r.status === 'rejected')).toBe(true);
    await expect(Promise.all(second)).resolves.toBeDefined();
    expect(b.stats().inFlight).toBe(0);
  });

  it('drain(): sends partial batches at once and resolves only after in-flight sends settle', async () => {
    const s = controllableSender();
    const b = new MicroBatcher(s.send, opts({ maxInFlight: 1 }));
    const ps = [1, 2, 3, 4].map((i) => b.enqueue(item(i)));
    let drained = false;
    const d = b.drain().then(() => (drained = true));

    s.calls[0].resolve();
    await flushMicrotasks();
    expect(s.ids(1)).toEqual(['k4']); // sent on drain without waiting for the timer
    expect(drained).toBe(false);

    s.calls[1].resolve();
    await d;
    expect(drained).toBe(true);
    await expect(Promise.all(ps)).resolves.toBeDefined();
  });

  it('after drain() new messages are rejected', async () => {
    const s = controllableSender();
    const b = new MicroBatcher(s.send, opts());
    await b.drain();
    await expect(b.enqueue(item(1))).rejects.toBeInstanceOf(BatcherClosedError);
  });

  it('reports flush details to the observer', async () => {
    const onFlush = vi.fn();
    const s = controllableSender();
    const b = new MicroBatcher(s.send, opts(), { onFlush });
    [1, 2, 3].forEach((i) => void b.enqueue(item(i, 7)));
    s.calls[0].resolve();
    await flushMicrotasks();
    expect(onFlush).toHaveBeenCalledWith(
      expect.objectContaining({ messages: 3, bytes: 21, reason: 'size', error: undefined }),
    );
  });

  it('rejects invalid options', () => {
    expect(() => new MicroBatcher(vi.fn(), opts({ maxMessages: 0 }))).toThrow();
    expect(() => new MicroBatcher(vi.fn(), opts({ maxInFlight: 0 }))).toThrow();
  });
});
