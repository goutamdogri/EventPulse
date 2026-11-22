// Micro-batcher: collects messages from many concurrent HTTP requests and sends them to Kafka in one
// producer.send() call, then resolves (or rejects) every waiting request with that batch's outcome.
//
// Deliberate design decisions (S2-F1, see docs/plans/stage-2-server-build-plan.md, step 1):
//   - Flush when the batch is full by count (maxMessages) or bytes (maxBytes), or when the oldest queued
//     message has waited maxWaitMs. The byte cap keeps a batch under the broker's message.max.bytes.
//   - Bounded queue (maxQueue). When full, enqueue() rejects at once with OverloadedError: no promise is kept,
//     the request body can be freed. (Mapping that to 503 is S2-F5.)
//   - At most maxInFlight batches are being sent at the same time. With more than 1, two batches carrying the
//     same key can complete out of order, so per-key ordering across batches is not guaranteed.
//   - One send() = one outcome for every message in it: all resolve or all reject. The batcher never retries
//     and never re-queues (the client's retries + the idempotent producer handle transient errors). A rejected
//     batch may still have been partly written, so a client retry can create a duplicate (Stage 3 problem).
//   - Every message is taken out of the queue before it is sent, so it is in exactly one batch: nothing is sent
//     twice by the batcher and nothing is dropped.
//
// No Nest or Kafka imports here, so it can be unit-tested with a fake sender and fake timers.

export interface BatchMessage {
  key: string;
  value: Buffer;
  headers: Record<string, string>;
}

export interface BatchItem {
  message: BatchMessage;
  /** Size used for the byte caps (key + value + headers). */
  sizeBytes: number;
}

/** Sends one batch. Resolves when Kafka acknowledged every message of it, rejects otherwise. */
export type BatchSender = (messages: BatchMessage[]) => Promise<void>;

export interface MicroBatcherOptions {
  maxMessages: number;
  maxWaitMs: number;
  maxBytes: number;
  maxQueue: number;
  maxInFlight: number;
}

export type FlushReason = 'size' | 'bytes' | 'time' | 'drain';

export interface FlushResult {
  messages: number;
  bytes: number;
  reason: FlushReason;
  durationMs: number;
  /** ms the oldest message of the batch waited in the queue before the send started. */
  oldestWaitMs: number;
  error?: unknown;
}

/** Optional hooks, used by the metrics layer (S2-F6). */
export interface MicroBatcherObserver {
  onFlush?(r: FlushResult): void;
  onReject?(reason: 'overloaded' | 'too_large' | 'closed'): void;
}

export class OverloadedError extends Error {
  readonly name = 'OverloadedError';
  constructor() {
    super('Ingest queue is full');
  }
}

export class MessageTooLargeError extends Error {
  readonly name = 'MessageTooLargeError';
  constructor(size: number, max: number) {
    super(`Message of ${size} bytes is larger than the batch byte limit (${max})`);
  }
}

export class BatcherClosedError extends Error {
  readonly name = 'BatcherClosedError';
  constructor() {
    super('Ingest is shutting down');
  }
}

interface Pending {
  item: BatchItem;
  enqueuedAt: number;
  resolve: () => void;
  reject: (e: unknown) => void;
}

export class MicroBatcher {
  private queue: Pending[] = [];
  private queuedBytes = 0;
  private inFlight = 0;
  private timer?: NodeJS.Timeout;
  private closed = false;
  private idleWaiters: (() => void)[] = [];

  constructor(
    private readonly send: BatchSender,
    private readonly opts: MicroBatcherOptions,
    private readonly observer: MicroBatcherObserver = {},
  ) {
    const { maxMessages, maxWaitMs, maxBytes, maxQueue, maxInFlight } = opts;
    if (maxMessages < 1 || maxBytes < 1 || maxQueue < 1 || maxInFlight < 1 || maxWaitMs < 0) {
      throw new Error(`Invalid MicroBatcher options: ${JSON.stringify(opts)}`);
    }
  }

  enqueue(item: BatchItem): Promise<void> {
    if (this.closed) {
      this.observer.onReject?.('closed');
      return Promise.reject(new BatcherClosedError());
    }
    if (item.sizeBytes > this.opts.maxBytes) {
      this.observer.onReject?.('too_large');
      return Promise.reject(new MessageTooLargeError(item.sizeBytes, this.opts.maxBytes));
    }
    if (this.queue.length >= this.opts.maxQueue) {
      this.observer.onReject?.('overloaded');
      return Promise.reject(new OverloadedError());
    }
    return new Promise<void>((resolve, reject) => {
      this.queue.push({ item, enqueuedAt: Date.now(), resolve, reject });
      this.queuedBytes += item.sizeBytes;
      this.pump();
    });
  }

  /**
   * Stop accepting messages, send everything still queued (partial batches included) and wait until every
   * in-flight send has settled. Used on shutdown so queued requests are answered, not dropped.
   */
  async drain(): Promise<void> {
    this.closed = true;
    this.pump();
    if (this.queue.length === 0 && this.inFlight === 0) return;
    await new Promise<void>((resolve) => this.idleWaiters.push(resolve));
  }

  stats() {
    return {
      queueDepth: this.queue.length,
      queuedBytes: this.queuedBytes,
      inFlight: this.inFlight,
      closed: this.closed,
    };
  }

  // ---------------------------------------------------------------------------------------------------------

  /** Why the head of the queue may be sent now, or undefined if it should keep waiting. */
  private readyReason(now: number): FlushReason | undefined {
    if (this.queue.length === 0) return undefined;
    if (this.queue.length >= this.opts.maxMessages) return 'size';
    if (this.queuedBytes >= this.opts.maxBytes) return 'bytes';
    if (this.closed) return 'drain';
    if (now - this.queue[0].enqueuedAt >= this.opts.maxWaitMs) return 'time';
    return undefined;
  }

  private pump(): void {
    const now = Date.now();
    let reason: FlushReason | undefined;
    while (this.inFlight < this.opts.maxInFlight && (reason = this.readyReason(now))) {
      this.startSend(this.takeBatch(), reason, now);
    }
    this.armTimer(now);
    this.notifyIfIdle();
  }

  /** Up to maxMessages and maxBytes, always at least one message. */
  private takeBatch(): Pending[] {
    let n = 0;
    let bytes = 0;
    while (n < this.queue.length && n < this.opts.maxMessages) {
      const size = this.queue[n].item.sizeBytes;
      if (n > 0 && bytes + size > this.opts.maxBytes) break;
      bytes += size;
      n++;
    }
    const batch = this.queue.splice(0, n);
    this.queuedBytes -= bytes;
    return batch;
  }

  private startSend(batch: Pending[], reason: FlushReason, now: number): void {
    this.inFlight++;
    const bytes = batch.reduce((a, p) => a + p.item.sizeBytes, 0);
    const oldestWaitMs = now - batch[0].enqueuedAt;
    const t0 = performance.now();
    const finish = (error?: unknown) => {
      this.inFlight--;
      this.observer.onFlush?.({
        messages: batch.length,
        bytes,
        reason,
        durationMs: performance.now() - t0,
        oldestWaitMs,
        error,
      });
      if (error === undefined) for (const p of batch) p.resolve();
      else for (const p of batch) p.reject(error);
      this.pump(); // a slot is free: send what became ready meanwhile
    };
    let sending: Promise<void>;
    try {
      sending = this.send(batch.map((p) => p.item.message));
    } catch (e) {
      sending = Promise.reject(e); // a sender that throws synchronously must not leak the in-flight slot
    }
    sending.then(
      () => finish(),
      (e: unknown) => finish(e ?? new Error('send failed')),
    );
  }

  /** Wake up when the oldest queued message reaches maxWaitMs (only if it could be sent then). */
  private armTimer(now: number): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    if (this.queue.length === 0 || this.inFlight >= this.opts.maxInFlight || this.closed) return;
    const due = this.queue[0].enqueuedAt + this.opts.maxWaitMs - now;
    this.timer = setTimeout(() => {
      this.timer = undefined;
      this.pump();
    }, Math.max(0, due));
  }

  private notifyIfIdle(): void {
    if (this.queue.length === 0 && this.inFlight === 0 && this.idleWaiters.length > 0) {
      const waiters = this.idleWaiters;
      this.idleWaiters = [];
      for (const w of waiters) w();
    }
  }
}
