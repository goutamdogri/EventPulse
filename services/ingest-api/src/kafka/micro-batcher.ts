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
// S2-F5 additions:
//   - The queue is bounded by count (maxQueue) AND bytes (maxQueueBytes); 0 means unbounded (K10 comparison mode).
//   - Every send has a timeout (sendTimeoutMs). A hung broker rejects the whole batch with KafkaUnavailableError.
//     The send may still complete later, so a client retry can create a duplicate (known Stage 2 flaw).
//   - A queued message never waits longer than sendTimeoutMs for its batch to start (then it is rejected the same
//     way), so a request waits at most "queue wait + send timeout".
//   - A circuit breaker (see breaker.ts) turns a known outage into immediate rejections, and rejects what is
//     already queued when it opens, so memory stays flat while Kafka is down.
//   - drain(deadlineMs) gives up at the deadline and rejects whatever is still queued or in flight.
//
// No Nest or Kafka imports here, so it can be unit-tested with a fake sender and fake timers.

import { CircuitBreaker } from './breaker.js';
import { KafkaUnavailableError, OverloadedError } from './errors.js';

export { OverloadedError, KafkaUnavailableError };

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
  /** Max queued messages. 0 = unbounded (also meant to be used with the breaker disabled). */
  maxQueue: number;
  /** Max queued bytes. 0 = unbounded. */
  maxQueueBytes?: number;
  maxInFlight: number;
  /** Per-send timeout. 0 / undefined = none. Also caps how long a message may wait in the queue. */
  sendTimeoutMs?: number;
}

export type FlushReason = 'size' | 'bytes' | 'time' | 'drain';

export interface FlushResult {
  messages: number;
  bytes: number;
  reason: FlushReason;
  durationMs: number;
  /** ms the oldest message of the batch waited in the queue before the send started. */
  oldestWaitMs: number;
  /** ms each message of the batch waited in the queue before the send started (S2-F6 histogram). */
  waitsMs: number[];
  error?: unknown;
}

/** Optional hooks, used by the metrics layer (S2-F6). */
export interface MicroBatcherObserver {
  onFlush?(r: FlushResult): void;
  onReject?(reason: 'overloaded' | 'breaker_open' | 'too_large' | 'closed' | 'queue_timeout'): void;
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
  private readonly inFlightBatches = new Set<Pending[]>();

  constructor(
    private readonly send: BatchSender,
    private readonly opts: MicroBatcherOptions,
    private readonly observer: MicroBatcherObserver = {},
    private readonly breaker: CircuitBreaker = new CircuitBreaker(0, 0),
  ) {
    const { maxMessages, maxWaitMs, maxBytes, maxQueue, maxInFlight } = opts;
    if (maxMessages < 1 || maxBytes < 1 || maxQueue < 0 || maxInFlight < 1 || maxWaitMs < 0) {
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
    const { maxQueue, maxQueueBytes = 0 } = this.opts;
    if (
      (maxQueue > 0 && this.queue.length >= maxQueue) ||
      (maxQueueBytes > 0 && this.queuedBytes + item.sizeBytes > maxQueueBytes)
    ) {
      this.observer.onReject?.('overloaded');
      return Promise.reject(new OverloadedError());
    }
    // Known outage: fail fast instead of queueing (the probe after the cooldown is let through here).
    if (!this.breaker.tryAcquire()) {
      this.observer.onReject?.('breaker_open');
      return Promise.reject(new KafkaUnavailableError('Kafka unavailable (circuit breaker open)'));
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
  async drain(deadlineMs = 0): Promise<void> {
    this.closed = true;
    this.pump();
    if (this.queue.length === 0 && this.inFlight === 0) return;
    let timer: NodeJS.Timeout | undefined;
    const idle = new Promise<void>((resolve) => this.idleWaiters.push(resolve));
    if (deadlineMs > 0) {
      const deadline = new Promise<void>((resolve) => {
        timer = setTimeout(() => {
          // Deadline: answer everybody still waiting (503), then let shutdown continue.
          const err = new KafkaUnavailableError('Ingest shutting down before Kafka acknowledged');
          this.rejectQueued(err);
          for (const batch of this.inFlightBatches) for (const p of batch) p.reject(err);
          resolve();
        }, deadlineMs);
      });
      await Promise.race([idle, deadline]);
      clearTimeout(timer);
    } else {
      await idle;
    }
  }

  stats() {
    return {
      queueDepth: this.queue.length,
      queuedBytes: this.queuedBytes,
      inFlight: this.inFlight,
      closed: this.closed,
      breaker: this.breaker.state(),
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

  /** Rejects messages that waited longer than the send timeout without their batch starting. */
  private expireQueued(now: number): void {
    const limit = this.opts.sendTimeoutMs ?? 0;
    if (limit <= 0) return;
    let n = 0;
    while (n < this.queue.length && now - this.queue[n].enqueuedAt >= limit) n++;
    if (n === 0) return;
    const expired = this.queue.splice(0, n);
    for (const p of expired) this.queuedBytes -= p.item.sizeBytes;
    this.observer.onReject?.('queue_timeout');
    const err = new KafkaUnavailableError('Timed out waiting in the ingest queue');
    for (const p of expired) p.reject(err);
  }

  private rejectQueued(err: unknown): void {
    const all = this.queue.splice(0);
    this.queuedBytes = 0;
    for (const p of all) p.reject(err);
    this.notifyIfIdle();
  }

  private pump(): void {
    const now = Date.now();
    this.expireQueued(now);
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
    this.inFlightBatches.add(batch);
    const bytes = batch.reduce((a, p) => a + p.item.sizeBytes, 0);
    const oldestWaitMs = now - batch[0].enqueuedAt;
    const waitsMs = batch.map((p) => now - p.enqueuedAt);
    const t0 = performance.now();
    let done = false;
    let timer: NodeJS.Timeout | undefined;
    const finish = (error?: unknown) => {
      if (done) return; // the send timeout and a late result can both arrive: first one wins
      done = true;
      if (timer) clearTimeout(timer);
      this.inFlight--;
      this.inFlightBatches.delete(batch);
      this.observer.onFlush?.({
        messages: batch.length,
        bytes,
        reason,
        durationMs: performance.now() - t0,
        oldestWaitMs,
        waitsMs,
        error,
      });
      if (error === undefined) {
        this.breaker.onSuccess();
        for (const p of batch) p.resolve();
      } else {
        // Only "Kafka is unavailable" failures count: a bad message must not open the breaker.
        if (error instanceof KafkaUnavailableError) this.breaker.onFailure();
        else this.breaker.onSuccess(); // the broker answered; release a half-open probe
        for (const p of batch) p.reject(error);
        // Breaker just opened: fail what is queued now instead of sending it one failing batch at a time.
        if (this.breaker.enabled && this.breaker.state() === 'open') {
          this.rejectQueued(new KafkaUnavailableError('Kafka unavailable (circuit breaker open)'));
        }
      }
      this.pump(); // a slot is free: send what became ready meanwhile
    };
    const timeoutMs = this.opts.sendTimeoutMs ?? 0;
    if (timeoutMs > 0) {
      timer = setTimeout(
        () => finish(new KafkaUnavailableError(`Kafka send timed out after ${timeoutMs} ms`)),
        timeoutMs,
      );
    }
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

  /** Wake up when the oldest message must be flushed or expires, whichever is first. */
  private armTimer(now: number): void {
    if (this.timer) {
      clearTimeout(this.timer);
      this.timer = undefined;
    }
    if (this.queue.length === 0) return;
    const head = this.queue[0].enqueuedAt;
    let due = Infinity;
    if (this.inFlight < this.opts.maxInFlight && !this.closed) due = head + this.opts.maxWaitMs - now;
    const expiry = this.opts.sendTimeoutMs ?? 0;
    if (expiry > 0) due = Math.min(due, head + expiry - now);
    if (due === Infinity) return;
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
