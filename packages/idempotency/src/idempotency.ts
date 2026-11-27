// API idempotency (S3-F1): the state machine of one idempotency key. Layer 1 of two: it saves Kafka and sink
// work, but correctness never depends on it (the sink's (tenant_id, event_id) key is the source of truth).
//
//   key       idem:{tenant}:{Idempotency-Key, or the body's event_id}   (tenant from the API key, never the body)
//   value     inflight:<hash>:<owner>:<startedMs>   while the first request is producing   (short TTL)
//             done:<hash>:<event_id>                 after Kafka acknowledged it              (IDEM_TTL_S)
//   hash      sha256 of the canonical event (same function as the sink's content_hash)
//
//   begin()                                                   -> the HTTP layer answers
//     key absent                -> acquire inflight           new          produce, then complete() -> 202
//     same hash, inflight       ->                            in_progress  409 + Retry-After
//     same hash, done           ->                            replay       202 with the original event_id
//     different hash, any state ->                            mismatch     422 (a client bug, reported)
//     key expired               -> it is simply absent        new          (the sink still stops a 2nd row)
//     store failed / guard open -> policy 'open'              bypass       produce without dedup (counted)
//                               -> policy 'closed'            throws IdempotencyUnavailableError -> 503
//   complete(ticket)  inflight -> done (owner only). Never throws: the event IS durable, a failure only means
//                     retries get 409 until the short inflight TTL frees the key.
//   release(ticket)   delete the inflight (owner only) after a failed produce, so the client's retry is new.

import { randomBytes } from 'node:crypto';
import type { IdempotencyStore } from './store.js';

export const IDEMPOTENCY_POLICIES = ['open', 'closed'] as const;
export type IdempotencyPolicy = (typeof IDEMPOTENCY_POLICIES)[number];

export type IdempotencyResult = 'new' | 'replayed' | 'in_progress' | 'payload_mismatch' | 'fail_open' | 'fail_closed' | 'disabled';
export type StoreOp = 'acquire' | 'complete' | 'release';

export interface IdempotencyOptions {
  enabled: boolean;
  /** Lifetime of a completed key (the dedup window). */
  ttlS: number;
  /** Lifetime of an in-progress key: frees a key whose request died, so keep it short. */
  inflightTtlS: number;
  policy: IdempotencyPolicy;
  prefix?: string;
  now?: () => number;
  newOwner?: () => string;
}

/** Shared shape with ingest-api's CircuitBreaker, so a dead store does not cost a timeout per request. */
export interface Guard {
  tryAcquire(): boolean;
  onSuccess(): void;
  onFailure(): void;
}

export interface IdempotencyObserver {
  result?(r: IdempotencyResult): void;
  latency?(op: StoreOp, seconds: number, ok: boolean): void;
  completeFailed?(reason: 'error' | 'lost_ownership'): void;
  storeError?(op: StoreOp, err: unknown): void;
}

export interface Ticket {
  readonly key: string;
  readonly hash: string;
  readonly inflight: string;
}

export type Begin =
  | { kind: 'new'; ticket: Ticket }
  | { kind: 'replay'; eventId: string }
  | { kind: 'in_progress' }
  | { kind: 'mismatch' }
  | { kind: 'bypass'; reason: 'fail_open' | 'disabled' };

export class IdempotencyUnavailableError extends Error {
  readonly name = 'IdempotencyUnavailableError';
  constructor(readonly cause?: unknown) {
    super(`idempotency store unavailable (IDEM_FAILURE_POLICY=closed)${cause ? `: ${String((cause as Error)?.message ?? cause)}` : ''}`);
  }
}

export class InvalidIdempotencyKeyError extends Error {
  readonly name = 'InvalidIdempotencyKeyError';
}

/** 1 to 255 printable ASCII characters, no spaces. */
const KEY_RE = /^[\x21-\x7e]{1,255}$/;

export function validateIdempotencyKey(k: string): string {
  if (!KEY_RE.test(k)) throw new InvalidIdempotencyKeyError('Idempotency-Key must be 1-255 printable ASCII characters without spaces');
  return k;
}

export const storeKey = (prefix: string, tenant: string, key: string) => `${prefix}:${tenant}:${key}`;

export type StoredValue =
  | { state: 'inflight'; hash: string; owner: string; startedMs: number }
  | { state: 'done'; hash: string; eventId: string };

const HASH_RE = /^[0-9a-f]{64}$/;

export function encodeInflight(hash: string, owner: string, startedMs: number) {
  return `inflight:${hash}:${owner}:${startedMs}`;
}
export function encodeDone(hash: string, eventId: string) {
  return `done:${hash}:${eventId}`;
}

/** null = not a value this code wrote (corrupt / foreign): handled like a store failure. */
export function decodeValue(v: string): StoredValue | null {
  const p = v.split(':');
  if (p[0] === 'inflight' && p.length === 4 && HASH_RE.test(p[1]) && p[2].length > 0 && /^\d+$/.test(p[3])) {
    return { state: 'inflight', hash: p[1], owner: p[2], startedMs: Number(p[3]) };
  }
  if (p[0] === 'done' && p.length === 3 && HASH_RE.test(p[1]) && p[2].length > 0) return { state: 'done', hash: p[1], eventId: p[2] };
  return null;
}

class CorruptValueError extends Error {
  readonly name = 'CorruptValueError';
}

export class Idempotency {
  private readonly prefix: string;
  private readonly now: () => number;
  private readonly newOwner: () => string;

  constructor(
    private readonly store: IdempotencyStore,
    readonly opts: IdempotencyOptions,
    private readonly guard?: Guard,
    private readonly obs: IdempotencyObserver = {},
  ) {
    if (opts.inflightTtlS < 1 || opts.ttlS < opts.inflightTtlS) throw new Error('need 1 <= inflightTtlS <= ttlS');
    this.prefix = opts.prefix ?? 'idem';
    this.now = opts.now ?? Date.now;
    this.newOwner = opts.newOwner ?? (() => randomBytes(9).toString('base64url'));
  }

  async begin(tenant: string, key: string, hash: Buffer | string): Promise<Begin> {
    if (!this.opts.enabled) return this.done({ kind: 'bypass', reason: 'disabled' }, 'disabled');
    const h = typeof hash === 'string' ? hash : hash.toString('hex');
    const k = storeKey(this.prefix, tenant, key);
    const inflight = encodeInflight(h, this.newOwner(), this.now());

    if (this.guard && !this.guard.tryAcquire()) return this.unavailable(undefined);
    let current: string | null;
    try {
      const r = await this.timed('acquire', () => this.store.acquire(k, inflight, this.opts.inflightTtlS));
      this.guard?.onSuccess();
      if (r.acquired) return this.done({ kind: 'new', ticket: { key: k, hash: h, inflight } }, 'new');
      current = r.current;
      // Absent after a failed SET NX can only mean it expired in between (not possible inside one Lua script, but
      // a store without atomic acquire could do it): try once more, then treat as a store failure.
      if (current === null) {
        const again = await this.timed('acquire', () => this.store.acquire(k, inflight, this.opts.inflightTtlS));
        if (again.acquired) return this.done({ kind: 'new', ticket: { key: k, hash: h, inflight } }, 'new');
        current = again.current;
      }
      if (current === null || decodeValue(current) === null) throw new CorruptValueError(`unexpected value at ${k}`);
    } catch (e) {
      this.guard?.onFailure();
      this.obs.storeError?.('acquire', e);
      return this.unavailable(e);
    }

    const v = decodeValue(current)!;
    if (v.hash !== h) return this.done({ kind: 'mismatch' }, 'payload_mismatch');
    if (v.state === 'inflight') return this.done({ kind: 'in_progress' }, 'in_progress');
    return this.done({ kind: 'replay', eventId: v.eventId }, 'replayed');
  }

  /** inflight -> done. Never throws (the event is already durable in Kafka). Returns whether it was recorded. */
  async complete(ticket: Ticket, eventId: string): Promise<boolean> {
    try {
      const ok = await this.timed('complete', () => this.store.replace(ticket.key, ticket.inflight, encodeDone(ticket.hash, eventId), this.opts.ttlS));
      this.guard?.onSuccess();
      if (!ok) this.obs.completeFailed?.('lost_ownership'); // our inflight expired (slow produce) or was flushed
      return ok;
    } catch (e) {
      this.guard?.onFailure();
      this.obs.storeError?.('complete', e);
      this.obs.completeFailed?.('error');
      return false;
    }
  }

  /** Delete our inflight after a failed produce, so the client's retry starts fresh. Never throws. */
  async release(ticket: Ticket): Promise<void> {
    try {
      await this.timed('release', () => this.store.remove(ticket.key, ticket.inflight));
      this.guard?.onSuccess();
    } catch (e) {
      // The inflight then expires after inflightTtlS; retries get 409 until then. Nothing is lost.
      this.guard?.onFailure();
      this.obs.storeError?.('release', e);
    }
  }

  private unavailable(e: unknown): Begin {
    if (this.opts.policy === 'open') return this.done({ kind: 'bypass', reason: 'fail_open' }, 'fail_open');
    this.obs.result?.('fail_closed');
    throw new IdempotencyUnavailableError(e);
  }

  private done<T extends Begin>(b: T, r: IdempotencyResult): T {
    this.obs.result?.(r);
    return b;
  }

  private async timed<T>(op: StoreOp, fn: () => Promise<T>): Promise<T> {
    const t0 = performance.now();
    try {
      const r = await fn();
      this.obs.latency?.(op, (performance.now() - t0) / 1000, true);
      return r;
    } catch (e) {
      this.obs.latency?.(op, (performance.now() - t0) / 1000, false);
      throw e;
    }
  }
}
