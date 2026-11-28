// Retry-tier routing (S3-F5). Pure: given a failed record's class and how many retry tiers it has been through,
// decide where it goes next, and build its headers.
//
//   tiers      events.retry.1 .. events.retry.N with fixed delays (RETRY_TIER_DELAYS_MS), N = RETRY_MAX_ATTEMPTS
//   attempt    number of retry-tier attempts already made: 0 for a failure in the main consumer, k for a failure
//              while processing on tier k
//   route      block class                -> block   (pause the partition; never a retry topic, never the DLQ)
//              dlq / isolate_then_dlq     -> dlq     (attempts = attempt)
//              retry_tier, attempt < N    -> tier attempt+1, not_before = now + jittered(delay of that tier)
//              retry_tier, attempt == N   -> dlq     (attempts exhausted)
//
// A retry record carries the ORIGINAL value bytes and headers plus retry.* headers; the original topic/partition/
// offset and the first failure time survive every tier, so the DLQ record of a thrice-retried event still points
// at where it came from.

import { jittered } from './backoff.js';
import type { Jitter } from './backoff.js';
import { ACTION, truncate } from './classifier.js';
import type { ErrorClass } from './classifier.js';

export const RETRY_HEADERS = {
  attempt: 'retry.attempt',
  notBefore: 'retry.not_before',
  originalTopic: 'retry.original_topic',
  originalPartition: 'retry.original_partition',
  originalOffset: 'retry.original_offset',
  firstFailedAt: 'retry.first_failed_at',
  lastErrorClass: 'retry.last_error_class',
  lastErrorCode: 'retry.last_error_code',
  lastErrorMessage: 'retry.last_error_message',
} as const;

const RETRY_HEADER_NAMES = new Set<string>(Object.values(RETRY_HEADERS));

export interface TierPolicy {
  topics: string[];
  delaysMs: number[];
  jitter: Jitter;
}

export interface Origin {
  topic: string;
  partition: number;
  offset: string;
  /** ISO time of the first failure. */
  firstFailedAt: string;
}

export interface Failure {
  errorClass: ErrorClass;
  errorCode: string;
  errorMessage: string;
}

export type Route =
  | { kind: 'retry'; tier: number; topic: string; notBefore: number; attempt: number }
  | { kind: 'dlq'; attempts: number }
  | { kind: 'block' };

export function validateTierPolicy(p: TierPolicy): void {
  if (p.topics.length === 0) throw new Error('at least one retry tier topic is needed');
  if (p.topics.length !== p.delaysMs.length) throw new Error(`${p.topics.length} retry topics but ${p.delaysMs.length} delays`);
  if (new Set(p.topics).size !== p.topics.length) throw new Error('retry topics must be distinct');
  if (p.delaysMs.some((d) => !Number.isInteger(d) || d < 0)) throw new Error('retry delays must be integers >= 0');
}

export function route(attempt: number, cls: ErrorClass, p: TierPolicy, now: number, rand: () => number = Math.random): Route {
  const action = ACTION[cls];
  if (action === 'block') return { kind: 'block' };
  if (action !== 'retry_tier') return { kind: 'dlq', attempts: attempt };
  if (attempt >= p.topics.length) return { kind: 'dlq', attempts: attempt };
  const tier = attempt + 1;
  return { kind: 'retry', tier, topic: p.topics[tier - 1], notBefore: now + jittered(p.delaysMs[tier - 1], rand, p.jitter), attempt: tier };
}

export type Headers = Record<string, string | Buffer>;

/** The original record's headers without our retry.* ones (so they are never duplicated across tiers). */
export function originalHeaders(h: Record<string, unknown> | undefined): Headers {
  const out: Headers = {};
  for (const [k, v] of Object.entries(h ?? {})) {
    if (RETRY_HEADER_NAMES.has(k)) continue;
    const one = Array.isArray(v) ? v[0] : v;
    if (Buffer.isBuffer(one) || typeof one === 'string') out[k] = one;
    else if (one !== undefined && one !== null) out[k] = String(one);
  }
  return out;
}

export function retryHeaders(original: Record<string, unknown> | undefined, r: Extract<Route, { kind: 'retry' }>, origin: Origin, f: Failure): Headers {
  return {
    ...originalHeaders(original),
    [RETRY_HEADERS.attempt]: String(r.attempt),
    [RETRY_HEADERS.notBefore]: String(r.notBefore),
    [RETRY_HEADERS.originalTopic]: origin.topic,
    [RETRY_HEADERS.originalPartition]: String(origin.partition),
    [RETRY_HEADERS.originalOffset]: origin.offset,
    [RETRY_HEADERS.firstFailedAt]: origin.firstFailedAt,
    [RETRY_HEADERS.lastErrorClass]: f.errorClass,
    [RETRY_HEADERS.lastErrorCode]: f.errorCode,
    [RETRY_HEADERS.lastErrorMessage]: truncate(f.errorMessage, 1024),
  };
}

export interface RetryState {
  attempt: number;
  notBefore: number;
  origin: Origin | null;
}

const header = (h: Record<string, unknown> | undefined, name: string): string | undefined => {
  const v = h?.[name];
  const one = Array.isArray(v) ? v[0] : v;
  if (one === undefined || one === null) return undefined;
  return Buffer.isBuffer(one) ? one.toString('utf8') : String(one);
};

/** Reads the retry.* headers of a record on a retry tier. Missing or broken headers: attempt 1, due now. */
export function parseRetryHeaders(h: Record<string, unknown> | undefined): RetryState {
  const attempt = Number(header(h, RETRY_HEADERS.attempt));
  const notBefore = Number(header(h, RETRY_HEADERS.notBefore));
  const topic = header(h, RETRY_HEADERS.originalTopic);
  const partition = Number(header(h, RETRY_HEADERS.originalPartition));
  const offset = header(h, RETRY_HEADERS.originalOffset);
  const firstFailedAt = header(h, RETRY_HEADERS.firstFailedAt);
  return {
    attempt: Number.isInteger(attempt) && attempt >= 1 ? attempt : 1,
    notBefore: Number.isFinite(notBefore) ? notBefore : 0,
    origin: topic && Number.isInteger(partition) && offset && firstFailedAt ? { topic, partition, offset, firstFailedAt } : null,
  };
}
