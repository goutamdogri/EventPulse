// What the retry-tier consumer does with one batch of one partition of events.retry.N (S3-F5). Free of Kafka
// and Nest types, like the main batch handler.
//
//   for each record, in order:
//     not_before still in the future -> commit what is done, PAUSE this partition until not_before and stop
//                                       (NotDueError: the client seeks back to this record; nothing sleeps in the
//                                        handler, other partitions keep going). Each tier has one fixed delay, so a
//                                        tier is in not_before order up to the jitter: the head waits, nobody else.
//     due -> the same parse + write as the main consumer (the (tenant_id, event_id) key absorbs duplicates)
//       stored / duplicate                          -> done
//       blocking class (DB / registry down)         -> pause the partition, retry this record later (never DLQ)
//       retriable, tier < RETRY_MAX_ATTEMPTS        -> next tier (not_before = now + that tier's delay)
//       retriable on the last tier, or permanent    -> DLQ with attempts = tiers tried, origin = where it was FIRST
//                                                       consumed (events.raw), failed_topic = this tier
//     settle (retry record / dead letter acked, dlq_meta), then resolve + commit this record's offset

import type { FaultInjector } from '@eventpulse/fault-injector';
import { RegistryUnavailableError } from '@eventpulse/schema';
import type { EventSerde } from '@eventpulse/schema';
import { DEFAULT_CLASSIFIER, classifyParseCode, errorCode, isBlocking, parseRetryHeaders, route } from '@eventpulse/retry';
import type { Classifier, ErrorClass, Origin, TierPolicy, TokenBucket } from '@eventpulse/retry';
import type { SinkStats } from '../observability/sink-stats.js';
import type { BatchWriter } from '../sink/batch-writer.js';
import type { DeadLetter, DlqRecorder, DlqSink } from '../sink/dlq-publisher.js';
import { DESERIALIZE_CODES, parseMessage } from '../sink/parse.js';
import type { RawMessage } from '../sink/parse.js';
import { DbUnavailableError, RegistryUnavailableChunkError } from '../consumer/batch-handler.js';
import type { BatchContext } from '../consumer/batch-handler.js';
import { settle } from '../consumer/settle.js';
import type { RetryLetter, RetrySink } from './retry-publisher.js';

/** Not an error: the head record of this partition is not due yet; the partition is paused until it is. */
export class NotDueError extends Error {
  readonly name = 'NotDueError';
  constructor(readonly waitMs: number) {
    super(`retry record not due for ${waitMs} ms: partition paused`);
  }
}

export interface RetryHandlerDeps {
  /** 1-based tier of the topic this batch came from. */
  tier: number;
  topic: string;
  policy: TierPolicy;
  writer: Pick<BatchWriter, 'writeChunk'>;
  stats: SinkStats;
  dlq: DlqSink;
  dlqMeta?: DlqRecorder;
  retryOut: RetrySink;
  limiter?: Pick<TokenBucket, 'tryTake'>;
  classifier?: Pick<Classifier, 'classify'>;
  schema?: { serde: EventSerde; acceptJson: boolean };
  faults: Pick<FaultInjector, 'hit'>;
  pauseMs: number;
  now?: () => number;
  rand?: () => number;
  commit(ctx: BatchContext): Promise<void>;
}

export async function handleRetryBatch(ctx: BatchContext, deps: RetryHandlerDeps): Promise<void> {
  const now = deps.now ?? Date.now;
  const classifier = deps.classifier ?? DEFAULT_CLASSIFIER;
  for (const m of ctx.messages) {
    if (!ctx.isRunning() || ctx.isStale()) return;
    const state = parseRetryHeaders(m.headers);
    const t = now();
    if (state.notBefore > t) {
      deps.stats.retryWaits++;
      ctx.pausePartition(state.notBefore - t);
      throw new NotDueError(state.notBefore - t);
    }
    const origin: Origin = state.origin ?? { topic: deps.topic, partition: m.partition, offset: m.offset, firstFailedAt: new Date(t).toISOString() };
    const outcome = await processOne(m, state.attempt, origin, ctx, deps, classifier, t);
    await settle(outcome.retries, outcome.dead, (ms) => ctx.pausePartition(ms), {
      stats: deps.stats,
      dlq: deps.dlq,
      dlqMeta: deps.dlqMeta,
      retryOut: deps.retryOut,
      limiter: deps.limiter,
      classifier,
      faults: deps.faults,
      pauseMs: deps.pauseMs,
    });
    deps.stats.completeChunk({
      consumed: 1,
      attempted: outcome.attempted,
      inserted: outcome.inserted,
      dlq: outcome.dead,
      retried: outcome.retries.length,
      // A duplicate here is a record already stored by someone else (a replay, a redelivery): never reprocessing.
      causes: { reprocessed: 0, redelivered: outcome.attempted - outcome.inserted, unknown: 0 },
    });
    deps.stats.countRetryProcessed(deps.tier, outcome.result, (t - state.notBefore) / 1000);
    if (ctx.isStale()) return;
    ctx.resolveOffset(m.offset);
    await deps.commit(ctx);
  }
}

interface Outcome {
  result: 'stored' | 'duplicate' | 'next_tier' | 'dlq';
  attempted: number;
  inserted: number;
  retries: RetryLetter[];
  dead: DeadLetter[];
}

async function processOne(
  m: RawMessage,
  attempt: number,
  origin: Origin,
  ctx: BatchContext,
  deps: RetryHandlerDeps,
  classifier: Pick<Classifier, 'classify'>,
  t: number,
): Promise<Outcome> {
  const dead = (errorClass: ErrorClass, code: string, message: string, row?: { tenant_id: string; event_id: string }): Outcome => ({
    result: 'dlq',
    attempted: 0,
    inserted: 0,
    retries: [],
    dead: [{ raw: m, errorClass, errorCode: code, errorMessage: message, attempts: attempt, origin, failedTopic: deps.topic, tenantId: row?.tenant_id, eventId: row?.event_id }],
  });

  if (deps.schema) {
    try {
      await deps.schema.serde.prefetch([m.value]);
    } catch (e) {
      if (!(e instanceof RegistryUnavailableError)) throw e;
      deps.stats.registryUnavailable++;
      ctx.pausePartition(deps.pauseMs);
      deps.stats.pauses++;
      throw new RegistryUnavailableChunkError(e, deps.pauseMs);
    }
  }
  const parsed = parseMessage(m, { serde: deps.schema?.serde, acceptJson: deps.schema?.acceptJson ?? true });
  if (!parsed.ok) {
    if (DESERIALIZE_CODES.includes(parsed.error.code)) deps.stats.countDeserializeFailure(parsed.error.code);
    const cls = classifyParseCode(parsed.error.code);
    deps.stats.countError(cls);
    return dead(cls, parsed.error.code, parsed.error.message);
  }
  const row = parsed.row;
  try {
    const w = await deps.writer.writeChunk([row]);
    return { result: w.inserted === 1 ? 'stored' : 'duplicate', attempted: w.attempted, inserted: w.inserted, retries: [], dead: [] };
  } catch (e) {
    const cls = classifier.classify(e);
    deps.stats.countError(cls);
    if (isBlocking(cls)) {
      ctx.pausePartition(deps.pauseMs);
      deps.stats.pauses++;
      throw new DbUnavailableError(cls, e, deps.pauseMs);
    }
    const r = route(attempt, cls, deps.policy, t, deps.rand);
    const failure = { errorClass: cls, errorCode: errorCode(e), errorMessage: String((e as Error)?.message ?? e) };
    if (r.kind === 'retry') return { result: 'next_tier', attempted: 0, inserted: 0, retries: [{ raw: m, route: r, origin, failure }], dead: [] };
    return dead(cls, failure.errorCode, failure.errorMessage, row);
  }
}
