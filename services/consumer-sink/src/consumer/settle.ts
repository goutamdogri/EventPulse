// The end of a chunk, shared by the main consumer and the retry-tier consumer (S3-F5): everything that must be
// acknowledged BEFORE the source offset may move.
//
//   1. retry records -> their tier topic (rate-limited; acks=all)      crash point retry:after-retry-publish-before-commit
//   2. dead letters  -> events.dlq (acks=all)                           crash point dlq:after-dlq-publish-before-meta
//   3. dead letters  -> dlq_meta (ON CONFLICT DO NOTHING on the source)  crash point dlq:after-meta-before-commit
//   (4. the caller resolves and commits the source offset)
//
// A crash anywhere before 4 makes the chunk be read again: retry/DLQ records may be published twice (at-least-once),
// dlq_meta keeps one row per source record and counts the duplicates.

import type { FaultInjector } from '@eventpulse/fault-injector';
import { isBlocking } from '@eventpulse/retry';
import type { Classifier, ErrorClass, TokenBucket } from '@eventpulse/retry';
import type { SinkStats } from '../observability/sink-stats.js';
import type { DeadLetter, DlqRecorder, DlqSink } from '../sink/dlq-publisher.js';
import type { RetryLetter, RetrySink } from '../retry/retry-publisher.js';

export class DlqPublishError extends Error {
  readonly name = 'DlqPublishError';
  constructor(readonly cause: unknown) {
    super(`DLQ publish failed; source offset not committed, chunk will be retried: ${String((cause as Error)?.message ?? cause)}`);
  }
}

export class RetryPublishError extends Error {
  readonly name = 'RetryPublishError';
  constructor(readonly cause: unknown) {
    super(`retry-tier publish failed; source offset not committed, chunk will be retried: ${String((cause as Error)?.message ?? cause)}`);
  }
}

/** Too many records want a retry tier right now (a misclassified outage or a bad deploy): pause instead. */
export class RetryRateLimitedError extends Error {
  readonly name = 'RetryRateLimitedError';
  constructor(n: number, pausedMs: number) {
    super(`${n} record(s) for the retry tiers exceed RETRY_MAX_RATE_PER_S: partition paused for ${pausedMs} ms, chunk will be retried`);
  }
}

/** dlq_meta could not be written: blocking class (DB down) -> pause, else retry the chunk. */
export class DlqMetaError extends Error {
  readonly name = 'DlqMetaError';
  constructor(readonly errorClass: ErrorClass, readonly cause: unknown, readonly blocking: boolean) {
    super(`dlq_meta insert failed (${errorClass}); source offset not committed, chunk will be retried: ${String((cause as Error)?.message ?? cause)}`);
  }
}

export interface SettleDeps {
  stats: SinkStats;
  dlq: DlqSink;
  dlqMeta?: DlqRecorder;
  retryOut?: RetrySink;
  limiter?: Pick<TokenBucket, 'tryTake'>;
  classifier: Pick<Classifier, 'classify'>;
  faults: Pick<FaultInjector, 'hit'>;
  pauseMs: number;
}

export async function settle(retries: RetryLetter[], dead: DeadLetter[], pausePartition: (ms: number) => void, deps: SettleDeps): Promise<void> {
  const { stats, faults } = deps;
  if (retries.length > 0) {
    if (!deps.retryOut) throw new Error('retry letters without a retry publisher');
    if (deps.limiter && !deps.limiter.tryTake(retries.length)) {
      stats.retryRateLimited++;
      stats.countRetryRateLimited();
      pausePartition(deps.pauseMs);
      stats.pauses++;
      throw new RetryRateLimitedError(retries.length, deps.pauseMs);
    }
    try {
      await deps.retryOut.publish(retries);
    } catch (e) {
      stats.retryPublishErrors++;
      throw new RetryPublishError(e);
    }
    for (const r of retries) stats.countRetryPublished(r.route.tier);
    faults.hit('retry:after-retry-publish-before-commit', { rows: retries.length });
  }

  if (dead.length > 0) {
    let published;
    try {
      published = await deps.dlq.publish(dead); // acks=all; only then may the source offset move past these records
    } catch (e) {
      stats.dlqErrors++;
      throw new DlqPublishError(e);
    }
    faults.hit('dlq:after-dlq-publish-before-meta', { rows: dead.length });
    if (deps.dlqMeta) {
      try {
        const r = await deps.dlqMeta.record(published);
        if (r.duplicates > 0) stats.countDlqDuplicates(r.duplicates);
      } catch (e) {
        const cls = deps.classifier.classify(e);
        const blocking = isBlocking(cls);
        if (blocking) {
          pausePartition(deps.pauseMs);
          stats.pauses++;
        }
        throw new DlqMetaError(cls, e, blocking);
      }
    }
    faults.hit('dlq:after-meta-before-commit', { rows: dead.length });
  }
}
