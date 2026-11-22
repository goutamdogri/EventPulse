// What the sink does with one batch of one partition. Kept free of Kafka and Nest types so it can be
// unit-tested with fakes.
//
// S2-F3 commit protocol: offsets are committed only after the database transaction commits.
// S2-F4 poison handling: a record that fails on its own goes to the DLQ; a database that is down never does.
//
//   for each chunk of the batch (SINK_MAX_ROWS messages):
//       stop if the consumer is shutting down or the partition was revoked (rebalance)
//       parse every record                        crash points: before_parse, after_parse
//         unparseable -> dead letter (PERMANENT_PARSE), no retry
//       write the good rows: BEGIN; INSERT ...; COMMIT
//         TRANSIENT/SYSTEMIC DB error -> backoff + retry inside the window; then pause the partition and throw
//                                        (nothing resolved: the chunk is read again after the pause)
//         PERMANENT/UNKNOWN DB error  -> isolate by bisection; good rows are written, each bad row is retried
//                                        SINK_MAX_RETRIES times, then becomes a dead letter (PERMANENT_DB)
//                                        crash point: after_db_commit
//       publish dead letters to the DLQ and wait for the ack (failure -> throw, nothing resolved)
//       stop if the partition was revoked meanwhile
//       resolveOffset(last offset of the chunk) + commit offsets to Kafka (a failed commit is retried
//                                        COMMIT_RETRIES times; the rows are stored, so a retry is always safe)
//
// This is the ONLY place that resolves or commits offsets in this service.
// With SINK_DLQ_ENABLED=false the old S2-F2/F3 behavior returns: a poison record throws and blocks its partition.

import type { ChunkFailure, SinkStats } from '../observability/sink-stats.js';
import type { BatchWriter, WriteResult } from '../sink/batch-writer.js';
import { chunk } from '../sink/batch-writer.js';
import type { DeadLetter, DlqSink } from '../sink/dlq-publisher.js';
import { backoffMs, classifyDbError, errorCode, isBlocking } from '../sink/errors.js';
import type { ErrorClass } from '../sink/errors.js';
import { parseMessage } from '../sink/parse.js';
import type { EventRow, ParseError, RawMessage } from '../sink/parse.js';
import type { FaultInjector } from './fault-injector.js';

export class PoisonBatchError extends Error {
  readonly name = 'PoisonBatchError';
  constructor(readonly errors: ParseError[]) {
    const e = errors[0];
    super(
      `${errors.length} unparseable record(s); first at partition ${e.partition} offset ${e.offset}: ${e.code} (${e.message})`,
    );
  }
}

export class OffsetCommitError extends Error {
  readonly name = 'OffsetCommitError';
  constructor(readonly cause: unknown) {
    super(`offset commit failed after the DB commit (rows are stored; they may be reprocessed): ${String((cause as Error)?.message ?? cause)}`);
  }
}

export class DbUnavailableError extends Error {
  readonly name = 'DbUnavailableError';
  constructor(readonly errorClass: ErrorClass, readonly cause: unknown, pausedMs: number) {
    super(`${errorClass} while writing (partition paused for ${pausedMs} ms, chunk will be retried): ${String((cause as Error)?.message ?? cause)}`);
  }
}

export class DlqPublishError extends Error {
  readonly name = 'DlqPublishError';
  constructor(readonly cause: unknown) {
    super(`DLQ publish failed; source offset not committed, chunk will be retried: ${String((cause as Error)?.message ?? cause)}`);
  }
}

export interface BatchContext {
  messages: RawMessage[];
  resolveOffset(offset: string): void;
  commitOffsetsIfNecessary(): Promise<void>;
  heartbeat(): Promise<void>;
  /** false once the consumer is disconnecting. */
  isRunning(): boolean;
  /** true once this partition was revoked by a rebalance (or seeked / paused). */
  isStale(): boolean;
  /** Pause fetching this partition for ms, then resume (the unresolved chunk is read again). */
  pausePartition(ms: number): void;
}

export interface RetryPolicy {
  dlqEnabled: boolean;
  /** Retries of one isolated bad row before it is dead-lettered. */
  maxRetries: number;
  baseMs: number;
  maxMs: number;
  /** How long to retry a down DB inside one eachBatch call before pausing the partition. */
  transientWindowMs: number;
  pauseMs: number;
}

export interface HandlerDeps {
  writer: Pick<BatchWriter, 'writeChunk'>;
  stats: SinkStats;
  maxRows: number;
  faults: Pick<FaultInjector, 'startBatch' | 'hit'>;
  /** SINK_PROCESSING_DELAY_MS: artificial work per chunk, before the DB write (tests, K8). */
  processingDelayMs: number;
  dlq: DlqSink;
  retry: RetryPolicy;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
  rand?: () => number;
}

const defaultSleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

interface WriteOutcome {
  attempted: number;
  inserted: number;
  dead: DeadLetter[];
}

const add = (a: WriteOutcome, b: WriteOutcome): WriteOutcome => ({
  attempted: a.attempted + b.attempted,
  inserted: a.inserted + b.inserted,
  dead: [...a.dead, ...b.dead],
});
const fromWrite = (w: WriteResult): WriteOutcome => ({ attempted: w.attempted, inserted: w.inserted, dead: [] });

export async function handleBatch(ctx: BatchContext, deps: HandlerDeps): Promise<void> {
  if (ctx.messages.length === 0) return;
  const { stats, faults } = deps;
  stats.observeBatch(ctx.messages.length);
  faults.startBatch();
  // Every DB write (including isolation sub-writes) feeds the write-latency, pool-wait and freshness metrics.
  const observed: HandlerDeps = {
    ...deps,
    writer: {
      writeChunk: async (rows) => {
        const w = await deps.writer.writeChunk(rows);
        stats.observeDbWrite(w.durationMs, w.connectMs ?? 0, w.freshnessS ?? []);
        return w;
      },
    },
  };

  for (const part of chunk(ctx.messages, deps.maxRows)) {
    if (!ctx.isRunning() || ctx.isStale()) {
      stats.stoppedEarly++; // the rest of the batch is not resolved: it will be read again
      return;
    }
    const t0 = performance.now();
    try {
      if (!(await handleChunk(part, ctx, observed))) return; // revoked meanwhile: stop this batch
    } catch (e) {
      stats.failChunk(failureReason(e, deps.retry.dlqEnabled));
      throw e;
    }
    stats.observeChunk(part.length, performance.now() - t0);
    stats.lastBatchAt = Date.now();
    await ctx.heartbeat();
  }
}

function failureReason(e: unknown, dlqEnabled: boolean): ChunkFailure {
  if (e instanceof PoisonBatchError) return 'poison_blocked';
  if (e instanceof DbUnavailableError) return 'db_unavailable';
  if (e instanceof DlqPublishError) return 'dlq_publish';
  if (e instanceof OffsetCommitError) return 'commit';
  return dlqEnabled ? 'other' : 'poison_blocked'; // DLQ off: a row Postgres rejects blocks the partition
}

/** One chunk: parse, write, dead-letter, commit. Returns false if the partition was revoked meanwhile. */
async function handleChunk(part: RawMessage[], ctx: BatchContext, deps: HandlerDeps): Promise<boolean> {
  const { stats, faults } = deps;
  faults.hit('before_parse', part.length);
  const good: { raw: RawMessage; row: EventRow }[] = [];
  const parseErrors: { raw: RawMessage; error: ParseError }[] = [];
  for (const m of part) {
    const r = parseMessage(m);
    if (r.ok) good.push({ raw: m, row: r.row });
    else parseErrors.push({ raw: m, error: r.error });
  }
  if (parseErrors.length > 0) {
    stats.countError('PERMANENT_PARSE', parseErrors.length);
    if (!deps.retry.dlqEnabled) throw new PoisonBatchError(parseErrors.map((p) => p.error));
  }
  faults.hit('after_parse', part.length);

  if (deps.processingDelayMs > 0) await (deps.sleep ?? defaultSleep)(deps.processingDelayMs);

  const out = await writeWithRetry(good, ctx, deps); // throws DbUnavailableError when the DB is down
  faults.hit('after_db_commit', part.length);

  const dead: DeadLetter[] = [
    ...parseErrors.map((p) => ({ raw: p.raw, errorClass: 'PERMANENT_PARSE' as const, errorCode: p.error.code, errorMessage: p.error.message, attempts: 1 })),
    ...out.dead,
  ];
  if (dead.length > 0) {
    try {
      await deps.dlq.publish(dead); // acks=all; only then may the source offset move past these records
    } catch (e) {
      stats.dlqErrors++;
      throw new DlqPublishError(e);
    }
  }
  // Rows committed and dead letters acknowledged: the chunk's work is done. consumed = inserted + duplicates + dlq.
  stats.completeChunk({ consumed: part.length, attempted: out.attempted, inserted: out.inserted, dlq: dead });

  if (ctx.isStale()) {
    // Revoked while we were writing: the new owner reads this chunk again; ON CONFLICT absorbs it.
    stats.stoppedEarly++;
    return false;
  }

  ctx.resolveOffset(part[part.length - 1].offset);
  const c0 = performance.now();
  await commitWithRetry(ctx, deps);
  stats.observeCommit(performance.now() - c0);
  return true;
}

/** Commit attempts for one chunk before giving up (1 + this many retries). */
export const COMMIT_RETRIES = 3;

/**
 * Commit the stored offsets. The rows are already in Postgres, so retrying the commit is always safe.
 * Without the retry, a commit that fails on a partition's LAST chunk stays stored-but-uncommitted until another
 * chunk of that partition commits; if none comes, lag never reaches 0 and a restart reprocesses the chunk.
 */
async function commitWithRetry(ctx: BatchContext, deps: HandlerDeps): Promise<void> {
  const sleep = deps.sleep ?? defaultSleep;
  for (let attempt = 0; ; attempt++) {
    try {
      await ctx.commitOffsetsIfNecessary();
      return;
    } catch (e) {
      deps.stats.countCommitError();
      if (attempt >= COMMIT_RETRIES || !ctx.isRunning() || ctx.isStale()) throw new OffsetCommitError(e);
      await sleep(backoffMs(attempt + 1, deps.retry.baseMs, deps.retry.maxMs, deps.rand));
    }
  }
}

/**
 * Writes the rows. Blocking errors (DB down, schema broken) are retried with backoff inside the window, then
 * the partition is paused and DbUnavailableError thrown. Permanent row errors are isolated (with the DLQ on).
 */
async function writeWithRetry(
  good: { raw: RawMessage; row: EventRow }[],
  ctx: BatchContext,
  deps: HandlerDeps,
): Promise<WriteOutcome> {
  if (good.length === 0) return { attempted: 0, inserted: 0, dead: [] };
  const sleep = deps.sleep ?? defaultSleep;
  const now = deps.now ?? Date.now;
  const p = deps.retry;
  const rawByOffset = new Map(good.map((g) => [g.row.offset, g.raw]));
  const rows = good.map((g) => g.row);
  const started = now();

  for (let attempt = 1; ; attempt++) {
    try {
      if (!p.dlqEnabled) return fromWrite(await deps.writer.writeChunk(rows)); // S2-F3 behavior: any error throws
      return await writeIsolating(rows, rawByOffset, deps);
    } catch (e) {
      const cls = classifyDbError(e);
      if (!p.dlqEnabled && !isBlocking(cls)) {
        deps.stats.countError(cls);
        throw e; // DLQ off: a permanent row error blocks the partition (the client retries the chunk)
      }
      deps.stats.countError(cls);
      if (!ctx.isRunning() || ctx.isStale()) throw e;
      const wait = backoffMs(attempt, p.baseMs, p.maxMs, deps.rand);
      if (now() - started + wait > p.transientWindowMs) {
        // Don't hold eachBatch past the rebalance/poll limit: pause the partition and let it come back.
        ctx.pausePartition(p.pauseMs);
        deps.stats.pauses++;
        throw new DbUnavailableError(cls, e, p.pauseMs);
      }
      deps.stats.countRetry(cls);
      await sleep(wait);
      await ctx.heartbeat();
    }
  }
}

/**
 * Write a set of rows in one transaction; if Postgres rejects it for a row-level reason, split it in halves
 * until the bad rows are alone, retry each bad row maxRetries times, then dead-letter it.
 * Blocking errors (DB down / schema broken) are rethrown at any depth: the whole chunk is then retried.
 */
async function writeIsolating(
  rows: EventRow[],
  rawByOffset: Map<string, RawMessage>,
  deps: HandlerDeps,
  depth = 0,
): Promise<WriteOutcome> {
  try {
    return fromWrite(await deps.writer.writeChunk(rows));
  } catch (e) {
    const cls = classifyDbError(e);
    if (isBlocking(cls)) throw e;
    // Not counted in errors_by_class here: a bad row is counted once, when it is finally dead-lettered.
    if (depth === 0) deps.stats.isolations++;
    if (rows.length > 1) {
      const mid = rows.length >> 1;
      const a = await writeIsolating(rows.slice(0, mid), rawByOffset, deps, depth + 1);
      const b = await writeIsolating(rows.slice(mid), rawByOffset, deps, depth + 1);
      return add(a, b);
    }
    return retryOneRow(rows[0], e, cls, rawByOffset, deps);
  }
}

async function retryOneRow(
  row: EventRow,
  firstError: unknown,
  firstClass: ErrorClass,
  rawByOffset: Map<string, RawMessage>,
  deps: HandlerDeps,
): Promise<WriteOutcome> {
  const sleep = deps.sleep ?? defaultSleep;
  let lastError = firstError;
  let lastClass = firstClass;
  for (let retry = 1; retry <= deps.retry.maxRetries; retry++) {
    deps.stats.countRetry(lastClass);
    await sleep(backoffMs(retry, deps.retry.baseMs, deps.retry.maxMs, deps.rand));
    try {
      return fromWrite(await deps.writer.writeChunk([row])); // it was flaky after all
    } catch (e) {
      const cls = classifyDbError(e);
      if (isBlocking(cls)) throw e;
      lastError = e;
      lastClass = cls;
    }
  }
  deps.stats.countError(lastClass); // one bad record = one error (retries are in retries_by_class)
  return {
    attempted: 0,
    inserted: 0,
    dead: [
      {
        raw: rawByOffset.get(row.offset)!,
        errorClass: lastClass === 'UNKNOWN' ? 'UNKNOWN' : 'PERMANENT_DB',
        errorCode: errorCode(lastError),
        errorMessage: String((lastError as Error)?.message ?? lastError),
        attempts: 1 + deps.retry.maxRetries,
      },
    ],
  };
}
