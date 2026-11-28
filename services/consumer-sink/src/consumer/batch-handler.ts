// What the sink does with one batch of one partition. Kept free of Kafka and Nest types so it can be
// unit-tested with fakes.
//
// S2-F3 commit protocol: offsets are committed only after the database transaction commits.
// S2-F4 poison handling: a record that fails on its own goes to the DLQ; a database that is down never does.
//
//   for each chunk of the batch (SINK_MAX_ROWS messages):
//       stop if the consumer is shutting down or the partition was revoked (rebalance)
//       parse every record                        crash points: consumer:before-parse, consumer:after-parse
//         unparseable -> dead letter (PERMANENT_PARSE), no retry
//       write the good rows: BEGIN; INSERT ...; sink_offsets; COMMIT   (S3-F2, idempotent on (tenant_id, event_id))
//                                        crash points: consumer:before-insert, consumer:after-insert-before-commit
//         blocking class (TRANSIENT_SYSTEMIC, SYSTEMIC_CONFIG: a dependency is down)
//                                     -> backoff + retry inside the window; then pause the partition and throw
//                                        (nothing resolved: the chunk is read again after the pause). NEVER a
//                                        retry topic, NEVER the DLQ.
//         TRANSIENT_RECORD (deadlock, serialization failure, statement timeout) for the whole chunk
//                                     -> the chunk is retried in-process RETRY_INPROCESS_ATTEMPTS times first
//                                        (usually enough), then isolated like below
//         row-level class             -> isolate by bisection; good rows are written; a row failing ON ITS OWN:
//                                          PERMANENT_DB                -> dead letter (retrying bad data never helps)
//                                          TRANSIENT_RECORD / UNKNOWN  -> retry tier 1 (S3-F5, not_before header)
//                                        crash point: consumer:after-db-commit
//       settle (settle.ts): retry records -> tier topic (rate-limited), dead letters -> events.dlq + dlq_meta,
//                                        each acked before the source offset moves (failure -> throw, nothing resolved)
//       With retry tiers off (RETRY_TOPICS_ENABLED=false) the Stage 2 behavior returns: TRANSIENT_RECORD blocks, and a
//       bad row is retried in-process SINK_MAX_RETRIES times before it is dead-lettered.
//       stop if the partition was revoked meanwhile
//       resolveOffset(last offset of the chunk) + commit offsets to Kafka (a failed commit is retried
//                                        COMMIT_RETRIES times; the rows are stored, so a retry is always safe)
//                                        crash point: consumer:after-offset-commit
//
// This is the ONLY place that resolves or commits offsets in this service.
// With SINK_DLQ_ENABLED=false the old S2-F2/F3 behavior returns: a poison record throws and blocks its partition.

import type { ChunkFailure, SinkStats } from '../observability/sink-stats.js';
import type { BatchWriter, DuplicateRow, WriteResult } from '../sink/batch-writer.js';
import { reprocessedMessages, splitByCause } from '../sink/progress.js';
import type { ProgressSource } from '../sink/progress.js';
import { chunk } from '../sink/batch-writer.js';
import type { DeadLetter, DlqRecorder, DlqSink } from '../sink/dlq-publisher.js';
import { DEFAULT_CLASSIFIER, backoffMs, classifyParseCode, errorCode, isBlocking, route } from '@eventpulse/retry';
import type { Classifier, ErrorClass, TierPolicy, TokenBucket } from '@eventpulse/retry';
import type { RetryLetter, RetrySink } from '../retry/retry-publisher.js';
import { DlqMetaError, DlqPublishError, RetryPublishError, RetryRateLimitedError, settle } from './settle.js';

export { DlqMetaError, DlqPublishError, RetryPublishError, RetryRateLimitedError } from './settle.js';
import { DESERIALIZE_CODES, parseMessage } from '../sink/parse.js';
import { RegistryUnavailableError } from '@eventpulse/schema';
import type { EventSerde } from '@eventpulse/schema';
import type { EventRow, ParseError, RawMessage } from '../sink/parse.js';
import type { FaultInjector } from '@eventpulse/fault-injector';

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

/** S3-F7: the schema registry could not be asked for a writer schema: pause and retry, NEVER dead-letter. */
export class RegistryUnavailableChunkError extends Error {
  readonly name = 'RegistryUnavailableChunkError';
  constructor(readonly cause: unknown, pausedMs: number) {
    super(`schema registry unavailable (partition paused for ${pausedMs} ms, chunk will be retried): ${String((cause as Error)?.message ?? cause)}`);
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
  /** K8: limits the delay to a window of chunks (skip the first `skip`, delay the next `left`). Absent = delay every chunk. */
  delayWindow?: { skip: number; left: number };
  dlq: DlqSink;
  retry: RetryPolicy;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
  rand?: () => number;
  /** S3-F3: highest stored offset per partition, to split duplicates into reprocessed / redelivered. */
  progress?: ProgressSource;
  /** One structured line per chunk that had duplicates or conflicts (never one per row). */
  log?: (line: string) => void;
  /** S3-F7: Avro decoding (writer schemas fetched per chunk) and whether Stage 2 JSON values are still accepted. */
  schema?: { serde: EventSerde; acceptJson: boolean };
  /** S3-F5: the one classifier (CLASSIFIER_OVERRIDE applied); default packages/retry's. */
  classifier?: Pick<Classifier, 'classify'>;
  /** S3-F5: retry tiers. Absent = RETRY_TOPICS_ENABLED=false (Stage 2 in-process retries). */
  tiers?: { policy: TierPolicy; inProcessAttempts: number };
  retryOut?: RetrySink;
  limiter?: Pick<TokenBucket, 'tryTake'>;
  dlqMeta?: DlqRecorder;
  /** Topic of the records (the origin written into retry and DLQ headers). */
  sourceTopic?: string;
}

interface RetryCandidate {
  raw: RawMessage;
  row: EventRow;
  errorClass: ErrorClass;
  error: unknown;
}

const classifierOf = (deps: HandlerDeps) => deps.classifier ?? DEFAULT_CLASSIFIER;
/** With tiers off, TRANSIENT_RECORD blocks the partition like an outage (the Stage 2 behavior). */
const blockingHere = (cls: ErrorClass, deps: HandlerDeps) => isBlocking(cls) || (!deps.tiers && cls === 'TRANSIENT_RECORD');

const defaultSleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

interface WriteOutcome {
  attempted: number;
  inserted: number;
  dead: DeadLetter[];
  retry: RetryCandidate[];
  dups: DuplicateRow[];
  conflicts: number;
}

const add = (a: WriteOutcome, b: WriteOutcome): WriteOutcome => ({
  attempted: a.attempted + b.attempted,
  inserted: a.inserted + b.inserted,
  dead: [...a.dead, ...b.dead],
  retry: [...a.retry, ...b.retry],
  dups: [...a.dups, ...b.dups],
  conflicts: a.conflicts + b.conflicts,
});
const fromWrite = (w: WriteResult): WriteOutcome => ({
  attempted: w.attempted,
  inserted: w.inserted,
  dead: [],
  retry: [],
  dups: w.duplicateKeys ?? [],
  conflicts: w.conflicts?.length ?? 0,
});
const NOTHING: WriteOutcome = { attempted: 0, inserted: 0, dead: [], retry: [], dups: [], conflicts: 0 };

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
        if (w.conflicts?.length) stats.countConflicts(w.conflicts);
        if (w.conflictCheckFailed) stats.conflictCheckErrors++;
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
  if (e instanceof RegistryUnavailableChunkError) return 'registry_unavailable';
  if (e instanceof DlqPublishError) return 'dlq_publish';
  if (e instanceof DlqMetaError) return e.blocking ? 'db_unavailable' : 'dlq_publish';
  if (e instanceof RetryRateLimitedError) return 'retry_rate_limited';
  if (e instanceof RetryPublishError) return 'retry_publish';
  if (e instanceof OffsetCommitError) return 'commit';
  return dlqEnabled ? 'other' : 'poison_blocked'; // DLQ off: a row Postgres rejects blocks the partition
}

/** One chunk: parse, write, dead-letter, commit. Returns false if the partition was revoked meanwhile. */
async function handleChunk(part: RawMessage[], ctx: BatchContext, deps: HandlerDeps): Promise<boolean> {
  const { stats, faults } = deps;
  faults.hit('consumer:before-parse', { rows: part.length });
  const good: { raw: RawMessage; row: EventRow }[] = [];
  const parseErrors: { raw: RawMessage; error: ParseError }[] = [];
  if (deps.schema) {
    // Fetch the writer schema of every Avro record of the chunk that this process has not seen yet (once per id).
    try {
      await deps.schema.serde.prefetch(part.map((m) => m.value));
    } catch (e) {
      if (!(e instanceof RegistryUnavailableError)) throw e;
      stats.registryUnavailable++;
      ctx.pausePartition(deps.retry.pauseMs);
      stats.pauses++;
      throw new RegistryUnavailableChunkError(e, deps.retry.pauseMs);
    }
  }
  const parseOpts = { serde: deps.schema?.serde, acceptJson: deps.schema?.acceptJson ?? true };
  for (const m of part) {
    const r = parseMessage(m, parseOpts);
    if (r.ok) {
      good.push({ raw: m, row: r.row });
      if (r.row.schema_id !== undefined) stats.noteSchemaId(r.row.schema_id);
    } else {
      parseErrors.push({ raw: m, error: r.error });
      if (DESERIALIZE_CODES.includes(r.error.code)) stats.countDeserializeFailure(r.error.code);
    }
  }
  if (parseErrors.length > 0) {
    for (const p of parseErrors) stats.countError(classifyParseCode(p.error.code));
    if (!deps.retry.dlqEnabled) throw new PoisonBatchError(parseErrors.map((p) => p.error));
  }
  faults.hit('consumer:after-parse', { rows: part.length });

  if (deps.processingDelayMs > 0) {
    const w = deps.delayWindow;
    let delay = true;
    if (w) {
      if (w.skip > 0) { w.skip--; delay = false; }
      else if (w.left > 0) w.left--;
      else delay = false;
    }
    if (delay) await (deps.sleep ?? defaultSleep)(deps.processingDelayMs);
  }

  // S3-F3: what this partition had already stored BEFORE this chunk (one query per partition after an assignment).
  const partition = part[0].partition;
  const storedBefore = good.length > 0 && deps.progress ? await deps.progress.known(partition) : undefined;
  const out = await writeWithRetry(good, ctx, deps); // throws DbUnavailableError when the DB is down
  faults.hit('consumer:after-db-commit', { rows: part.length });

  const dead: DeadLetter[] = [
    ...parseErrors.map((p) => ({ raw: p.raw, errorClass: classifyParseCode(p.error.code), errorCode: p.error.code, errorMessage: p.error.message, attempts: 0 })),
    ...out.dead,
  ];
  const now = (deps.now ?? Date.now)();
  const retries: RetryLetter[] = out.retry.map((c) => {
    const r = route(0, c.errorClass, deps.tiers!.policy, now, deps.rand);
    if (r.kind !== 'retry') throw new Error(`a retry candidate routed to ${r.kind}`); // retriable at attempt 0 always gets tier 1
    return {
      raw: c.raw,
      route: r,
      origin: { topic: deps.sourceTopic ?? 'unknown', partition: c.raw.partition, offset: c.raw.offset, firstFailedAt: new Date(now).toISOString() },
      failure: { errorClass: c.errorClass, errorCode: errorCode(c.error), errorMessage: String((c.error as Error)?.message ?? c.error) },
    };
  });
  // Retry records and dead letters acknowledged (and indexed) before the source offset may move.
  await settle(retries, dead, (ms) => ctx.pausePartition(ms), {
    stats,
    dlq: deps.dlq,
    dlqMeta: deps.dlqMeta,
    retryOut: deps.retryOut,
    limiter: deps.limiter,
    classifier: classifierOf(deps),
    faults: deps.faults,
    pauseMs: deps.retry.pauseMs,
  });
  // The chunk's work is done. consumed = inserted + duplicates + dlq + retried.
  const causes = splitByCause(out.dups, storedBefore);
  const reprocessed = reprocessedMessages(part.map((m) => m.offset), storedBefore);
  stats.completeChunk({ consumed: part.length, attempted: out.attempted, inserted: out.inserted, dlq: dead, retried: retries.length, causes, reprocessed, partition });
  if (deps.progress) {
    const notWritten = new Set([...out.dead.map((d) => d.raw.offset), ...out.retry.map((c) => c.raw.offset)]);
    const written = good.filter((g) => !notWritten.has(g.row.offset));
    if (written.length > 0) deps.progress.advance(partition, written.reduce((m, g) => (BigInt(g.row.offset) > BigInt(m) ? g.row.offset : m), written[0].row.offset));
  }
  if (deps.log && (out.dups.length > 0 || out.conflicts > 0)) {
    deps.log(JSON.stringify({ msg: 'duplicates', partition, from_offset: part[0].offset, to_offset: part[part.length - 1].offset, attempted: out.attempted, inserted: out.inserted, duplicates: out.dups.length, ...causes, reprocessed_messages: reprocessed, conflicts: out.conflicts }));
  }

  if (ctx.isStale()) {
    // Revoked while we were writing: the new owner reads this chunk again; ON CONFLICT absorbs it.
    stats.stoppedEarly++;
    return false;
  }

  ctx.resolveOffset(part[part.length - 1].offset);
  const c0 = performance.now();
  await commitWithRetry(ctx, deps);
  stats.observeCommit(performance.now() - c0);
  faults.hit('consumer:after-offset-commit', { rows: part.length });
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
  if (good.length === 0) return NOTHING;
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
      const cls = classifierOf(deps).classify(e);
      if (!p.dlqEnabled && !blockingHere(cls, deps)) {
        deps.stats.countError(cls);
        throw e; // DLQ off: a permanent row error blocks the partition (the client retries the chunk)
      }
      if (!blockingHere(cls, deps)) throw e; // a bug: writeIsolating handles every row-level class
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
 * until the bad rows are alone. A row failing alone: PERMANENT_DB -> dead letter, TRANSIENT_RECORD / UNKNOWN ->
 * retry tier (S3-F5); with tiers off, the Stage 2 in-process retries (retryOneRow).
 * Blocking errors (DB down / schema broken) are rethrown at any depth: the whole chunk is then retried.
 */
async function writeIsolating(
  rows: EventRow[],
  rawByOffset: Map<string, RawMessage>,
  deps: HandlerDeps,
  depth = 0,
): Promise<WriteOutcome> {
  const classify = (e: unknown) => classifierOf(deps).classify(e);
  let error: unknown;
  try {
    return fromWrite(await deps.writer.writeChunk(rows));
  } catch (e) {
    error = e;
  }
  let cls = classify(error);
  if (blockingHere(cls, deps)) throw error;
  // A transient record-level error for the WHOLE chunk is usually gone on the next try: retry in-process first,
  // so a deadlock does not turn a healthy chunk into retry-topic traffic.
  if (depth === 0 && deps.tiers && cls === 'TRANSIENT_RECORD') {
    for (let attempt = 1; attempt <= deps.tiers.inProcessAttempts; attempt++) {
      deps.stats.countRetry(cls);
      await (deps.sleep ?? defaultSleep)(backoffMs(attempt, deps.retry.baseMs, deps.retry.maxMs, deps.rand));
      try {
        return fromWrite(await deps.writer.writeChunk(rows));
      } catch (e) {
        error = e;
        cls = classify(e);
        if (blockingHere(cls, deps)) throw e;
        if (cls !== 'TRANSIENT_RECORD') break;
      }
    }
  }
  // Not counted in errors_by_class here: a bad row is counted once, when its fate is decided.
  if (depth === 0) deps.stats.isolations++;
  if (rows.length > 1) {
    const mid = rows.length >> 1;
    const a = await writeIsolating(rows.slice(0, mid), rawByOffset, deps, depth + 1);
    const b = await writeIsolating(rows.slice(mid), rawByOffset, deps, depth + 1);
    return add(a, b);
  }
  const row = rows[0];
  if (!deps.tiers) return retryOneRow(row, error, cls, rawByOffset, deps);
  deps.stats.countError(cls);
  const raw = rawByOffset.get(row.offset)!;
  if (cls === 'PERMANENT_DB' || cls === 'PERMANENT_PARSE' || cls === 'PERMANENT_SCHEMA') {
    return { ...NOTHING, dead: [{ raw, errorClass: cls, errorCode: errorCode(error), errorMessage: String((error as Error)?.message ?? error), attempts: 0, tenantId: row.tenant_id, eventId: row.event_id }] };
  }
  return { ...NOTHING, retry: [{ raw, row, errorClass: cls, error }] };
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
      const cls = classifierOf(deps).classify(e);
      if (blockingHere(cls, deps)) throw e;
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
        tenantId: row.tenant_id,
        eventId: row.event_id,
      },
    ],
    retry: [],
    dups: [],
    conflicts: 0,
  };
}
