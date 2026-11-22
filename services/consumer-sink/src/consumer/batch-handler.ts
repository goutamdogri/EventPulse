// What the sink does with one batch of one partition. Kept free of Kafka and Nest types so it can be
// unit-tested with a fake writer and fake offset callbacks.
//
// S2-F3 commit protocol: offsets are committed only after the database transaction commits.
//
//   for each chunk of the batch (SINK_MAX_ROWS messages):
//       stop if the consumer is shutting down or the partition was revoked (rebalance)
//       parse                                   crash points: before_parse, after_parse
//       write: BEGIN; INSERT ...; COMMIT        (offset NOT resolved if this throws)
//                                               crash point: after_db_commit (rows stored, offset not committed)
//       stop if the partition was revoked meanwhile (the new owner reprocesses; ON CONFLICT absorbs it)
//       resolveOffset(last offset of the chunk) the client stores last + 1, the next offset to read
//       commit offsets to Kafka
//
// This is the ONLY place that resolves or commits offsets in this service.
//
// Consequences (at-least-once):
//   - DB write fails     -> nothing resolved, the client seeks back, the chunk is retried: no loss.
//   - crash before COMMIT -> chunk reprocessed: no loss, no duplicate rows.
//   - crash after COMMIT but before the offset commit -> chunk reprocessed: duplicates, absorbed and counted.
//   - offset commit fails -> rows are already stored; the error propagates, the offset is committed later
//     or the chunk is reprocessed by the next owner: duplicates, never loss.
//   - at most one chunk per partition is ever "written but not committed".
//
// Poison records still throw (S2-F2 behavior; S2-F4 adds bounded retry + DLQ). Chunks before the poison
// chunk are committed, so the partition is blocked exactly at the bad chunk.

import type { SinkStats } from '../observability/sink-stats.js';
import type { BatchWriter } from '../sink/batch-writer.js';
import { chunk } from '../sink/batch-writer.js';
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

export interface BatchContext {
  messages: RawMessage[];
  resolveOffset(offset: string): void;
  commitOffsetsIfNecessary(): Promise<void>;
  heartbeat(): Promise<void>;
  /** false once the consumer is disconnecting. */
  isRunning(): boolean;
  /** true once this partition was revoked by a rebalance (or seeked). */
  isStale(): boolean;
}

export interface HandlerDeps {
  writer: Pick<BatchWriter, 'writeChunk'>;
  stats: SinkStats;
  maxRows: number;
  faults: Pick<FaultInjector, 'startBatch' | 'hit'>;
  /** SINK_PROCESSING_DELAY_MS: artificial work per chunk, before the DB write (tests, K8). */
  processingDelayMs: number;
  sleep?: (ms: number) => Promise<void>;
}

const defaultSleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

export async function handleBatch(ctx: BatchContext, deps: HandlerDeps): Promise<void> {
  if (ctx.messages.length === 0) return;
  const { stats, faults } = deps;
  const sleep = deps.sleep ?? defaultSleep;
  stats.batches++;
  faults.startBatch();

  for (const part of chunk(ctx.messages, deps.maxRows)) {
    if (!ctx.isRunning() || ctx.isStale()) {
      stats.stoppedEarly++; // the rest of the batch is not resolved: it will be read again
      return;
    }

    faults.hit('before_parse');
    stats.consumed += part.length;
    const rows: EventRow[] = [];
    const errors: ParseError[] = [];
    for (const m of part) {
      const r = parseMessage(m);
      if (r.ok) rows.push(r.row);
      else errors.push(r.error);
    }
    if (errors.length > 0) throw new PoisonBatchError(errors);
    faults.hit('after_parse');

    if (deps.processingDelayMs > 0) await sleep(deps.processingDelayMs);

    const w = await deps.writer.writeChunk(rows); // resolves only after COMMIT
    stats.chunks++;
    stats.attempted += w.attempted;
    stats.inserted += w.inserted;
    faults.hit('after_db_commit');

    if (ctx.isStale()) {
      // Revoked while we were writing: committing for a partition we no longer own would fail. The rows are
      // stored; the new owner reads this chunk again and ON CONFLICT absorbs it (counted there as duplicates).
      stats.stoppedEarly++;
      return;
    }

    ctx.resolveOffset(part[part.length - 1].offset);
    const t0 = performance.now();
    try {
      await ctx.commitOffsetsIfNecessary();
    } catch (e) {
      stats.commitErrors++;
      throw new OffsetCommitError(e);
    }
    stats.commits++;
    stats.lastCommitMs = performance.now() - t0;
    stats.lastBatchAt = Date.now();
    await ctx.heartbeat();
  }
}
