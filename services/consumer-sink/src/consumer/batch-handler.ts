// What the sink does with one batch of one partition. Kept free of Kafka and Nest types so it can be
// unit-tested with a fake writer and fake offset callbacks.
//
// S2-F2 behavior:
//   1. Parse every message. If any is invalid, throw: the client seeks back and the batch is retried
//      (a poison message blocks its partition; S2-F4 replaces this with bounded retry + DLQ).
//   2. Write the rows in chunks of maxRows, one transaction per chunk.
//   3. After the last chunk committed in Postgres, resolve the last offset and commit it to Kafka.
// S2-F3 refines step 3 into a per-chunk protocol with rebalance checks and crash-point hooks.

import type { SinkStats } from '../observability/sink-stats.js';
import type { BatchWriter } from '../sink/batch-writer.js';
import { chunk } from '../sink/batch-writer.js';
import { parseMessage } from '../sink/parse.js';
import type { EventRow, ParseError, RawMessage } from '../sink/parse.js';

export class PoisonBatchError extends Error {
  readonly name = 'PoisonBatchError';
  constructor(readonly errors: ParseError[]) {
    const e = errors[0];
    super(
      `${errors.length} unparseable record(s); first at partition ${e.partition} offset ${e.offset}: ${e.code} (${e.message})`,
    );
  }
}

export interface BatchContext {
  messages: RawMessage[];
  resolveOffset(offset: string): void;
  commitOffsetsIfNecessary(): Promise<void>;
}

export interface HandlerDeps {
  writer: Pick<BatchWriter, 'writeChunk'>;
  stats: SinkStats;
  maxRows: number;
}

export async function handleBatch(ctx: BatchContext, deps: HandlerDeps): Promise<void> {
  const { messages } = ctx;
  if (messages.length === 0) return;
  deps.stats.consumed += messages.length;
  deps.stats.batches++;

  const rows: EventRow[] = [];
  const errors: ParseError[] = [];
  for (const m of messages) {
    const r = parseMessage(m);
    if (r.ok) rows.push(r.row);
    else errors.push(r.error);
  }
  if (errors.length > 0) throw new PoisonBatchError(errors);

  for (const part of chunk(rows, deps.maxRows)) {
    const w = await deps.writer.writeChunk(part);
    deps.stats.chunks++;
    deps.stats.attempted += w.attempted;
    deps.stats.inserted += w.inserted;
  }

  // Only after every chunk's COMMIT: mark the batch done and commit the offset to Kafka.
  ctx.resolveOffset(messages[messages.length - 1].offset);
  await ctx.commitOffsetsIfNecessary();
  deps.stats.lastBatchAt = Date.now();
}
