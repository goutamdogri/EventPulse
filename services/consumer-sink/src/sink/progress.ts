// S3-F3: telling WHY a duplicate reached the sink.
//
//   reprocessed   the record's offset was already stored once for this partition (sink_offsets.max_offset, written
//                 in the same transaction as the rows): a crash between DB commit and offset commit, a rebalance,
//                 a failed offset commit, an offset reset
//   redelivered   a NEW offset carrying a key that is already stored: a client retry that passed the API's
//                 idempotency (window expired, Redis lost, fail_open, ambiguous produce, API crash), a DLQ replay,
//                 a producer-side duplicate
//   unknown       the stored offset could not be read (Postgres trouble); counted, never dropped
//
// The per-partition max is read from sink_offsets the first time a partition is handled after an assignment, then
// advanced in memory after every successful write (so a chunk re-read by THIS process after a failed offset commit
// is also "reprocessed"). It is forgotten on every assignment change: another member may have advanced it.

import type { Pool } from 'pg';
import type { DuplicateRow } from './batch-writer.js';

export type DuplicateCause = 'reprocessed' | 'redelivered' | 'unknown';
export const DUPLICATE_CAUSES: readonly DuplicateCause[] = ['reprocessed', 'redelivered', 'unknown'];

export interface CauseSplit {
  reprocessed: number;
  redelivered: number;
  unknown: number;
}

/**
 * Pure: split duplicate rows by cause, given the highest offset stored for their partition BEFORE this chunk
 * (undefined = nothing stored yet for this partition, null = could not be read).
 */
export function splitByCause(dups: DuplicateRow[], storedMax: bigint | null | undefined): CauseSplit {
  const out: CauseSplit = { reprocessed: 0, redelivered: 0, unknown: 0 };
  for (const d of dups) {
    if (storedMax === null) out.unknown++;
    else if (storedMax !== undefined && BigInt(d.offset) <= storedMax) out.reprocessed++;
    else out.redelivered++;
  }
  return out;
}

/** Messages of a chunk whose offset was already stored once (all of them were processed before). */
export function reprocessedMessages(offsets: string[], storedMax: bigint | null | undefined): number {
  if (storedMax === null || storedMax === undefined) return 0;
  let n = 0;
  for (const o of offsets) if (BigInt(o) <= storedMax) n++;
  return n;
}

export interface ProgressSource {
  /** Highest stored offset of the partition before the next write: undefined = none yet, null = unknown. */
  known(partition: number): Promise<bigint | null | undefined>;
  advance(partition: number, offset: string): void;
  forget(partitions?: number[]): void;
}

export class PartitionProgress implements ProgressSource {
  private readonly max = new Map<number, bigint | undefined>();

  constructor(
    private readonly pool: Pick<Pool, 'query'>,
    private readonly group: string,
    private readonly topic: string,
    private readonly onError: (e: unknown) => void = () => undefined,
  ) {}

  async known(partition: number): Promise<bigint | null | undefined> {
    if (this.max.has(partition)) return this.max.get(partition);
    try {
      const r = await this.pool.query<{ max_offset: string }>(
        'SELECT max_offset FROM sink_offsets WHERE consumer_group = $1 AND topic = $2 AND partition = $3',
        [this.group, this.topic, partition],
      );
      const v = r.rows[0] ? BigInt(r.rows[0].max_offset) : undefined;
      this.max.set(partition, v);
      return v;
    } catch (e) {
      this.onError(e); // not cached: the next chunk tries again
      return null;
    }
  }

  advance(partition: number, offset: string): void {
    if (!this.max.has(partition)) return; // not loaded: the next known() reads the database, which has it
    const o = BigInt(offset);
    const cur = this.max.get(partition);
    if (cur === undefined || o > cur) this.max.set(partition, o);
  }

  forget(partitions?: number[]): void {
    if (!partitions) this.max.clear();
    else for (const p of partitions) this.max.delete(p);
  }
}
