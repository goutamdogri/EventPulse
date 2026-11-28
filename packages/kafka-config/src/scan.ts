// Reading exact records by (partition, offset) without a consumer group (S3-F6: tools/dlq-replay reads one dead letter
// at its DLQ offset, and `reindex` scans the whole DLQ topic).
//
// Uses the low-level (librdkafka-style) consumer of @confluentinc/kafka-javascript because the KafkaJS-compatible
// one has no manual assign: `assign` + start offset, `enable.auto.commit=false`, never a commit. The group id is a
// throw-away one that is never joined (no subscribe), so no offsets are ever stored for it.

import { randomUUID } from 'node:crypto';
import Confluent from '@confluentinc/kafka-javascript';
import type { KafkaClientConfig } from './env.js';

export interface ScannedRecord {
  topic: string;
  partition: number;
  offset: string;
  key: Buffer | null;
  value: Buffer | null;
  headers: Record<string, Buffer>;
  timestamp?: number;
}

/** Offsets [from, to] inclusive on one partition; `to` omitted = up to the current end of the log. */
export interface PartitionRange {
  partition: number;
  from: number;
  to?: number;
}

export interface ScanOptions {
  /** Give up after this long without reaching the end of every range (default 30 s). */
  timeoutMs?: number;
}

type Cb<T> = (err: unknown, v: T) => void;
const call = <T>(fn: (cb: Cb<T>) => void) => new Promise<T>((res, rej) => fn((err, v) => (err ? rej(err) : res(v))));

function headersOf(h: unknown): Record<string, Buffer> {
  const out: Record<string, Buffer> = {};
  for (const one of (h as Record<string, unknown>[] | undefined) ?? []) {
    for (const [k, v] of Object.entries(one)) out[k] = Buffer.isBuffer(v) ? v : Buffer.from(String(v));
  }
  return out;
}

/** Partition ids of a topic (throws if the topic does not exist). */
async function partitionsOf(c: any, topic: string, timeout: number): Promise<number[]> {
  const md: any = await call((cb) => c.getMetadata({ topic, timeout }, cb));
  const t = md.topics.find((x: any) => x.name === topic);
  if (!t || t.partitions.length === 0) throw new Error(`topic ${topic} not found`);
  return t.partitions.map((p: any) => p.id as number).sort((a: number, b: number) => a - b);
}

/**
 * Calls onRecord for every record in the ranges (or the whole topic when ranges = 'all'), in offset order per
 * partition. Offsets below the log start (deleted by retention) are simply not there: the caller sees which
 * wanted offsets never came back.
 */
export async function scanTopic(
  cfg: KafkaClientConfig,
  topic: string,
  ranges: PartitionRange[] | 'all',
  onRecord: (r: ScannedRecord) => void | Promise<void>,
  opts: ScanOptions = {},
): Promise<void> {
  const timeoutMs = opts.timeoutMs ?? 30_000;
  const c: any = new (Confluent as any).KafkaConsumer(
    {
      'bootstrap.servers': cfg.brokers.join(','),
      'client.id': `${cfg.clientId}-scan`,
      'group.id': `scan-${randomUUID()}`,
      'enable.auto.commit': false,
      'enable.auto.offset.store': false,
      'enable.partition.eof': true,
    },
    { 'auto.offset.reset': 'earliest' },
  );
  await call((cb) => c.connect({ timeout: cfg.connectionTimeoutMs * 3 }, cb));
  try {
    let wanted: PartitionRange[];
    if (ranges === 'all') wanted = (await partitionsOf(c, topic, cfg.requestTimeoutMs)).map((partition) => ({ partition, from: 0 }));
    else {
      // A topic or partition that no longer exists simply has none of the wanted records.
      const known = new Set(await partitionsOf(c, topic, cfg.requestTimeoutMs).catch(() => [] as number[]));
      wanted = ranges.filter((r) => known.has(r.partition));
    }
    // Clamp each range to [low watermark, high watermark) so we know when a partition is done.
    const todo = new Map<number, { from: number; end: number }>();
    for (const r of wanted) {
      const w: any = await call((cb) => c.queryWatermarkOffsets(topic, r.partition, cfg.requestTimeoutMs, cb));
      const from = Math.max(r.from, Number(w.lowOffset));
      const end = Math.min(r.to === undefined ? Number(w.highOffset) : r.to + 1, Number(w.highOffset));
      if (from < end) {
        const prev = todo.get(r.partition);
        todo.set(r.partition, prev ? { from: Math.min(prev.from, from), end: Math.max(prev.end, end) } : { from, end });
      }
    }
    if (todo.size === 0) return;
    c.assign([...todo].map(([partition, r]) => ({ topic, partition, offset: r.from })));
    const deadline = Date.now() + timeoutMs;
    while (todo.size > 0) {
      if (Date.now() > deadline) throw new Error(`scan of ${topic} timed out with partitions ${[...todo.keys()].join(',')} unfinished`);
      const batch: any[] = await call((cb) => c.consume(500, cb));
      for (const m of batch) {
        const r = todo.get(m.partition);
        if (!r) continue;
        if (m.offset >= r.from && m.offset < r.end) {
          await onRecord({ topic, partition: m.partition, offset: String(m.offset), key: m.key ?? null, value: m.value ?? null, headers: headersOf(m.headers), timestamp: m.timestamp });
        }
        if (m.offset >= r.end - 1) todo.delete(m.partition);
      }
    }
  } finally {
    await call((cb) => c.disconnect(cb)).catch(() => undefined);
  }
}

/** The exact records at these (partition, offset) pairs. Missing ones (retention) are absent from the map. */
export async function readRecordsAt(cfg: KafkaClientConfig, topic: string, at: { partition: number; offset: number }[], opts: ScanOptions = {}): Promise<Map<string, ScannedRecord>> {
  const byPart = new Map<number, Set<number>>();
  for (const a of at) (byPart.get(a.partition) ?? byPart.set(a.partition, new Set()).get(a.partition)!).add(a.offset);
  const ranges = [...byPart].map(([partition, s]) => ({ partition, from: Math.min(...s), to: Math.max(...s) }));
  const out = new Map<string, ScannedRecord>();
  await scanTopic(cfg, topic, ranges, (r) => {
    if (byPart.get(r.partition)?.has(Number(r.offset))) out.set(`${r.partition}:${r.offset}`, r);
  }, opts);
  return out;
}
