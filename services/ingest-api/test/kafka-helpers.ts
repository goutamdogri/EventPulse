// Test helpers that read events.raw directly, to check what the API really wrote.
import { randomUUID } from 'node:crypto';
import { TOPIC_EVENTS_RAW, createKafka, loadKafkaClientConfig } from '@eventpulse/kafka-config';

export interface ReadRecord {
  partition: number;
  offset: bigint;
  key: string;
  value: any;
  headers: Record<string, string>;
}

export function testKafka() {
  return createKafka({ ...loadKafkaClientConfig(process.env, 'ingest-api-e2e'), logLevel: 'error' });
}

/** High-water mark (next offset to be written) of every partition of events.raw. */
export async function endOffsets(): Promise<Map<number, bigint>> {
  const admin = testKafka().admin();
  await admin.connect();
  try {
    const parts = await admin.fetchTopicOffsets(TOPIC_EVENTS_RAW);
    return new Map(parts.map((p) => [p.partition, BigInt(p.high)]));
  } finally {
    await admin.disconnect();
  }
}

export const totalOf = (m: Map<number, bigint>) => [...m.values()].reduce((a, b) => a + b, 0n);

/**
 * Reads events.raw from the given start offsets until `done(records)` is true or the timeout expires.
 * Uses a throw-away consumer group, seeks each partition to its start offset as soon as it is assigned,
 * and also filters by offset (in case some older records were fetched before the seek).
 */
export async function readFrom(
  start: Map<number, bigint>,
  done: (records: ReadRecord[]) => boolean,
  timeoutMs = 20_000,
): Promise<ReadRecord[]> {
  const consumer = testKafka().consumer({
    kafkaJS: { groupId: `ingest-api-e2e-${randomUUID()}`, fromBeginning: true, autoCommit: false },
  });
  const records: ReadRecord[] = [];
  await consumer.connect();
  try {
    await consumer.subscribe({ topics: [TOPIC_EVENTS_RAW] });
    const seeked = new Set<number>();
    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        const offset = BigInt(message.offset);
        if (offset < (start.get(partition) ?? 0n)) return;
        const headers: Record<string, string> = {};
        for (const [k, v] of Object.entries(message.headers ?? {})) headers[k] = String(v);
        records.push({
          partition,
          offset,
          key: message.key?.toString() ?? '',
          value: JSON.parse(message.value?.toString() ?? 'null'),
          headers,
        });
      },
    });
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline && !done(records)) {
      for (const tp of consumer.assignment()) {
        if (seeked.has(tp.partition)) continue;
        consumer.seek({ topic: TOPIC_EVENTS_RAW, partition: tp.partition, offset: String(start.get(tp.partition) ?? 0n) });
        seeked.add(tp.partition);
      }
      await new Promise((r) => setTimeout(r, 100));
    }
    return records;
  } finally {
    await consumer.disconnect();
  }
}
