// Shared harness for the correctness suite. Process control, topics and producing come from consumer-sink's
// e2e helpers (one implementation); this file adds what the Stage 3 tests need on top: group offset resets,
// a fingerprint of the stored data, and per-tenant counts.
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import { createKafka, encodeEvent, loadKafkaClientConfig } from '@eventpulse/kafka-config';

export * from '../../../services/consumer-sink/test/process-helpers.js';

const run = promisify(execFile);
const KAFKA_CONTAINER = process.env.KAFKA_CONTAINER ?? 'ep-kafka1';
const INTERNAL_BOOTSTRAP = process.env.KAFKA_INTERNAL_BOOTSTRAP ?? 'kafka1:19092';

/**
 * Moves a consumer group's committed offsets (kafka-consumer-groups.sh inside the broker container). The group
 * must have no active member: stop the sink first. Retried because a member that just left can still be seen for
 * a moment. `to` = 'earliest', or one offset per partition.
 */
export async function resetGroupOffsets(group: string, topic: string, to: 'earliest' | Record<number, number>): Promise<void> {
  const base = ['exec', KAFKA_CONTAINER, '/opt/kafka/bin/kafka-consumer-groups.sh', '--bootstrap-server', INTERNAL_BOOTSTRAP, '--group', group, '--reset-offsets', '--execute'];
  const commands =
    to === 'earliest'
      ? [[...base, '--topic', topic, '--to-earliest']]
      : Object.entries(to).map(([p, o]) => [...base, '--topic', `${topic}:${p}`, '--to-offset', String(o)]);
  for (const args of commands) {
    let last: unknown;
    for (let attempt = 0; attempt < 20; attempt++) {
      try {
        const { stdout, stderr } = await run('docker', args);
        if (/Error|inactive/i.test(stderr) && !/NEW-OFFSET/.test(stdout)) throw new Error(stderr.trim());
        last = undefined;
        break;
      } catch (e) {
        last = e;
        await new Promise((r) => setTimeout(r, 1000));
      }
    }
    if (last) throw new Error(`reset offsets of ${group} failed: ${(last as Error).message}`);
  }
}

export interface Fingerprint {
  rows: number;
  distinct: number;
  /** md5 over every stored column that a duplicate could change, in a fixed order. */
  md5: string;
}

export class StoreProbe {
  readonly pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 2 });
  constructor() {
    this.pool.on('error', () => undefined);
  }

  /** Fingerprint of everything stored for a tenant. Equal fingerprints = "stored data unchanged" (S3-F2). */
  async fingerprint(tenant: string): Promise<Fingerprint> {
    const r = await this.pool.query(
      `select count(*)::int as rows, count(distinct event_id)::int as distinct,
              md5(coalesce(string_agg(
                event_id::text || '|' || type || '|' || occurred_at::text || '|' || received_at::text || '|' ||
                stored_at::text || '|' || schema_version::text || '|' || payload::text || '|' ||
                encode(coalesce(content_hash, ''::bytea), 'hex'),
                ',' order by event_id), '')) as md5
         from events where tenant_id = $1`,
      [tenant],
    );
    return r.rows[0];
  }

  async countTenant(tenant: string): Promise<number> {
    return Number((await this.pool.query('select count(*) from events where tenant_id = $1', [tenant])).rows[0].count);
  }

  async sinkOffsets(group: string, topic: string): Promise<Record<number, number>> {
    const r = await this.pool.query('select partition, max_offset from sink_offsets where consumer_group = $1 and topic = $2', [group, topic]);
    return Object.fromEntries(r.rows.map((x) => [x.partition, Number(x.max_offset)]));
  }

  async cleanup(tenant: string, group?: string) {
    await this.pool.query('delete from events where tenant_id = $1', [tenant]);
    if (group) await this.pool.query('delete from sink_offsets where consumer_group = $1', [group]);
  }

  end() {
    return this.pool.end();
  }
}

export interface FixedEvent {
  event_id: string;
  type: string;
  occurred_at: string;
  schema_version: number;
  payload: Record<string, unknown>;
}

/** N events with FIXED content (so producing them again is a true duplicate, not a conflicting one). */
export const fixedEvents = (n: number, tag = 'fixed'): FixedEvent[] =>
  Array.from({ length: n }, (_, i) => ({ event_id: randomUUID(), type: `${tag}.event`, occurred_at: '2026-11-27T10:00:00.000Z', schema_version: 1, payload: { i } }));

/** Produce exactly these events (encoded like the API does), keyed by event_id; returns when Kafka acknowledged all. */
export async function produceFixed(topic: string, tenant: string, events: FixedEvent[]): Promise<void> {
  const producer = createKafka({ ...loadKafkaClientConfig(process.env, 'correctness-fixed'), logLevel: 'error' }).producer({ kafkaJS: { acks: -1, idempotent: true }, 'linger.ms': 0 } as any);
  await producer.connect();
  try {
    for (let i = 0; i < events.length; i += 500) {
      const messages = events.slice(i, i + 500).map((event) => {
        const e = encodeEvent({ tenantId: tenant, event, receivedAt: new Date('2026-11-27T10:00:01.000Z'), requestId: 'fixed', keyMode: 'event' });
        return { key: e.key, value: e.value, headers: e.headers };
      });
      await producer.send({ topic, messages });
    }
  } finally {
    await producer.disconnect();
  }
}
