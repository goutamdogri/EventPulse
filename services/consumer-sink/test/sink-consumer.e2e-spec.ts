// S2-F2 end to end: records on events.raw (encoded exactly like the API does) -> consumer group -> Postgres.
// Each test file run uses its own consumer group, so it never disturbs a real sink-postgres consumer.
import { Test } from '@nestjs/testing';
import type { INestApplication } from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { Pool } from 'pg';
import {
  TOPIC_EVENTS_RAW,
  createKafka,
  encodeEvent,
  loadKafkaClientConfig,
} from '@eventpulse/kafka-config';
import type { KeyMode, Producer } from '@eventpulse/kafka-config';
import { AppModule } from '../src/app.module.js';
import { SinkConsumer } from '../src/consumer/sink-consumer.js';

const GROUP = `sink-e2e-${randomUUID()}`;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function startSink(): Promise<{ app: INestApplication; sink: SinkConsumer }> {
  process.env.CONSUMER_GROUP_ID = GROUP;
  // A brand-new group starts at the end of the log (no history replay in tests); once it has committed
  // offsets it resumes from them, which is what the restart test checks.
  process.env.CONSUMER_FROM_BEGINNING = 'false';
  process.env.CONSUMER_SESSION_TIMEOUT_MS = '10000';
  const mod = await Test.createTestingModule({ imports: [AppModule] }).compile();
  const app = mod.createNestApplication({ logger: false });
  await app.init(); // onApplicationBootstrap: connect, subscribe, run
  return { app, sink: app.get(SinkConsumer) };
}

async function waitFor(cond: () => Promise<boolean> | boolean, timeoutMs: number, what: string) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await cond()) return;
    await sleep(200);
  }
  throw new Error(`timed out waiting for ${what}`);
}

describe('consumer-sink: events.raw -> Postgres', () => {
  const pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 2 });
  let producer: Producer;
  let a: { app: INestApplication; sink: SinkConsumer };

  const makeEvents = (n: number) =>
    Array.from({ length: n }, () => ({
      event_id: randomUUID(),
      type: 'order.created',
      occurred_at: new Date().toISOString(),
      schema_version: 1,
      payload: { n: Math.random() },
    }));

  const produce = async (events: ReturnType<typeof makeEvents>, keyMode: KeyMode = 'event') => {
    const messages = events.map((event) => {
      const e = encodeEvent({ tenantId: 'sink_e2e', event, receivedAt: new Date(), requestId: 'e2e', keyMode });
      return { key: e.key, value: e.value, headers: e.headers };
    });
    for (let i = 0; i < messages.length; i += 500) {
      await producer.send({ topic: TOPIC_EVENTS_RAW, messages: messages.slice(i, i + 500) });
    }
  };

  const countRows = async (ids: string[]) =>
    Number((await pool.query('select count(*) from events where event_id = any($1::uuid[])', [ids])).rows[0].count);

  beforeAll(async () => {
    producer = createKafka({ ...loadKafkaClientConfig(process.env, 'sink-e2e-producer'), logLevel: 'error' }).producer({
      kafkaJS: { acks: -1, idempotent: true },
      'linger.ms': 0,
    } as any);
    await producer.connect();
    a = await startSink();
    await a.sink.waitForAssignment();
    await sleep(1500); // let the new group resolve its "latest" start position before we produce
  });

  afterAll(async () => {
    await a?.app.close();
    await producer?.disconnect();
    await pool.end();
  });

  it('writes 1000 events (all 6 partitions) to Postgres within a bounded delay', async () => {
    const events = makeEvents(1000);
    const ids = events.map((e) => e.event_id);
    const t0 = Date.now();
    await produce(events);
    await waitFor(async () => (await countRows(ids)) === 1000, 15_000, '1000 rows');
    const delayMs = Date.now() - t0;
    expect(delayMs).toBeLessThan(15_000);

    const r = (
      await pool.query(
        `select count(*) filter (where stored_at >= received_at) as fresh_ok,
                count(*) filter (where tenant_id = 'sink_e2e') as tenant_ok
           from events where event_id = any($1::uuid[])`,
        [ids],
      )
    ).rows[0];
    expect(Number(r.fresh_ok)).toBe(1000);
    expect(Number(r.tenant_ok)).toBe(1000);
    expect(a.sink.stats.assigned.size).toBe(6); // only member -> owns every partition
  });

  it('duplicate records: one row each, and the duplicates are counted', async () => {
    const events = makeEvents(50);
    const before = a.sink.stats.duplicates;
    await produce(events);
    await produce(events); // same event_ids again (e.g. a client retry)
    await waitFor(() => a.sink.stats.duplicates - before >= 50, 15_000, '50 duplicates counted');
    expect(await countRows(events.map((e) => e.event_id))).toBe(50);
  });

  it('a second member splits the partitions: none owned twice, all six owned', async () => {
    const b = await startSink();
    try {
      await waitFor(
        () => {
          const pa = [...a.sink.stats.assigned];
          const pb = [...b.sink.stats.assigned];
          return pa.length > 0 && pb.length > 0 && pa.length + pb.length === 6 && new Set([...pa, ...pb]).size === 6;
        },
        45_000,
        'both members to own disjoint partitions',
      );
      // And the group still delivers: events land while two members share the work.
      const events = makeEvents(300);
      await produce(events);
      await waitFor(async () => (await countRows(events.map((e) => e.event_id))) === 300, 15_000, '300 rows');
    } finally {
      await b.app.close();
    }
    // After b leaves, a takes every partition back.
    await waitFor(() => a.sink.stats.assigned.size === 6, 45_000, 'member a to own all 6 again');
  });

  it('restart resumes from committed offsets: events produced while down are written, nothing replayed', async () => {
    await a.app.close(); // graceful: finishes the batch, leaves the group
    const events = makeEvents(100);
    await produce(events);

    a = await startSink(); // same group: resumes at its committed offsets, not at "latest"
    await waitFor(async () => (await countRows(events.map((e) => e.event_id))) === 100, 30_000, '100 rows after restart');
    await sleep(1000);
    // A clean shutdown committed everything it wrote, so the new member consumed exactly the new 100.
    expect(a.sink.stats.consumed).toBe(100);
    expect(a.sink.stats.duplicates).toBe(0);
  });
});
