// S2-F4: poison messages must not block a partition forever.
// Real consumer-sink processes, real Kafka + Postgres; each test has its own source topic, DLQ topic and group.
// Poison records are written with scripts/produce-poison.sh (the same tool used by hand).
import { randomUUID } from 'node:crypto';
import {
  Db,
  createTopic,
  createTopicNamed,
  databaseUrlViaProxy,
  deleteTopic,
  dockerPg,
  fullyCommitted,
  groupOffsets,
  producePoison,
  produceEvents,
  readTopic,
  setPgProxy,
  sleep,
  spawnSink,
  waitFor,
  waitPgReady,
  waitReady,
} from './process-helpers.js';
import type { SinkProcess } from './process-helpers.js';

describe('S2-F4 poison messages (real process)', () => {
  const db = new Db();
  const topics: string[] = [];
  const procs: SinkProcess[] = [];

  async function setupTopics() {
    const topic = await createTopic(6);
    const dlq = await createTopicNamed(`${topic}-dlq`, 1);
    topics.push(topic, dlq);
    return { topic, dlq, group: `sink-e2e-${randomUUID()}` };
  }

  const start = (env: Record<string, string>) => {
    const p = spawnSink({ SINK_RETRY_BASE_MS: '50', SINK_RETRY_MAX_MS: '200', ...env });
    procs.push(p);
    return p;
  };

  afterEach(async () => {
    await Promise.all(procs.splice(0).map((p) => p.stop()));
  });

  afterAll(async () => {
    await setPgProxy(true).catch(() => undefined);
    await dockerPg('start').catch(() => undefined); // never leave Postgres stopped
    await waitPgReady().catch(() => undefined);
    await Promise.all(topics.map(deleteTopic));
    await db.end();
  });

  it.each([
    ['invalid-json', 'PERMANENT_PARSE', 'invalid_json'],
    ['bad-uuid', 'PERMANENT_PARSE', 'bad_event_id'],
    ['oversized-field', 'PERMANENT_PARSE', 'bad_type'],
    ['constraint', 'PERMANENT_DB', '22P05'],
  ])('%s in the middle of a busy partition: the partition drains, the record is in the DLQ', async (kind, errorClass, errorCode) => {
    const { topic, dlq, group } = await setupTopics();
    const key = `busy-${randomUUID()}`; // every record below lands in this key's partition
    const before = await produceEvents(topic, 1000, { key });
    await producePoison(kind, key, topic);
    const after = await produceEvents(topic, 1000, { key });

    const sink = start({ CONSUMER_TOPIC: topic, SINK_DLQ_TOPIC: dlq, CONSUMER_GROUP_ID: group, SINK_MAX_ROWS: '500' });
    await waitFor(
      async () => (await db.countRows([...before, ...after])) === 2000 && (await fullyCommitted(group, topic)),
      60_000,
      `2000 rows stored and every offset (poison included) committed\n${sink.log.join('').slice(-2000)}`,
    );

    const recs = await readTopic(dlq, 15_000, (r) => r.length >= 1);
    expect(recs).toHaveLength(1);
    const d = recs[0];
    expect(d.key).toBe(key);
    expect(d.headers).toMatchObject({
      'dlq.original_topic': topic,
      'dlq.consumer_group': group,
      'dlq.error_class': errorClass,
      'dlq.error_code': errorCode,
      'dlq.original_offset': '1000', // exactly the poison record: 1000 good ones came before it
      request_id: `poison-${kind}`, // original headers kept
    });
    // Stage 3: attempts counts retry-tier attempts (0 for a parse error); with tiers off a bad row is still retried
    // in-process SINK_MAX_RETRIES (3) times, recorded as 1 + 3 (the Stage 2 meaning).
    expect(d.headers['dlq.attempts']).toBe(errorClass === 'PERMANENT_DB' ? '4' : '0');
    if (kind === 'invalid-json') expect(d.value!.toString()).toContain('broken json'); // original bytes
    const st = await sink.stats();
    expect(st.dlq).toBe(1);
    expect(st.paused_partitions).toEqual([]);
  });

  it('Postgres unreachable for 20 s under load: zero records dead-lettered, everything stored after recovery', async () => {
    const { topic, dlq, group } = await setupTopics();
    const sink = start({
      CONSUMER_TOPIC: topic,
      SINK_DLQ_TOPIC: dlq,
      CONSUMER_GROUP_ID: group,
      DATABASE_URL: databaseUrlViaProxy(),
      PG_CONN_TIMEOUT_MS: '1000',
      PG_STATEMENT_TIMEOUT_MS: '3000',
      SINK_TRANSIENT_RETRY_WINDOW_MS: '3000',
      SINK_PAUSE_MS: '2000',
    });
    await waitReady(sink);
    let ids: string[] = [];
    try {
      await setPgProxy(false);
      ids = await produceEvents(topic, 1000);
      await sleep(20_000);
      const st = await sink.stats();
      expect(st.dlq).toBe(0);
      expect(st.errors_by_class.TRANSIENT_SYSTEMIC).toBeGreaterThan(0);
      expect(st.pauses).toBeGreaterThan(0); // paused, not a hot retry loop
      expect(await db.countRows(ids)).toBe(0);
    } finally {
      await setPgProxy(true);
    }
    await waitFor(async () => (await db.countRows(ids)) === 1000 && (await fullyCommitted(group, topic)), 60_000, '1000 rows after recovery');
    expect((await sink.stats()).dlq).toBe(0);
    expect(await readTopic(dlq, 3000)).toHaveLength(0);
  });

  it('Postgres container STOPPED for 30 s under load (plan 4.4): zero dead letters, all rows after restart', async () => {
    const { topic, dlq, group } = await setupTopics();
    const sink = start({
      CONSUMER_TOPIC: topic,
      SINK_DLQ_TOPIC: dlq,
      CONSUMER_GROUP_ID: group,
      PG_CONN_TIMEOUT_MS: '1000',
      PG_STATEMENT_TIMEOUT_MS: '3000',
      SINK_TRANSIENT_RETRY_WINDOW_MS: '3000',
      SINK_PAUSE_MS: '2000',
    });
    await waitReady(sink);
    const warm = await produceEvents(topic, 300); // load before the stop
    await waitFor(async () => (await db.countRows(warm)) === 300, 30_000, 'warm-up rows');

    let during: string[] = [];
    try {
      await dockerPg('stop'); // a real shutdown: connections are closed (57P01 / ECONNREFUSED / terminated)
      during = await produceEvents(topic, 1000); // load while Postgres is down
      await sleep(30_000);
      const st = await sink.stats();
      expect(st.dlq).toBe(0); // the DB being down is never the record's fault
      expect(st.errors_by_class.TRANSIENT_SYSTEMIC).toBeGreaterThan(0);
      expect(st.pauses).toBeGreaterThan(0);
    } finally {
      await dockerPg('start');
      await waitPgReady();
    }
    await waitFor(async () => (await db.countRows(during)) === 1000 && (await fullyCommitted(group, topic)), 90_000, '1000 rows after Postgres restarted');
    expect((await sink.stats()).dlq).toBe(0);
    expect(await readTopic(dlq, 3000)).toHaveLength(0);
  });

  it('SINK_DLQ_ENABLED=false reproduces the blocking: the poison partition stops, the others drain', async () => {
    const { topic, dlq, group } = await setupTopics();
    await produceEvents(topic, 600); // spread over all partitions
    const key = `busy-${randomUUID()}`;
    await produceEvents(topic, 300, { key });
    await producePoison('invalid-json', key, topic);
    const blocked = await produceEvents(topic, 300, { key });

    const sink = start({ CONSUMER_TOPIC: topic, SINK_DLQ_TOPIC: dlq, CONSUMER_GROUP_ID: group, SINK_DLQ_ENABLED: 'false' });
    // Five partitions drain completely; exactly one (the poison's) stays behind.
    await waitFor(
      async () => {
        const offs = await groupOffsets(group, topic);
        return offs.filter((p) => p.committed === p.end).length === 5;
      },
      60_000,
      'the 5 healthy partitions to drain',
    );
    await sleep(8000);
    const lagging = (await groupOffsets(group, topic)).filter((p) => p.committed !== p.end);
    expect(lagging).toHaveLength(1); // still stuck after 8 more seconds
    expect(await db.countRows(blocked)).toBe(0); // everything after the poison is invisible
    expect((await sink.stats()).batch_errors).toBeGreaterThan(0);
    expect(await readTopic(dlq, 3000)).toHaveLength(0);
  });
});
