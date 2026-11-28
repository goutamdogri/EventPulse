// S3-F5 / G6: a failing message cannot block a partition indefinitely and is never silently dropped: it is retried
// within limits (retry tiers with not_before), then quarantined with metadata (events.dlq + dlq_meta). A dependency
// that is down pauses the partition instead (never retry traffic, never the DLQ).
//
// Real consumer-sink processes (main + retry-tier consumer in one process), throw-away source, retry and DLQ topics,
// the test database. Failures are injected with FAULT_ERRORS on rows whose payload carries a marker.
import { randomUUID } from 'node:crypto';
import {
  StoreProbe,
  createTopic,
  createTopicNamed,
  databaseUrlViaProxy,
  deleteTopic,
  fullyCommitted,
  producePoison,
  produceFixed,
  readTopic,
  setPgProxy,
  sleep,
  spawnSink,
  waitFor,
  waitReady,
  withAdmin,
} from '../src/harness.js';
import type { FixedEvent, SinkProcess } from '../src/harness.js';

const DELAYS = [2000, 3000, 4000];
const total = async (topic: string) => (await withAdmin((a) => a.fetchTopicOffsets(topic))).reduce((s, p) => s + Number(p.high), 0);
const ev = (tag: string, i: number): FixedEvent => ({ event_id: randomUUID(), type: 'retry.e2e', occurred_at: '2026-11-28T09:00:00.000Z', schema_version: 1, payload: { i, tag } });

describe('S3-F5 retry tiers and DLQ (real process)', () => {
  const store = new StoreProbe();
  const procs: SinkProcess[] = [];
  const topics: string[] = [];
  const tenants: string[] = [];

  /** Fresh source + 3 tiers + DLQ topics and a group for one test. */
  async function lane() {
    const source = await createTopic(3);
    const id = randomUUID().slice(0, 8);
    const tiers = await Promise.all([1, 2, 3].map((n) => createTopicNamed(`${source}-retry-${n}`, 3)));
    const dlq = await createTopicNamed(`${source}-dlq`, 1);
    topics.push(source, ...tiers, dlq);
    const tenant = `retry_${id}`;
    tenants.push(tenant);
    const group = `retry-e2e-${id}`;
    const env = (extra: Record<string, string> = {}) => ({
      CONSUMER_TOPIC: source,
      CONSUMER_GROUP_ID: group,
      SINK_DLQ_TOPIC: dlq,
      RETRY_TOPICS: tiers.join(','),
      RETRY_TIER_DELAYS_MS: DELAYS.join(','),
      SINK_MAX_ROWS: '100',
      SINK_RETRY_BASE_MS: '20',
      SINK_RETRY_MAX_MS: '100',
      DLQ_SIZE_POLL_MS: '500',
      ...extra,
    });
    const start = async (extra: Record<string, string> = {}) => {
      const p = spawnSink(env(extra));
      procs.push(p);
      await waitReady(p);
      return p;
    };
    const dlqMeta = async () => (await store.pool.query('select * from dlq_meta where dlq_topic = $1 order by id', [dlq])).rows;
    return { source, tiers, dlq, tenant, group, start, dlqMeta };
  }

  afterEach(async () => {
    await setPgProxy(true).catch(() => undefined);
    await Promise.all(procs.splice(0).map((p) => p.stop()));
  });

  afterAll(async () => {
    await Promise.all(topics.map(deleteTopic));
    for (const t of tenants) await store.cleanup(t);
    await store.end();
  });

  it('[G6.permanent] an unparseable record: one DLQ record + one dlq_meta row with full metadata, no retry traffic', async () => {
    const l = await lane();
    await l.start();
    await producePoison('invalid-json', 'k', l.source);
    await waitFor(async () => (await l.dlqMeta()).length === 1, 30_000, 'dlq_meta row');
    const [m] = await l.dlqMeta();
    expect(m).toMatchObject({ source_topic: l.source, failed_topic: l.source, dlq_topic: l.dlq, error_class: 'PERMANENT_PARSE', error_code: 'invalid_json', attempts: 0, tenant_id: null, replayed_at: null });
    expect(Number(m.dlq_offset)).toBe(0);
    const [rec] = await readTopic(l.dlq, 10_000, (r) => r.length >= 1);
    expect(rec.headers['dlq.original_offset']).toBe(String(m.source_offset));
    for (const t of l.tiers) expect(await total(t)).toBe(0);
  });

  it('[G6.transient-recovers] a row failing with 40001 on the main path is stored from tier 1 after its delay; the records behind it do not wait', async () => {
    const l = await lane();
    const sink = await l.start({ DEBUG_FAULTS: 'true', FAULT_ERRORS: 'consumer:before-insert=40001x*:marker=retry-me' });
    const bad = ev('retry-me', 0);
    const healthy = Array.from({ length: 200 }, (_, i) => ev('ok', i + 1));
    await produceFixed(l.source, l.tenant, [bad, ...healthy]);
    // The 200 healthy records are stored at once, while the bad one waits on tier 1.
    await waitFor(async () => (await store.countTenant(l.tenant)) === 200, 30_000, 'healthy records stored');
    expect((await store.pool.query('select 1 from events where tenant_id = $1 and event_id = $2', [l.tenant, bad.event_id])).rowCount).toBe(0);
    // Then the retry tier stores it.
    await waitFor(async () => (await store.countTenant(l.tenant)) === 201, 30_000, 'retried record stored');
    const [r1] = await readTopic(l.tiers[0], 10_000, (r) => r.length >= 1);
    expect(r1.headers).toMatchObject({ 'retry.attempt': '1', 'retry.original_topic': l.source, 'retry.last_error_code': '40001', 'retry.last_error_class': 'TRANSIENT_RECORD' });
    const stored = (await store.pool.query('select stored_at from events where tenant_id = $1 and event_id = $2', [l.tenant, bad.event_id])).rows[0].stored_at as Date;
    expect(stored.getTime() - Date.parse(r1.headers['retry.first_failed_at'])).toBeGreaterThanOrEqual(DELAYS[0] / 2);
    const st = await sink.stats();
    expect(st.retry.published_by_tier).toEqual({ 1: 1 });
    expect(st.retry.processed_by_tier).toEqual({ 1: { stored: 1 } });
    expect(await l.dlqMeta()).toEqual([]);
    expect(st.consumed).toBe(st.inserted + st.duplicates + st.dlq + st.retried);
  });

  it('[G6.never-recovers] a row that keeps failing: tiers 1, 2, 3, then the DLQ with attempts 3, origin = the source record', async () => {
    const l = await lane();
    const sink = await l.start({ DEBUG_FAULTS: 'true', FAULT_ERRORS: 'consumer:before-insert=40001x*:marker=never,retry:before-insert=40001x*:marker=never' });
    const bad = ev('never', 0);
    await produceFixed(l.source, l.tenant, [bad, ev('ok', 1)]);
    await waitFor(async () => (await l.dlqMeta()).length === 1, 60_000, 'dead-lettered after the last tier');
    const [m] = await l.dlqMeta();
    expect(m).toMatchObject({ source_topic: l.source, source_partition: expect.any(Number), failed_topic: l.tiers[2], error_class: 'TRANSIENT_RECORD', error_code: '40001', attempts: 3, tenant_id: l.tenant, event_id: bad.event_id });
    expect(m.last_failed_at.getTime() - m.first_failed_at.getTime()).toBeGreaterThanOrEqual((DELAYS[0] + DELAYS[1] + DELAYS[2]) / 2);
    for (const t of l.tiers) expect(await total(t)).toBe(1); // exactly one record per tier
    const st = await sink.stats();
    expect(st.retry.processed_by_tier).toEqual({ 1: { next_tier: 1 }, 2: { next_tier: 1 }, 3: { dlq: 1 } });
    expect(await store.countTenant(l.tenant)).toBe(1); // the healthy one
    await waitFor(async () => (await sink.stats()).dlq_size?.unreplayed >= 1, 10_000, 'ep_dlq_size');
  });

  it('[G6.outage] Postgres unreachable for 15 s: the partition pauses; ZERO retry or DLQ records; everything stored after', async () => {
    const l = await lane();
    await l.start({ DATABASE_URL: databaseUrlViaProxy(), PG_CONN_TIMEOUT_MS: '500', PG_STATEMENT_TIMEOUT_MS: '2000', SINK_TRANSIENT_RETRY_WINDOW_MS: '1000', SINK_PAUSE_MS: '1000' });
    try {
      await setPgProxy(false);
      await produceFixed(l.source, l.tenant, Array.from({ length: 300 }, (_, i) => ev('outage', i)));
      await sleep(15_000);
      for (const t of [...l.tiers, l.dlq]) expect(await total(t)).toBe(0);
      expect(await store.countTenant(l.tenant)).toBe(0);
      await setPgProxy(true);
      await waitFor(async () => (await store.countTenant(l.tenant)) === 300 && (await fullyCommitted(l.group, l.source)), 60_000, 'stored after the outage');
      for (const t of [...l.tiers, l.dlq]) expect(await total(t)).toBe(0);
    } finally {
      await setPgProxy(true);
    }
  });

  it('[G6.rate-limit] more retriable rows than RETRY_MAX_RATE_PER_S: the partition is paused instead of flooding the tiers', async () => {
    const l = await lane();
    const sink = await l.start({ DEBUG_FAULTS: 'true', FAULT_ERRORS: 'consumer:before-insert=40001x*:marker=storm', RETRY_MAX_RATE_PER_S: '2', SINK_PAUSE_MS: '1000', RETRY_INPROCESS_ATTEMPTS: '0' });
    await produceFixed(l.source, l.tenant, Array.from({ length: 20 }, (_, i) => ev('storm', i)));
    await waitFor(async () => (await sink.stats()).retry.rate_limited > 0, 30_000, 'rate limit hit');
    const st = await sink.stats();
    expect(st.chunk_failures_by_reason.retry_rate_limited).toBeGreaterThan(0);
    expect(await total(l.tiers[0])).toBeLessThan(20); // bounded, not the whole storm at once
  });

  it('[G6.poison-in-batch] 1 poison record among 500: 499 stored, 1 dead letter, no retry traffic for the good rows', async () => {
    const l = await lane();
    const sink = await l.start({ SINK_MAX_ROWS: '500' });
    const good = Array.from({ length: 499 }, (_, i) => ev('batch', i));
    await produceFixed(l.source, l.tenant, good.slice(0, 250));
    await producePoison('constraint', 'k', l.source); // \u0000 in jsonb: Postgres rejects that one row (22P05)
    await produceFixed(l.source, l.tenant, good.slice(250));
    await waitFor(async () => (await store.countTenant(l.tenant)) === 499 && (await l.dlqMeta()).length === 1, 60_000, '499 rows + 1 dead letter');
    expect((await l.dlqMeta())[0]).toMatchObject({ error_class: 'PERMANENT_DB', error_code: '22P05', attempts: 0 });
    for (const t of l.tiers) expect(await total(t)).toBe(0);
    expect((await sink.stats()).isolations).toBeGreaterThan(0);
  });
});
