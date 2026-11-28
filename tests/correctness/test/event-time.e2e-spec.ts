// S3-F8 end to end: late and out-of-order events are stored correctly, keeping both occurred_at and received_at.
// Everything goes through the real API (Avro on the topic: received_at travels IN the record), into real
// consumer-sink processes, on throw-away topics and the test database.
import { randomUUID } from 'node:crypto';
import { StoreProbe, createTopic, createTopicNamed, deleteTopic, sleep, spawnSink, waitFor, waitReady } from '../src/harness.js';
import type { SinkProcess } from '../src/harness.js';
import { postEvent, spawnApi, waitApiReady } from '../src/api-process.js';
import type { ApiProcess } from '../src/api-process.js';
import { FIXABLE, FIXABLE_FAULT, logTotal, runReplayCli } from '../src/dlq-replay.js';

const MIN = 60_000;
const H = 60 * MIN;
const DAY = 24 * H;

describe('S3-F8 late and out-of-order events (API -> Kafka -> sink)', () => {
  const store = new StoreProbe();
  const cleanups: (() => Promise<unknown>)[] = [];

  async function lane(opts: { tiers?: boolean; dlq?: boolean } = {}) {
    const id = randomUUID().slice(0, 8);
    const topic = await createTopic(3);
    const tenant = `time_${id}`;
    const apiKey = `k_${randomUUID()}`;
    const tiers = opts.tiers ? await Promise.all([1, 2, 3].map((n) => createTopicNamed(`${topic}-retry-${n}`, 3))) : [];
    const dlq = opts.dlq || opts.tiers ? await createTopicNamed(`${topic}-dlq`, 1) : undefined;
    const group = `time-e2e-${id}`;
    const api = spawnApi({ KAFKA_TOPIC: topic, API_KEYS: `${apiKey}:${tenant}`, IDEM_KEY_PREFIX: `t${id}` });
    const sinks: SinkProcess[] = [];
    const startSink = async (extra: Record<string, string> = {}) => {
      const s = spawnSink({
        CONSUMER_TOPIC: topic,
        CONSUMER_GROUP_ID: group,
        SINK_MAX_ROWS: '50',
        ...(dlq ? { SINK_DLQ_TOPIC: dlq, DLQ_SIZE_POLL_MS: '0' } : {}),
        ...(tiers.length ? { RETRY_TOPICS: tiers.join(','), RETRY_TIER_DELAYS_MS: '3000,3000,3000' } : {}),
        ...extra,
      });
      sinks.push(s);
      await waitReady(s);
      return s;
    };
    cleanups.push(async () => {
      await Promise.all([api.stop(), ...sinks.map((s) => s.stop())]);
      await Promise.all([topic, ...tiers, ...(dlq ? [dlq] : [])].map(deleteTopic));
      if (dlq) await store.pool.query('delete from dlq_meta where dlq_topic = $1', [dlq]);
      await store.cleanup(tenant, group);
    });
    await waitApiReady(api);
    const post = async (body: object) => {
      const t0 = Date.now();
      const r = await postEvent(api, apiKey, body);
      return { ...r, t0, t1: Date.now() };
    };
    const row = async (eventId: string) =>
      (await store.pool.query('select occurred_at, received_at, stored_at, payload from events where tenant_id = $1 and event_id = $2', [tenant, eventId])).rows[0];
    return { id, topic, tenant, api, dlq, startSink, post, row, sinks };
  }

  afterEach(async () => {
    for (const c of cleanups.splice(0)) await c().catch(() => undefined);
  });
  afterAll(async () => {
    await store.end();
  });

  const ev = (occurredAt: string, payload: Record<string, unknown> = {}) => ({ event_id: randomUUID(), type: 'time.e2e', occurred_at: occurredAt, schema_version: 1, payload });

  it('[G5.reorder-late] dataset D shuffled, 10% 1 min / 1 h / 24 h late, some sent seconds later: the table equals D exactly', async () => {
    const l = await lane();
    await l.startSink();
    const now = Date.now();
    const lateBy = [MIN, H, DAY];
    // D: 300 events. Every 10th is late (by occurred_at); a few use other zone offsets or microseconds (normalized).
    const D = Array.from({ length: 300 }, (_, i) => {
      const t = now - (i % 10 === 0 ? lateBy[(i / 10) % 3] : i * 10);
      const iso = new Date(t).toISOString();
      const sent = i % 25 === 1 ? iso.replace('Z', '789Z') /* microseconds: truncated */ : i % 25 === 2 ? new Date(t + 2 * H).toISOString().replace('Z', '+02:00') : iso;
      return { body: ev(sent, { i }), utc: iso };
    });
    const shuffled = [...D].sort(() => Math.random() - 0.5);
    const now_ = shuffled.slice(0, 270);
    const later = shuffled.slice(270); // actually sent later (arrive after newer ones are stored)
    const sentAt = new Map<string, { t0: number; t1: number }>();
    for (let i = 0; i < now_.length; i += 20) {
      for (const r of await Promise.all(now_.slice(i, i + 20).map((d) => l.post(d.body).then((x) => ({ x, d }))))) {
        expect(r.x.status).toBe(202);
        sentAt.set(r.d.body.event_id, r.x);
      }
    }
    await waitFor(async () => (await store.countTenant(l.tenant)) === 270, 30_000, 'first 270 stored');
    await sleep(2000);
    for (const d of later) {
      const r = await l.post(d.body);
      expect(r.status).toBe(202);
      sentAt.set(d.body.event_id, r);
    }
    await waitFor(async () => (await store.countTenant(l.tenant)) === 300, 30_000, 'all stored');

    const rows = (await store.pool.query('select event_id::text, occurred_at, received_at, payload from events where tenant_id = $1', [l.tenant])).rows;
    expect(rows).toHaveLength(D.length);
    const byId = new Map(rows.map((r) => [r.event_id, r]));
    for (const d of D) {
      const r = byId.get(d.body.event_id);
      expect(r, d.body.event_id).toBeDefined();
      expect(r.occurred_at.toISOString()).toBe(d.utc); // UTC, millisecond precision, exactly
      expect(r.payload).toEqual(d.body.payload);
      const s = sentAt.get(d.body.event_id)!;
      expect(r.received_at.getTime()).toBeGreaterThanOrEqual(s.t0 - 5); // server time of THAT request
      expect(r.received_at.getTime()).toBeLessThanOrEqual(s.t1 + 5);
    }
    // Late by event time is not "late" for the pipeline: stored regardless, and the lateness is visible.
    const lateRows = rows.filter((r) => r.received_at.getTime() - r.occurred_at.getTime() >= 23 * H);
    expect(lateRows.length).toBe(10);
  });

  it('[G5.retry-late] a record waits on a retry tier while NEWER events of the tenant are stored first: both stored, occurred_at correct, received_at the original', async () => {
    const l = await lane({ tiers: true });
    await l.startSink({ DEBUG_FAULTS: 'true', FAULT_ERRORS: 'consumer:before-insert=40001x*:marker=retry-late' });
    const old = ev(new Date(Date.now() - 2 * H).toISOString(), { tag: 'retry-late' });
    const sent = await l.post(old);
    expect(sent.status).toBe(202);
    const newer = Array.from({ length: 50 }, (_, i) => ev(new Date(Date.now() - i).toISOString(), { i }));
    for (const n of newer) expect((await l.post(n)).status).toBe(202);
    await waitFor(async () => (await store.countTenant(l.tenant)) === 50, 30_000, 'newer events stored first');
    expect(await l.row(old.event_id)).toBeUndefined(); // still on tier 1
    await waitFor(async () => (await store.countTenant(l.tenant)) === 51, 30_000, 'the retried record stored');

    const r = await l.row(old.event_id);
    expect(r.occurred_at.toISOString()).toBe(old.occurred_at);
    // received_at is when the API got it, not when the retry tier stored it.
    expect(r.received_at.getTime()).toBeGreaterThanOrEqual(sent.t0 - 5);
    expect(r.received_at.getTime()).toBeLessThanOrEqual(sent.t1 + 5);
    expect(r.stored_at.getTime() - r.received_at.getTime()).toBeGreaterThanOrEqual(1500); // waited half the tier delay at least
    const newest = (await store.pool.query('select min(stored_at) as s from events where tenant_id = $1 and event_id <> $2', [l.tenant, old.event_id])).rows[0].s as Date;
    expect(r.stored_at.getTime()).toBeGreaterThan(newest.getTime()); // stored AFTER newer events: order does not matter
  });

  it('[G5.replay-late] DLQ replay of an old event: stored with its ORIGINAL received_at (it does not look new)', async () => {
    const l = await lane({ dlq: true });
    const faulty = await l.startSink(FIXABLE_FAULT);
    const old = ev(new Date(Date.now() - 180 * DAY).toISOString(), { tag: FIXABLE }); // half a year late
    const sent = await l.post(old);
    expect(sent.status).toBe(202);
    await waitFor(async () => Number((await store.pool.query('select count(*) from dlq_meta where dlq_topic = $1', [l.dlq])).rows[0].count) === 1, 30_000, 'dead-lettered');
    await faulty.stop();
    await l.startSink(); // the cause is fixed
    await sleep(1500); // the replay happens clearly later than the original request
    const replayAt = Date.now();
    const r = await runReplayCli(['replay', '--dlq-topic', l.dlq!, '--execute', '--yes', '--rate', '0', '--json']);
    expect(r.code, r.stderr).toBe(0);
    expect(r.json.counts.replayed).toBe(1);
    await waitFor(async () => (await l.row(old.event_id)) !== undefined, 30_000, 'replayed event stored');
    const row = await l.row(old.event_id);
    expect(row.occurred_at.toISOString()).toBe(old.occurred_at);
    expect(row.received_at.getTime()).toBeGreaterThanOrEqual(sent.t0 - 5);
    expect(row.received_at.getTime()).toBeLessThanOrEqual(sent.t1 + 5);
    expect(row.received_at.getTime()).toBeLessThan(replayAt);
    expect(row.stored_at.getTime()).toBeGreaterThanOrEqual(replayAt);
  });

  it('[G5.skew] occurred_at 1 year + 1 day old -> 422 occurred_at_too_old, 10 min ahead -> 422 occurred_at_in_future, no zone -> 400; nothing produced', async () => {
    const l = await lane();
    const before = await logTotal(l.topic);
    const tooOld = await l.post(ev(new Date(Date.now() - 366 * DAY).toISOString()));
    expect(tooOld.status).toBe(422);
    expect(tooOld.body).toMatchObject({ reason: 'occurred_at_too_old' });
    const future = await l.post(ev(new Date(Date.now() + 10 * MIN).toISOString()));
    expect(future.status).toBe(422);
    expect(future.body).toMatchObject({ reason: 'occurred_at_in_future' });
    const noZone = await l.post(ev(new Date().toISOString().replace('Z', '')));
    expect(noZone.status).toBe(400);
    // Inside the bounds: 364 days old and 4 min ahead are accepted.
    expect((await l.post(ev(new Date(Date.now() - 364 * DAY).toISOString()))).status).toBe(202);
    expect((await l.post(ev(new Date(Date.now() + 4 * MIN).toISOString()))).status).toBe(202);
    expect((await logTotal(l.topic)) - before).toBe(2); // only the two accepted ones
    const metrics = await (await fetch(l.api.url('/metrics'))).text();
    expect(metrics).toMatch(/ep_event_rejected_total\{reason="too_old"\} 1/);
    expect(metrics).toMatch(/ep_event_rejected_total\{reason="future"\} 1/);
    expect(metrics).toMatch(/ep_event_lateness_seconds_count 2/);
    expect(metrics).toMatch(/ep_event_ahead_total 1/);
    // A rejected event did not consume its idempotency key: the corrected request is accepted with the same id.
    const fixed = ev(new Date().toISOString());
    expect((await l.post({ ...fixed, occurred_at: new Date(Date.now() + 10 * MIN).toISOString() })).status).toBe(422);
    expect((await l.post(fixed)).status).toBe(202);
  });

  it('the sink measures lateness of what it stores (ep_sink_lateness_seconds)', async () => {
    const l = await lane();
    const sink = await l.startSink();
    for (const late of [0, H - MIN, DAY - MIN]) expect((await l.post(ev(new Date(Date.now() - late).toISOString()))).status).toBe(202);
    await waitFor(async () => (await store.countTenant(l.tenant)) === 3, 30_000, 'stored');
    const text = await (await fetch(`http://localhost:${sink.port}/metrics`)).text();
    expect(text).toMatch(/ep_sink_lateness_seconds_count 3/);
    expect(text).toMatch(/ep_sink_lateness_seconds_bucket\{le="900"\} 1/); // the on-time one
    expect(text).toMatch(/ep_sink_lateness_seconds_bucket\{le="3600"\} 2/); // + the hour-late one
    expect(text).toMatch(/ep_sink_lateness_seconds_bucket\{le="86400"\} 3/); // + the day-late one
  });
});
