// S3-F7: every event on events.raw uses a registered Avro schema; incompatible changes are rejected.
//
// Against the real Schema Registry (localhost:8081, `make schemas` registered events.raw-value) and a real pipeline
// on throw-away topics and the test database. Registry outages go through Toxiproxy (proxy "registry",
// localhost:18081, `make toxiproxy`).
import { randomUUID } from 'node:crypto';
import { RegistryClient, decodeWire, encodeWire, loadSubjects, schemasDir, subjectSpec } from '@eventpulse/schema';
import { checkAgainst, checkFixtures, checkHistory } from '../../../tools/schema-check/src/check.ts';
import { postEvent, spawnApi, waitApiReady } from '../src/api-process.js';
import type { ApiProcess } from '../src/api-process.js';
import {
  StoreProbe,
  createTopic,
  deleteTopic,
  fixedEvents,
  fullyCommitted,
  produceFixed,
  readTopic,
  sleep,
  spawnSink,
  waitFor,
  waitReady,
  withAdmin,
} from '../src/harness.js';
import type { SinkProcess } from '../src/harness.js';

const REGISTRY = process.env.SCHEMA_REGISTRY_URL ?? 'http://localhost:8081';
const VIA_PROXY = 'http://localhost:18081';
const TOXI = process.env.TOXI_URL ?? 'http://localhost:8474';
const SUBJECT = 'events.raw-value';
const setRegistryProxy = async (enabled: boolean) => {
  const r = await fetch(`${TOXI}/proxies/registry`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ enabled }) });
  if (!r.ok) throw new Error(`toxiproxy proxy "registry" missing (make toxiproxy): ${r.status}`);
};

describe('S3-F7 compatibility rules (real registry, throw-away subjects)', () => {
  const client = new RegistryClient({ url: REGISTRY, timeoutMs: 10_000 });
  const dir = schemasDir();
  const prefix = `e2e-${randomUUID().slice(0, 8)}`;

  it('[G10.history] the whole events.raw-value history is accepted under FULL_TRANSITIVE', async () => {
    const r = await checkHistory(client, subjectSpec(SUBJECT, dir), { prefix });
    expect(r).toMatchObject({ ok: true, level: 'FULL_TRANSITIVE' });
    expect(r.steps.map((s) => s.ok)).toEqual([true, true]);
  });

  it('[G10.fixtures] every incompatible fixture is rejected, the compatible one accepted; BACKWARD alone would let a field removal through', async () => {
    const fx = await checkFixtures(client, subjectSpec(SUBJECT, dir), dir, prefix);
    expect(fx.length).toBeGreaterThanOrEqual(5);
    expect(fx.filter((f) => !f.pass)).toEqual([]);
    const byName = Object.fromEntries(fx.map((f) => [f.file.split('/').pop(), f]));
    expect(byName['rename-without-alias.avsc'].verdict).toBe(false);
    expect(byName['type-change.avsc'].verdict).toBe(false);
    expect(byName['required-field-without-default.avsc'].verdict).toBe(false);
    expect(byName['remove-field-without-default.avsc']).toMatchObject({ verdict: false, backward: true }); // the policy argument
    expect(byName['add-optional-field.avsc'].verdict).toBe(true);
  });

  it('throw-away subjects are removed again', async () => {
    const r = await fetch(`${REGISTRY}/subjects`);
    expect(((await r.json()) as string[]).filter((s) => s.startsWith(prefix))).toEqual([]);
  });

  it('the newest local version is compatible with the REAL registered subject (what a deploy would register)', async () => {
    for (const spec of loadSubjects(dir)) expect((await checkAgainst(client, spec)).ok).toBe(true);
  });
});

describe('S3-F7 pipeline: Avro on events.raw (API -> Kafka -> sink)', () => {
  const store = new StoreProbe();
  const tenant = `schema_${randomUUID().slice(0, 8)}`;
  const apiKey = `key_${tenant}`;
  const group = `schema-${randomUUID()}`;
  let topic: string;
  let dlqTopic: string;
  const apis: ApiProcess[] = [];
  const sinks: SinkProcess[] = [];
  const startApi = async (env: Record<string, string> = {}) => {
    const a = spawnApi({ KAFKA_TOPIC: topic, API_KEYS: `${apiKey}:${tenant}`, IDEM_KEY_PREFIX: `sc${randomUUID().slice(0, 6)}`, ...env });
    apis.push(a);
    await waitApiReady(a);
    return a;
  };
  const startSink = async (env: Record<string, string> = {}) => {
    const s = spawnSink({ CONSUMER_TOPIC: topic, CONSUMER_GROUP_ID: group, SINK_DLQ_TOPIC: dlqTopic, SINK_MAX_ROWS: '100', ...env });
    sinks.push(s);
    await waitReady(s);
    return s;
  };
  const ev = (i: number) => ({ event_id: randomUUID(), type: 'schema.e2e', occurred_at: '2026-11-28T09:00:00.000Z', schema_version: 1, payload: { i, note: 'é' } });

  beforeAll(async () => {
    topic = await createTopic(6);
    dlqTopic = await createTopic(3);
  }, 120_000);

  afterAll(async () => {
    await setRegistryProxy(true).catch(() => undefined);
    await Promise.all([...apis.map((a) => a.stop()), ...sinks.map((s) => s.stop())]);
    await deleteTopic(topic);
    await deleteTopic(dlqTopic);
    await store.cleanup(tenant, group);
    await store.end();
  });

  it('[G10.avro-e2e] the API writes Avro with the registered id; the sink stores the same rows; old JSON records in the same topic too', async () => {
    // Stage 2 JSON records first (replaying old data must keep working), then Avro from the API.
    await produceFixed(topic, tenant, fixedEvents(50, 'legacy-json'));
    const api = await startApi();
    for (let i = 0; i < 100; i++) expect((await postEvent(api, apiKey, ev(i))).status).toBe(202);
    const registeredId = (await api.stats()).schema.writer_schema_id;
    expect(registeredId).toBeGreaterThan(0);
    // Every API record in the log is the wire format with that id.
    const recs = await readTopic(topic, 20_000, (r) => r.length >= 150);
    const avro = recs.filter((r) => r.value && r.value[0] === 0);
    expect(avro).toHaveLength(100);
    expect(new Set(avro.map((r) => decodeWire(r.value!).schemaId))).toEqual(new Set([registeredId]));
    expect(avro.every((r) => r.headers['content-type'] === 'application/vnd.eventpulse.event+avro')).toBe(true);

    const sink = await startSink();
    await waitFor(async () => (await store.countTenant(tenant)) === 150 && (await fullyCommitted(group, topic)), 60_000, '150 rows');
    const st = await sink.stats();
    expect(st.schema.ids_in_use).toEqual([registeredId]);
    expect(st.schema.deserialize_failures).toEqual({});
    const row = (await store.pool.query(`select payload, schema_version, received_at from events where tenant_id = $1 and type = 'schema.e2e' and payload->>'i' = '7'`, [tenant])).rows[0];
    expect(row).toMatchObject({ payload: { i: 7, note: 'é' }, schema_version: 1 });
  });

  it('[G10.unknown-id] a record with an unregistered schema id is dead-lettered (permanent), counted, and does not block the partition', async () => {
    const dlqBefore = (await withAdmin((a) => a.fetchTopicOffsets(dlqTopic))).reduce((s, p) => s + Number(p.high), 0);
    const producer = (await import('@eventpulse/kafka-config')).createKafka({ ...(await import('@eventpulse/kafka-config')).loadKafkaClientConfig(process.env, 'schema-e2e'), logLevel: 'error' }).producer({ kafkaJS: { acks: -1 } } as any);
    await producer.connect();
    await producer.send({ topic, messages: [{ key: 'x', value: encodeWire(999_999, Buffer.from([2, 0x61])), headers: { received_at: new Date().toISOString() } }] });
    await producer.disconnect();
    const api = apis[0];
    expect((await postEvent(api, apiKey, ev(1000))).status).toBe(202); // a good record after it
    await waitFor(async () => (await store.countTenant(tenant)) === 151 && (await fullyCommitted(group, topic)), 60_000, 'good record stored after the bad one');
    const st = await sinks[0].stats();
    expect(st.schema.deserialize_failures).toEqual({ unknown_schema_id: 1 });
    const dlqAfter = (await withAdmin((a) => a.fetchTopicOffsets(dlqTopic))).reduce((s, p) => s + Number(p.high), 0);
    expect(dlqAfter - dlqBefore).toBe(1);
    const dl = (await readTopic(dlqTopic, 15_000, (r) => r.length >= 1)).at(-1)!;
    expect(dl.headers['dlq.error_code']).toBe('unknown_schema_id');
  });

  it('[G10.registry-down-producer] registry down AFTER the API started: the API keeps answering 202 (cached id, no call)', async () => {
    await setRegistryProxy(true);
    const api = await startApi({ SCHEMA_REGISTRY_URL: VIA_PROXY });
    try {
      await setRegistryProxy(false);
      for (let i = 0; i < 20; i++) expect((await postEvent(api, apiKey, ev(2000 + i))).status).toBe(202);
      expect((await api.stats()).schema.ready).toBe(true);
    } finally {
      await setRegistryProxy(true);
      await api.stop();
    }
  });

  it('[G10.registry-down-startup] registry down at API startup: /readyz 503 and requests 503 schema_unavailable; ready once it is back', async () => {
    await setRegistryProxy(false);
    const api = spawnApi({ KAFKA_TOPIC: topic, API_KEYS: `${apiKey}:${tenant}`, SCHEMA_REGISTRY_URL: VIA_PROXY, SCHEMA_REGISTRY_TIMEOUT_MS: '500', SCHEMA_INIT_RETRY_MAX_MS: '500' });
    apis.push(api);
    try {
      await waitFor(async () => {
        try {
          return (await fetch(api.url('/healthz'))).ok;
        } catch {
          return false;
        }
      }, 30_000, 'api listening');
      const ready = await fetch(api.url('/readyz'));
      expect(ready.status).toBe(503);
      expect(await ready.json()).toMatchObject({ schema: 'unavailable' });
      const r = await postEvent(api, apiKey, ev(3000));
      expect(r.status).toBe(503);
      expect(r.body).toMatchObject({ reason: 'schema_unavailable' });
      await setRegistryProxy(true);
      await waitApiReady(api, 20_000);
      expect((await postEvent(api, apiKey, ev(3001))).status).toBe(202);
    } finally {
      await setRegistryProxy(true);
      await api.stop();
    }
  });

  it('[G10.registry-down-consumer] registry down for a FRESH sink (writer id not cached): partition paused, nothing dead-lettered, stored once it is back', async () => {
    await Promise.all(sinks.splice(0).map((s) => s.stop()));
    const rowsBefore = await store.countTenant(tenant);
    const dlqBefore = (await withAdmin((a) => a.fetchTopicOffsets(dlqTopic))).reduce((s, p) => s + Number(p.high), 0);
    const api = apis[0];
    for (let i = 0; i < 30; i++) expect((await postEvent(api, apiKey, ev(4000 + i))).status).toBe(202);
    await setRegistryProxy(false);
    try {
      const sink = await startSink({ SCHEMA_REGISTRY_URL: VIA_PROXY, SCHEMA_REGISTRY_TIMEOUT_MS: '500', SINK_PAUSE_MS: '1000' });
      await waitFor(async () => (await sink.stats()).schema.registry_unavailable > 0, 30_000, 'registry failure seen');
      await sleep(2_000);
      expect(await store.countTenant(tenant)).toBe(rowsBefore);
      expect((await withAdmin((a) => a.fetchTopicOffsets(dlqTopic))).reduce((s, p) => s + Number(p.high), 0)).toBe(dlqBefore);
      expect((await sink.stats()).chunk_failures_by_reason.registry_unavailable).toBeGreaterThan(0);
      await setRegistryProxy(true);
      await waitFor(async () => (await store.countTenant(tenant)) === rowsBefore + 30 && (await fullyCommitted(group, topic)), 60_000, 'stored after the registry came back');
      expect((await withAdmin((a) => a.fetchTopicOffsets(dlqTopic))).reduce((s, p) => s + Number(p.high), 0)).toBe(dlqBefore);
    } finally {
      await setRegistryProxy(true);
    }
  });
});
