// S3-F3: duplicates are counted and visible at every layer, never silently swallowed.
//
// A real pipeline on throw-away topics and the TEST database: ingest-api (child process, Redis db 15) ->
// Kafka -> consumer-sink (child process). Each test checks that the copies it creates show up in exactly the
// counter of the layer that stopped them, and the last two run the real audit (tools/audit) against this
// isolated pipeline and require the unexplained difference (check J) to be 0.
import { spawnSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { postEvent, spawnApi, waitApiReady } from '../src/api-process.js';
import type { ApiProcess } from '../src/api-process.js';
import {
  StoreProbe,
  createTopic,
  deleteTopic,
  fixedEvents,
  fullyCommitted,
  produceFixed,
  resetGroupOffsets,
  sleep,
  spawnSink,
  waitFor,
  waitReady,
  withAdmin,
} from '../src/harness.js';
import type { FixedEvent, SinkProcess } from '../src/harness.js';

const N = 300;
const logTotal = async (topic: string) => (await withAdmin((a) => a.fetchTopicOffsets(topic))).reduce((s, p) => s + Number(p.high), 0);

describe('S3-F3 duplicates are counted at the layer that stopped them', () => {
  const store = new StoreProbe();
  const tenant = `dup_${randomUUID().slice(0, 8)}`;
  const apiKey = `key_${tenant}`;
  const group = `dup-${randomUUID()}`;
  let topic: string;
  let dlqTopic: string;
  let sink: SinkProcess;
  const apis: ApiProcess[] = [];
  const events: FixedEvent[] = fixedEvents(N, 'dup');

  const startApi = async (env: Record<string, string> = {}) => {
    const api = spawnApi({ KAFKA_TOPIC: topic, API_KEYS: `${apiKey}:${tenant}`, IDEM_KEY_PREFIX: `dup${randomUUID().slice(0, 6)}`, ...env });
    apis.push(api);
    await waitApiReady(api);
    return api;
  };
  const sendAll = async (api: ApiProcess, evs: FixedEvent[], key = (e: FixedEvent) => `k-${e.event_id}`) => {
    const statuses: Record<string, number> = {};
    for (const e of evs) {
      const r = await postEvent(api, apiKey, e, key(e));
      const s = `${r.status}${r.headers?.get('idempotent-replayed') ? '-replayed' : ''}`;
      statuses[s] = (statuses[s] ?? 0) + 1;
    }
    return statuses;
  };
  const drained = () => waitFor(async () => fullyCommitted(group, topic), 60_000, 'sink drained');
  const sinkStats = async () => sink.stats();

  beforeAll(async () => {
    topic = await createTopic(6);
    dlqTopic = await createTopic(3);
    sink = spawnSink({ CONSUMER_TOPIC: topic, CONSUMER_GROUP_ID: group, SINK_DLQ_TOPIC: dlqTopic, SINK_MAX_ROWS: '100' });
    await waitReady(sink);
  }, 120_000);

  afterAll(async () => {
    await Promise.all([...apis.map((a) => a.stop()), sink?.stop()]);
    await deleteTopic(topic);
    await deleteTopic(dlqTopic);
    await store.cleanup(tenant, group);
    await store.end();
  });

  it('[G9.replay-api] the same N events sent 3 times with the same keys: N in Kafka, 2N stopped at the API, 0 at the sink', async () => {
    const api = await startApi();
    for (let round = 0; round < 3; round++) {
      const st = await sendAll(api, events);
      expect(st, `round ${round}`).toEqual(round === 0 ? { '202': N } : { '202-replayed': N });
    }
    await drained();
    const a = await api.stats();
    const s = await sinkStats();
    expect(await logTotal(topic)).toBe(N);
    expect(a.idempotency.by_result).toMatchObject({ new: N, replayed: 2 * N });
    expect(s).toMatchObject({ inserted: N, duplicates: 0 });
    expect(await store.countTenant(tenant)).toBe(N);
    await api.stop();
  });

  it('[G9.redelivered] API dedup off: the same N twice more reach Kafka (3N) and are counted at the sink as redelivered (2N)', async () => {
    const before = await store.fingerprint(tenant);
    const api = await startApi({ IDEM_ENABLED: 'false' });
    for (let round = 0; round < 2; round++) expect(await sendAll(api, events)).toEqual({ '202': N });
    await drained();
    const s = await sinkStats();
    expect(await logTotal(topic)).toBe(3 * N);
    expect((await api.stats()).idempotency.by_result.disabled).toBe(2 * N);
    expect(s.duplicates_by_cause).toEqual({ reprocessed: 0, redelivered: 2 * N, unknown: 0 });
    expect(s.conflicting_duplicates).toBe(0);
    expect(await store.fingerprint(tenant)).toEqual(before); // storage unchanged
    await api.stop();
  });

  it('[G9.reprocessed] the log consumed again from the start: counted as reprocessed, not redelivered', async () => {
    const before = await store.fingerprint(tenant);
    await sink.stop();
    await resetGroupOffsets(group, topic, 'earliest');
    sink = spawnSink({ CONSUMER_TOPIC: topic, CONSUMER_GROUP_ID: group, SINK_DLQ_TOPIC: dlqTopic, SINK_MAX_ROWS: '100' });
    await waitReady(sink);
    await waitFor(async () => (await sinkStats()).consumed >= 3 * N && (await fullyCommitted(group, topic)), 60_000, 'replayed');
    const s = await sinkStats();
    expect(s.duplicates_by_cause).toEqual({ reprocessed: 3 * N, redelivered: 0, unknown: 0 });
    expect(s.reprocessed_messages).toBe(3 * N);
    expect(await store.fingerprint(tenant)).toEqual(before);
  });

  it('[G9.conflict] same (tenant, event_id) with other content: counted as a conflict, stored row untouched', async () => {
    const before = await store.fingerprint(tenant);
    const changed = { ...events[0], payload: { i: 'changed' } };
    await produceFixed(topic, tenant, [changed]);
    await waitFor(async () => (await sinkStats()).conflicting_duplicates === 1, 30_000, 'conflict counted');
    const m = await (await fetch(`http://localhost:${sink.port}/metrics`)).text();
    expect(m).toMatch(/ep_sink_conflicting_duplicates_total 1/);
    expect(await store.fingerprint(tenant)).toEqual(before);
  });

  it('[G9.metrics-present] every catalogue series exists on /metrics of both services', async () => {
    const api = await startApi();
    const am = await (await fetch(api.url('/metrics'))).text();
    for (const r of ['new', 'replayed', 'in_progress', 'payload_mismatch', 'fail_open', 'fail_closed', 'disabled']) {
      expect(am, r).toContain(`ep_idem_requests_total{result="${r}"}`);
    }
    for (const name of ['ep_idem_complete_failures_total', 'ep_idem_redis_latency_seconds', 'ep_idem_redis_errors_total']) expect(am).toContain(name);
    const sm = await (await fetch(`http://localhost:${sink.port}/metrics`)).text();
    for (const c of ['reprocessed', 'redelivered', 'unknown']) expect(sm, c).toContain(`ep_sink_duplicates_total{cause="${c}"}`);
    for (const name of ['ep_sink_rows_attempted_total', 'ep_sink_rows_inserted_total', 'ep_sink_reprocessed_messages_total', 'ep_sink_conflicting_duplicates_total']) {
      expect(sm).toContain(name);
    }
    const st = await sinkStats();
    expect(Object.keys(st)).toEqual(expect.arrayContaining(['duplicates_by_cause', 'reprocessed_messages', 'conflicting_duplicates']));
    await api.stop();
  });

  // --- The real audit (tools/audit) against this isolated pipeline -------------------------------------------------
  function audit(api: ApiProcess, env: Record<string, string>) {
    const results = mkdtempSync(join(tmpdir(), 'ep-audit-'));
    const r = spawnSync(process.execPath, [resolve(import.meta.dirname, '../../../tools/audit/src/cli.ts'), 'run'], {
      encoding: 'utf8',
      env: {
        ...process.env,
        AUDIT_TOPIC: topic,
        AUDIT_DLQ_TOPIC: dlqTopic,
        AUDIT_GROUP: group,
        AUDIT_DATABASE_URL: process.env.DATABASE_URL,
        API_STATS_URL: api.url('/debug/stats'),
        CONSUMER_STATS_URLS: `http://localhost:${sink.port}/debug/stats`,
        AUDIT_RESULTS_DIR: results,
        AUDIT_DRAIN_TIMEOUT_S: '60',
        BASE_URL: api.url(''),
        API_KEY: apiKey,
        RATE: '100',
        DURATION: '10s',
        ...env,
      },
      timeout: 180_000,
    });
    const runId = /run ([0-9a-f]{8})/.exec(r.stderr)?.[1];
    if (!runId) throw new Error(`audit did not start: ${r.stderr}\n${r.stdout}`);
    const result = JSON.parse(readFileSync(join(results, runId, 'audit-result.json'), 'utf8'));
    return { exit: r.status, stdout: r.stdout, result, check: (id: string) => result.checks.find((c: any) => c.id === id) };
  }

  it('[G9.audit-resend] audit with 20% resends: RECONCILED, resends stopped at the API (K), unexplained difference 0 (J)', async () => {
    const api = await startApi();
    const a = audit(api, { RESEND_FRACTION: '0.2' });
    console.info(a.stdout);
    expect(a.exit, a.stdout).toBe(0);
    expect(a.check('J')).toMatchObject({ status: 'ok' });
    expect(a.result.numbers.unexplained).toBe(0);
    expect(a.result.numbers.api_replayed_delta).toBeGreaterThan(0);
    expect(a.result.numbers.api_replayed_delta).toBe(a.result.numbers.replayed_202_k6);
    expect(a.check('B')).toMatchObject({ status: 'ok' });
    expect(a.check('D')).toMatchObject({ status: 'ok' }); // no extra copy reached Kafka
    await api.stop();
  }, 240_000);

  it('[G9.audit-redelivered] audit with resends and API dedup OFF: every extra copy is accounted for at the sink (J = 0)', async () => {
    const api = await startApi({ IDEM_ENABLED: 'false' });
    const a = audit(api, { RESEND_FRACTION: '0.2' });
    console.info(a.stdout);
    expect(a.exit, a.stdout).toBe(0);
    expect(a.check('D')).toMatchObject({ status: 'finding' }); // the resends are in the log
    expect(a.result.numbers.written_not_acked).toBe(a.result.numbers.resends_sent);
    expect(a.result.numbers.dup_redelivered).toBe(a.result.numbers.resends_sent);
    expect(a.check('J')).toMatchObject({ status: 'ok' });
    expect(a.result.numbers.unexplained).toBe(0);
    await api.stop();
    await sleep(100);
  }, 240_000);
});
