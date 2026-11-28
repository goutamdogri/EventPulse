// One crash scenario per component. The crash matrix iterates over CRASH_POINTS (packages/fault-injector) and
// looks up the component's scenario here, so a point whose component has no scenario fails the build
// (crash-matrix.spec.ts). Later Stage 3 steps add relay and admin scenarios.
import { randomUUID } from 'node:crypto';
import type { CrashPointInfo } from '@eventpulse/fault-injector';
import { createKafka, loadKafkaClientConfig } from '@eventpulse/kafka-config';
import {
  StoreProbe,
  createTopic,
  createTopicNamed,
  fixedEvents,
  produceFixed,
  deleteTopic,
  fullyCommitted,
  groupOffsets,
  produceEvents,
  sleep,
  spawnSink,
  totalCommitted,
  waitFor,
  withAdmin,
} from '../harness.js';
import type { SinkProcess } from '../harness.js';
import type { CrashResult } from './report.js';
import { postEvent, spawnApi, waitApiReady } from '../api-process.js';
import type { ApiProcess } from '../api-process.js';
import { dlqLane, runReplayCli } from '../dlq-replay.js';

export type Scenario = (point: CrashPointInfo, store: StoreProbe) => Promise<CrashResult>;

/** Consumer: N events over 6 partitions, chunks of CHUNK rows, crash on the 4th batch (batch 1 is committed). */
export const CONSUMER_N = 5000;
export const CONSUMER_CHUNK = 500;

async function consumerScenario(point: CrashPointInfo, store: StoreProbe): Promise<CrashResult> {
  const topic = await createTopic(6);
  const group = `crash-matrix-${randomUUID()}`;
  const tenant = `crash_${randomUUID().slice(0, 8)}`;
  const procs: SinkProcess[] = [];
  try {
    await produceEvents(topic, CONSUMER_N, { tenant });
    const common = { CONSUMER_TOPIC: topic, CONSUMER_GROUP_ID: group, SINK_MAX_ROWS: String(CONSUMER_CHUNK), CONSUMER_SESSION_TIMEOUT_MS: '10000' };

    const crashing = spawnSink({ ...common, DEBUG_FAULTS: 'true', FAULTS: `${point.name}@b4` });
    procs.push(crashing);
    const exit = await Promise.race([crashing.exited, sleep(60_000).then(() => null)]);
    const crashed = exit?.signal === 'SIGKILL' && crashing.log.join('').includes(`CRASH_AT=${point.name}`);
    if (!crashed) throw new Error(`${point.name}: process did not crash there (exit ${JSON.stringify(exit)})\n${crashing.log.join('').slice(-3000)}`);
    const storedAtCrash = await store.countTenant(tenant);
    const committedAtCrash = await totalCommitted(group, topic);

    const t0 = Date.now();
    const recovered = spawnSink(common);
    procs.push(recovered);
    await waitFor(async () => (await store.countTenant(tenant)) >= CONSUMER_N && (await fullyCommitted(group, topic)), 90_000, `${point.name}: recovery`);
    const recoveryMs = Date.now() - t0;
    await sleep(300);
    const st = await recovered.stats();

    const fp = await store.fingerprint(tenant);
    const inLog = (await withAdmin((a) => a.fetchTopicOffsets(topic))).reduce((s, p) => s + Number(p.high), 0);
    return {
      point: point.name,
      component: point.component,
      description: point.description,
      sent: CONSUMER_N,
      duplicatesProduced: inLog - CONSUMER_N,
      storedAtCrash,
      committedAtCrash,
      duplicatesProcessed: st.duplicates,
      duplicatesStored: fp.rows - fp.distinct,
      lost: CONSUMER_N - fp.distinct,
      recoveryMs,
      crashed,
    };
  } catch (e) {
    const offsets = await groupOffsets(group, topic).catch(() => null);
    throw new Error(`${(e as Error).message}\noffsets=${JSON.stringify(offsets)}\nlog=${procs.at(-1)?.log.join('').slice(-2000)}`);
  } finally {
    await Promise.all(procs.map((p) => p.stop()));
    await deleteTopic(topic);
    await store.cleanup(tenant, group);
  }
}

/**
 * API: N events posted one by one, each with its own Idempotency-Key, by a client that behaves like a real one:
 * on a network error, 409 or 503 it retries the SAME key until it gets 202. The API is armed to die at the point
 * on its API_CRASH_AT-th request; the client restarts it (without the fault) and keeps going. Then the sink stores
 * everything and the store is audited. A short inflight TTL keeps the test fast.
 */
export const API_N = 200;
const API_CRASH_AT = 50;
const API_INFLIGHT_TTL_S = 2;

async function apiScenario(point: CrashPointInfo, store: StoreProbe): Promise<CrashResult> {
  const topic = await createTopic(6);
  const group = `crash-matrix-${randomUUID()}`;
  const tenant = `crash_api_${randomUUID().slice(0, 8)}`;
  const apiKey = `key_${tenant}`;
  const env = {
    KAFKA_TOPIC: topic,
    API_KEYS: `${apiKey}:${tenant}`,
    IDEM_KEY_PREFIX: `cm${randomUUID().slice(0, 8)}`, // nothing shared with other runs in the test Redis db
    IDEM_INFLIGHT_TTL_S: String(API_INFLIGHT_TTL_S),
    KAFKA_SEND_TIMEOUT_MS: '1500', // must stay below the inflight TTL (config check)
  };
  const apis: ApiProcess[] = [];
  const sinks: SinkProcess[] = [];
  const seen = { s409: 0, s503: 0, replayed: 0, networkErrors: 0 };
  try {
    let api = spawnApi({ ...env, DEBUG_FAULTS: 'true', FAULTS: `${point.name}@${API_CRASH_AT}` });
    apis.push(api);
    await waitApiReady(api);
    let crashed = false;
    let answeredAtCrash = 0;
    let inKafkaAtCrash = 0;
    let recoveryMs = 0;
    let answered = 0;

    for (let i = 0; i < API_N; i++) {
      const body = { event_id: randomUUID(), type: 'crash.matrix', occurred_at: new Date().toISOString(), schema_version: 1, payload: { i } };
      for (let attempt = 0; ; attempt++) {
        if (attempt > 200) throw new Error(`event ${i}: no 202 after 200 attempts`);
        const r = await postEvent(api, apiKey, body, `cm-${i}`);
        if (r.status === 202) {
          if (r.headers?.get('idempotent-replayed') === 'true') seen.replayed++;
          answered++;
          break;
        }
        if (r.status === 409) seen.s409++;
        else if (r.status === 503) seen.s503++;
        else if (r.status === 'network_error') seen.networkErrors++;
        else throw new Error(`event ${i}: unexpected ${r.status} ${JSON.stringify(r.body)}`);
        if (r.status === 'network_error') {
          // The connection dropped: if the API died, restart it without the fault (a transient network error
          // without an exit just gets retried).
          const exit = await Promise.race([api.exited, sleep(5_000).then(() => null)]);
          if (exit) {
            crashed = exit.signal === 'SIGKILL' && api.log.join('').includes(`CRASH_AT=${point.name}`);
            if (!crashed) throw new Error(`${point.name}: api exited without the crash (${JSON.stringify(exit)})\n${api.log.join('').slice(-2000)}`);
            answeredAtCrash = answered;
            inKafkaAtCrash = await logTotal(topic);
            const t0 = Date.now();
            api = spawnApi(env); // restarted without the fault
            apis.push(api);
            await waitApiReady(api);
            recoveryMs = Date.now() - t0;
          }
        }
        await sleep(250);
      }
    }
    if (!crashed) throw new Error(`${point.name}: the api never crashed`);

    const sink = spawnSink({ CONSUMER_TOPIC: topic, CONSUMER_GROUP_ID: group, SINK_MAX_ROWS: '100' });
    sinks.push(sink);
    await waitFor(async () => (await store.countTenant(tenant)) >= API_N && (await fullyCommitted(group, topic)), 90_000, `${point.name}: sink drained`);
    await sleep(300);
    const st = await sink.stats();
    const fp = await store.fingerprint(tenant);
    return {
      point: point.name,
      component: point.component,
      description: point.description,
      sent: API_N,
      duplicatesProduced: (await logTotal(topic)) - API_N,
      storedAtCrash: answeredAtCrash,
      committedAtCrash: inKafkaAtCrash,
      duplicatesProcessed: st.duplicates,
      duplicatesStored: fp.rows - fp.distinct,
      lost: API_N - fp.distinct,
      recoveryMs,
      crashed,
      notes: `client: ${seen.networkErrors} network error, ${seen.s409}x 409, ${seen.replayed}x replayed 202`,
    };
  } finally {
    await Promise.all([...apis.map((p) => p.stop()), ...sinks.map((p) => p.stop())]);
    await deleteTopic(topic);
    await store.cleanup(tenant, group);
  }
}

const logTotal = async (topic: string) => (await withAdmin((a) => a.fetchTopicOffsets(topic))).reduce((s, p) => s + Number(p.high), 0);

// --- Retry tiers and DLQ (S3-F5) ---------------------------------------------------------------------------------
// One sink process (main + retry-tier consumer), throw-away source, tier and DLQ topics. The crashing process is
// armed with the point; the restarted one runs without any fault, so every record ends up stored (or dead-lettered).

const RETRY_N = 300;
const RETRY_MARKED = 10;
const DLQ_POISON = 5;

async function retryLane() {
  const source = await createTopic(3);
  const tiers = await Promise.all([1, 2, 3].map((n) => createTopicNamed(`${source}-retry-${n}`, 3)));
  const dlq = await createTopicNamed(`${source}-dlq`, 1);
  const id = randomUUID().slice(0, 8);
  const group = `crash-matrix-${id}`;
  const env = {
    CONSUMER_TOPIC: source,
    CONSUMER_GROUP_ID: group,
    SINK_DLQ_TOPIC: dlq,
    RETRY_TOPICS: tiers.join(','),
    RETRY_TIER_DELAYS_MS: '500,500,500',
    SINK_MAX_ROWS: '100',
    SINK_RETRY_BASE_MS: '20',
    SINK_RETRY_MAX_MS: '100',
    CONSUMER_SESSION_TIMEOUT_MS: '10000',
  };
  const cleanup = () => Promise.all([source, ...tiers, dlq].map(deleteTopic));
  return { source, tiers, dlq, group, retryGroup: `${group}-retry`, env, tenant: `crash_${id}`, cleanup };
}

const sumTopics = async (ts: string[]) => {
  let n = 0;
  for (const t of ts) n += await logTotal(t);
  return n;
};

/** Raw records that are not JSON (PERMANENT_PARSE -> DLQ at once), keyed so they spread over the partitions. */
async function producePoisonRaw(topic: string, n: number) {
  const producer = createKafka({ ...loadKafkaClientConfig(process.env, 'crash-matrix-poison'), logLevel: 'error' }).producer({ kafkaJS: { acks: -1, idempotent: true } } as any);
  await producer.connect();
  try {
    await producer.send({ topic, messages: Array.from({ length: n }, (_, i) => ({ key: `poison-${i}`, value: Buffer.from(`{not json ${i}`), headers: { received_at: new Date().toISOString() } })) });
  } finally {
    await producer.disconnect();
  }
}

async function crashAndRecover(point: CrashPointInfo, store: StoreProbe, lane: Awaited<ReturnType<typeof retryLane>>, crashEnv: Record<string, string>, done: () => Promise<boolean>) {
  const procs: SinkProcess[] = [];
  try {
    const crashing = spawnSink({ ...lane.env, DEBUG_FAULTS: 'true', ...crashEnv });
    procs.push(crashing);
    const exit = await Promise.race([crashing.exited, sleep(60_000).then(() => null)]);
    const crashed = exit?.signal === 'SIGKILL' && crashing.log.join('').includes(`CRASH_AT=${point.name}`);
    if (!crashed) throw new Error(`${point.name}: process did not crash there (exit ${JSON.stringify(exit)})\n${crashing.log.join('').slice(-3000)}`);
    const storedAtCrash = await store.countTenant(lane.tenant);
    const committedAtCrash = await totalCommitted(lane.group, lane.source);
    const t0 = Date.now();
    const recovered = spawnSink(lane.env);
    procs.push(recovered);
    await waitFor(done, 90_000, `${point.name}: recovery`);
    const recoveryMs = Date.now() - t0;
    await sleep(300);
    return { crashed, storedAtCrash, committedAtCrash, recoveryMs, stats: await recovered.stats() };
  } finally {
    await Promise.all(procs.map((p) => p.stop()));
  }
}

async function retryScenario(point: CrashPointInfo, store: StoreProbe): Promise<CrashResult> {
  const lane = await retryLane();
  try {
    const events = fixedEvents(RETRY_N, 'crash-retry').map((e, i) => (i % (RETRY_N / RETRY_MARKED) === 0 ? { ...e, payload: { ...e.payload, tag: 'retry-me' } } : e));
    await produceFixed(lane.source, lane.tenant, events);
    // The main path fails the marked rows (-> tier 1); the crashing process dies at the point on its first hit.
    const r = await crashAndRecover(point, store, lane, { FAULT_ERRORS: 'consumer:before-insert=40001x*:marker=retry-me', FAULTS: `${point.name}@1` }, async () => {
      if ((await store.countTenant(lane.tenant)) < RETRY_N || !(await fullyCommitted(lane.group, lane.source))) return false;
      for (const t of lane.tiers) if (!(await fullyCommitted(lane.retryGroup, t))) return false;
      return true;
    });
    const fp = await store.fingerprint(lane.tenant);
    return {
      point: point.name,
      component: point.component,
      description: point.description,
      sent: RETRY_N,
      duplicatesProduced: (await logTotal(lane.source)) - RETRY_N,
      storedAtCrash: r.storedAtCrash,
      committedAtCrash: r.committedAtCrash,
      duplicatesProcessed: r.stats.duplicates,
      duplicatesStored: fp.rows - fp.distinct,
      lost: RETRY_N - fp.distinct,
      recoveryMs: r.recoveryMs,
      crashed: r.crashed,
      notes: `retry-tier records ${await sumTopics(lane.tiers)} for ${RETRY_MARKED} marked rows, DLQ ${await logTotal(lane.dlq)}`,
    };
  } finally {
    await lane.cleanup();
    await store.cleanup(lane.tenant, lane.group);
  }
}

async function dlqScenario(point: CrashPointInfo, store: StoreProbe): Promise<CrashResult> {
  const lane = await retryLane();
  const metaRows = async () => Number((await store.pool.query('select count(*) from dlq_meta where dlq_topic = $1', [lane.dlq])).rows[0].count);
  try {
    await produceFixed(lane.source, lane.tenant, fixedEvents(RETRY_N, 'crash-dlq'));
    await producePoisonRaw(lane.source, DLQ_POISON);
    const r = await crashAndRecover(point, store, lane, { FAULTS: `${point.name}@1` }, async () =>
      (await store.countTenant(lane.tenant)) >= RETRY_N && (await fullyCommitted(lane.group, lane.source)) && (await metaRows()) >= DLQ_POISON,
    );
    const fp = await store.fingerprint(lane.tenant);
    const dlqRecords = await logTotal(lane.dlq);
    const meta = await metaRows();
    if (meta !== DLQ_POISON) throw new Error(`${point.name}: dlq_meta has ${meta} rows for ${DLQ_POISON} poison records`);
    return {
      point: point.name,
      component: point.component,
      description: point.description,
      sent: RETRY_N + DLQ_POISON,
      duplicatesProduced: dlqRecords - DLQ_POISON, // dead letters published twice (re-read after the crash)
      storedAtCrash: r.storedAtCrash,
      committedAtCrash: r.committedAtCrash,
      duplicatesProcessed: r.stats.duplicates,
      duplicatesStored: fp.rows - fp.distinct,
      lost: RETRY_N - fp.distinct,
      recoveryMs: r.recoveryMs,
      crashed: r.crashed,
      notes: `DLQ records ${dlqRecords}, dlq_meta rows ${meta} (one per poison record), DLQ duplicates counted ${r.stats.dlq_duplicates}`,
    };
  } finally {
    await lane.cleanup();
    await store.pool.query('delete from dlq_meta where dlq_topic = $1', [lane.dlq]);
    await store.cleanup(lane.tenant, lane.group);
  }
}

// --- DLQ replay tool (S3-F6) -------------------------------------------------------------------------------------
// REPLAY_N fixable dead letters (PERMANENT_DB from an injected fault that the restarted sink no longer has) next to
// REPLAY_GOOD healthy events. `dlq-replay replay --execute` is killed at the point on its REPLAY_CRASH_AT-th record;
// the SAME command is run again. Every event must be stored once and every dead letter marked once.

const REPLAY_N = 20;
const REPLAY_GOOD = 30;
const REPLAY_CRASH_AT = 8;
/** Marked when the tool dies on its REPLAY_CRASH_AT-th record: the ones before it. */
export const REPLAY_CRASH_MARKED = REPLAY_CRASH_AT - 1;

async function replayScenario(point: CrashPointInfo, store: StoreProbe): Promise<CrashResult> {
  const lane = await dlqLane(store);
  try {
    const { sink } = await lane.seed({ fixable: REPLAY_N, poison: 0, good: REPLAY_GOOD });
    const inLogBefore = await logTotal(lane.source);
    const args = ['replay', '--dlq-topic', lane.dlq, '--json', '--rate', '0', '--execute', '--yes'];
    const crashing = await runReplayCli(args, { DEBUG_FAULTS: 'true', FAULTS: `${point.name}@${REPLAY_CRASH_AT}` });
    const crashed = crashing.signal === 'SIGKILL' && crashing.stderr.includes(`CRASH_AT=${point.name}`);
    if (!crashed) throw new Error(`${point.name}: replay did not crash there (code ${crashing.code}, signal ${crashing.signal})\n${crashing.stderr.slice(-2000)}`);
    const marked = async () => Number((await store.pool.query('select count(*) from dlq_meta where dlq_topic = $1 and replayed_at is not null', [lane.dlq])).rows[0].count);
    const markedAtCrash = await marked();
    const storedAtCrash = await store.countTenant(lane.tenant);

    const t0 = Date.now();
    const again = await runReplayCli(args);
    if (again.code !== 0) throw new Error(`${point.name}: re-run failed (${again.code})\n${again.stderr.slice(-2000)}`);
    await waitFor(async () => (await store.countTenant(lane.tenant)) >= REPLAY_N + REPLAY_GOOD && (await marked()) === REPLAY_N && (await fullyCommitted(lane.group, lane.source)), 60_000, `${point.name}: recovery`);
    const recoveryMs = Date.now() - t0;
    await sleep(300);
    const st = await sink.stats();
    const fp = await store.fingerprint(lane.tenant);
    const replayedRecords = (await logTotal(lane.source)) - inLogBefore;
    const twice = Number((await store.pool.query('select count(*) from dlq_meta where dlq_topic = $1 and replay_count <> 1', [lane.dlq])).rows[0].count);
    if (twice !== 0) throw new Error(`${point.name}: ${twice} dlq_meta row(s) not marked exactly once`);
    return {
      point: point.name,
      component: point.component,
      description: point.description,
      sent: REPLAY_N + REPLAY_GOOD,
      duplicatesProduced: replayedRecords - REPLAY_N, // the record produced before the crash but not marked
      storedAtCrash,
      committedAtCrash: markedAtCrash, // replay: dead letters marked as replayed when the tool died
      duplicatesProcessed: st.duplicates,
      duplicatesStored: fp.rows - fp.distinct,
      lost: REPLAY_N + REPLAY_GOOD - fp.distinct,
      recoveryMs,
      crashed,
      notes: `${REPLAY_N} dead letters, ${markedAtCrash} marked at the crash, ${replayedRecords} records re-injected in total, every dlq_meta row marked once`,
    };
  } finally {
    await lane.cleanup();
  }
}

export const SCENARIOS: Readonly<Record<string, Scenario>> = {
  consumer: consumerScenario,
  api: apiScenario,
  retry: retryScenario,
  dlq: dlqScenario,
  replay: replayScenario,
};
