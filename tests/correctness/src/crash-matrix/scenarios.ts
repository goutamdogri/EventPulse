// One crash scenario per component. The crash matrix iterates over CRASH_POINTS (packages/fault-injector) and
// looks up the component's scenario here, so a point whose component has no scenario fails the build
// (crash-matrix.spec.ts). Later Stage 3 steps add retry, dlq, replay, relay and admin scenarios.
import { randomUUID } from 'node:crypto';
import type { CrashPointInfo } from '@eventpulse/fault-injector';
import {
  StoreProbe,
  createTopic,
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

export const SCENARIOS: Readonly<Record<string, Scenario>> = {
  consumer: consumerScenario,
  api: apiScenario,
};
