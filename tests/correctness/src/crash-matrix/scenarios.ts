// One crash scenario per component. The crash matrix iterates over CRASH_POINTS (packages/fault-injector) and
// looks up the component's scenario here, so a point whose component has no scenario fails the build
// (crash-matrix.spec.ts). Later Stage 3 steps add api, retry, dlq, replay, relay and admin scenarios.
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

export const SCENARIOS: Readonly<Record<string, Scenario>> = {
  consumer: consumerScenario,
};
