// G1 durability (S3-F9): after a 202 (acks=all, min.insync.replicas=2, RF=3) the event is not lost, even when the
// API, the consumer and one broker are killed (SIGKILL / docker kill) around it.
// This file KILLS AND RESTARTS a shared broker container (ep-kafka2): don't run it during a benchmark.
import { execFileSync } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { createKafka, loadKafkaClientConfig, waitTopicReady } from '@eventpulse/kafka-config';
import { StoreProbe, createTopic, deleteTopic, sleep, spawnSink, waitFor, waitReady } from '../src/harness.js';
import type { SinkProcess } from '../src/harness.js';
import { postEvent, spawnApi, waitApiReady } from '../src/api-process.js';

const BROKER = 'ep-kafka2';
const N = 300;
const docker = (...args: string[]) => execFileSync('docker', args, { stdio: ['ignore', 'pipe', 'pipe'] }).toString().trim();
const health = (n: string) => {
  try {
    return docker('inspect', '-f', '{{.State.Health.Status}}', n);
  } catch {
    return 'missing';
  }
};

async function brokerBack(topic: string) {
  docker('start', BROKER);
  await waitFor(() => health(BROKER) === 'healthy', 120_000, `${BROKER} healthy`);
  const admin = createKafka({ ...loadKafkaClientConfig(process.env, 'durability-e2e'), logLevel: 'error' }).admin();
  await admin.connect();
  try {
    await waitTopicReady(admin, topic, 6, 3000, 90_000); // every partition back to full ISR
  } finally {
    await admin.disconnect();
  }
}

describe('G1 durability', () => {
  const store = new StoreProbe();
  afterAll(async () => {
    await store.end();
  });

  it(`[G1.durability] ${N} events, a broker killed mid-run, then the API and the consumer SIGKILLed: every 202 is stored after restart`, async () => {
    const topic = await createTopic(6); // RF 3, min.insync.replicas 2
    const tenant = `durable_${randomUUID().slice(0, 8)}`;
    const group = `durability-${randomUUID().slice(0, 8)}`;
    const apiKey = `k_${randomUUID()}`;
    const api = spawnApi({ KAFKA_TOPIC: topic, API_KEYS: `${apiKey}:${tenant}`, IDEM_KEY_PREFIX: `du${randomUUID().slice(0, 6)}` });
    const sinkEnv = { CONSUMER_TOPIC: topic, CONSUMER_GROUP_ID: group, SINK_MAX_ROWS: '100' };
    let sink: SinkProcess = spawnSink(sinkEnv);
    let brokerDown = false;
    try {
      await Promise.all([waitApiReady(api), waitReady(sink)]);
      const acked: string[] = [];
      const other: Record<string, number> = {};
      for (let i = 0; i < N; i += 20) {
        if (i === 100) {
          docker('kill', BROKER); // abrupt, like a crash: in-flight requests on it fail over
          brokerDown = true;
        }
        const batch = Array.from({ length: 20 }, (_, j) => ({ event_id: randomUUID(), type: 'durability', occurred_at: new Date().toISOString(), schema_version: 1, payload: { i: i + j } }));
        // Like a real client: a 503 (leaders moving off the dead broker) is NOT an ack; retry the same event_id.
        const send = async (e: object) => {
          const deadline = Date.now() + 60_000;
          for (;;) {
            const r = await postEvent(api, apiKey, e);
            if (r.status === 202 || Date.now() > deadline) return r.status;
            other[String(r.status)] = (other[String(r.status)] ?? 0) + 1;
            await sleep(250);
          }
        };
        const rs = await Promise.all(batch.map((e) => send(e).then((status) => ({ status, e }))));
        for (const { status, e } of rs) if (status === 202) acked.push(e.event_id);
      }
      if (acked.length !== N) {
        const st = await api.stats().catch(() => null);
        throw new Error(`only ${acked.length}/${N} acknowledged; retried responses ${JSON.stringify(other)}; api stats ${JSON.stringify(st?.producer ?? st)?.slice(0, 1500)}\n${api.log.join('').slice(-3000)}`);
      }

      // Kill both processes without any chance to drain or commit.
      api.proc.kill('SIGKILL');
      sink.proc.kill('SIGKILL');
      await Promise.all([api.exited, sink.exited]);
      const storedBeforeRestart = await store.countTenant(tenant);

      await brokerBack(topic);
      brokerDown = false;
      sink = spawnSink(sinkEnv);
      await waitReady(sink);
      const count = async () => Number((await store.pool.query('select count(*) from events where tenant_id = $1 and event_id = any($2)', [tenant, acked])).rows[0].count);
      await waitFor(async () => (await count()) === acked.length, 120_000, 'every acknowledged event stored');
      await sleep(500);
      const fp = await store.fingerprint(tenant);
      expect(fp.rows).toBe(fp.distinct); // nothing stored twice
      expect(await count()).toBe(acked.length); // nothing lost
      console.info(`[G1.durability] ${acked.length} acknowledged (retried responses before the 202: ${JSON.stringify(other)}), ${storedBeforeRestart} stored before the kill, all ${acked.length} after restart`);
    } finally {
      if (brokerDown) await brokerBack(topic).catch(() => undefined);
      await Promise.all([api.stop(), sink.stop()]);
      await deleteTopic(topic);
      await store.cleanup(tenant, group);
    }
  });
});
