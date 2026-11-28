// Helpers for the S3-F6 tests (dlq-replay e2e + its crash-matrix scenario): a lane of throw-away source and DLQ
// topics with its own sink, a way to make dead letters that are fixable (an injected PERMANENT_DB error that goes
// away when the sink restarts without it) or still poison (bytes that are not JSON), and the replay CLI as a child
// process (so it can be SIGKILLed at its crash point).
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { createKafka, loadKafkaClientConfig } from '@eventpulse/kafka-config';
import { StoreProbe, createTopic, createTopicNamed, deleteTopic, fixedEvents, produceFixed, spawnSink, waitFor, waitReady, withAdmin } from './harness.js';
import type { FixedEvent, SinkProcess } from './harness.js';

const CLI = resolve(import.meta.dirname, '../../../tools/dlq-replay/src/cli.ts');

export interface CliResult {
  code: number | null;
  signal: NodeJS.Signals | null;
  stdout: string;
  stderr: string;
  /** stdout parsed as JSON (commands run with --json), undefined if it is not JSON. */
  json?: any;
}

/** Runs `node tools/dlq-replay/src/cli.ts <args>` against the test database (DATABASE_URL from the test env). */
export function runReplayCli(args: string[], env: Record<string, string> = {}): Promise<CliResult> {
  return new Promise((res) => {
    const p = spawn(process.execPath, [CLI, ...args], {
      env: { ...process.env, KAFKA_LOG_LEVEL: 'error', DLQ_REPLAY_TEXTFILE: 'off', DLQ_REPLAY_OPERATOR: 'correctness-tests', ...env },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    let stdout = '';
    let stderr = '';
    p.stdout.on('data', (d) => (stdout += d));
    p.stderr.on('data', (d) => (stderr += d));
    p.once('exit', (code, signal) => {
      let json: any;
      try {
        json = JSON.parse(stdout);
      } catch {
        json = undefined;
      }
      res({ code, signal, stdout, stderr, json });
    });
  });
}

/** Raw records that are not JSON (PERMANENT_PARSE, and the replay pre-check rejects them too). */
export async function producePoisonRaw(topic: string, n: number, tag = 'poison') {
  const producer = createKafka({ ...loadKafkaClientConfig(process.env, 'dlq-replay-poison'), logLevel: 'error' }).producer({ kafkaJS: { acks: -1, idempotent: true } } as any);
  await producer.connect();
  try {
    await producer.send({ topic, messages: Array.from({ length: n }, (_, i) => ({ key: `${tag}-${i}`, value: Buffer.from(`{not json ${tag} ${i}`), headers: { received_at: new Date().toISOString() } })) });
  } finally {
    await producer.disconnect();
  }
}

export const logTotal = async (topic: string) => (await withAdmin((a) => a.fetchTopicOffsets(topic))).reduce((s, p) => s + Number(p.high), 0);

/** The marker the sink is armed with: rows whose payload contains it fail with 22001 (PERMANENT_DB) at insert. */
export const FIXABLE = 'fixable';
export const FIXABLE_FAULT = { DEBUG_FAULTS: 'true', FAULT_ERRORS: `consumer:before-insert=22001x*:marker=${FIXABLE}` };

export async function dlqLane(store: StoreProbe) {
  const id = randomUUID().slice(0, 8);
  const source = await createTopic(3);
  const dlq = await createTopicNamed(`${source}-dlq`, 1);
  const group = `dlq-replay-${id}`;
  const tenant = `replay_${id}`;
  const procs: SinkProcess[] = [];
  const startSink = async (extra: Record<string, string> = {}) => {
    const p = spawnSink({ CONSUMER_TOPIC: source, CONSUMER_GROUP_ID: group, SINK_DLQ_TOPIC: dlq, SINK_MAX_ROWS: '100', SINK_RETRY_BASE_MS: '20', SINK_RETRY_MAX_MS: '100', DLQ_SIZE_POLL_MS: '0', ...extra });
    procs.push(p);
    await waitReady(p);
    return p;
  };
  const metaRows = async () => (await store.pool.query('select * from dlq_meta where dlq_topic = $1 order by id', [dlq])).rows;

  /**
   * Makes `fixable` dead letters (PERMANENT_DB from the injected fault) and `poison` ones (not JSON) next to `good`
   * healthy events, then restarts the sink WITHOUT the fault: the fixable ones would now be stored. Returns the
   * running (healthy) sink and the fixable events.
   */
  const seed = async (o: { fixable: number; poison: number; good: number }) => {
    const faulty = await startSink(FIXABLE_FAULT);
    const fixable: FixedEvent[] = fixedEvents(o.fixable, 'replay').map((e) => ({ ...e, payload: { ...e.payload, tag: FIXABLE } }));
    const good = fixedEvents(o.good, 'replay-good');
    await produceFixed(source, tenant, [...good.slice(0, Math.floor(o.good / 2)), ...fixable, ...good.slice(Math.floor(o.good / 2))]);
    if (o.poison) await producePoisonRaw(source, o.poison, `poison-${id}`);
    await waitFor(async () => (await metaRows()).length === o.fixable + o.poison && (await store.countTenant(tenant)) === o.good, 60_000, 'dead letters indexed');
    await faulty.stop();
    procs.splice(procs.indexOf(faulty), 1);
    const sink = await startSink();
    return { sink, fixable, good };
  };

  const cleanup = async () => {
    await Promise.all(procs.splice(0).map((p) => p.stop()));
    await Promise.all([source, dlq].map(deleteTopic));
    await store.pool.query('delete from dlq_meta where dlq_topic = $1', [dlq]);
    await store.cleanup(tenant, group);
  };
  return { id, source, dlq, group, tenant, startSink, metaRows, seed, cleanup, procs };
}
