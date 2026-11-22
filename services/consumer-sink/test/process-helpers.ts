// Helpers for tests that run consumer-sink as a real child process (so it can be SIGKILLed / SIGTERMed),
// each against its own throw-away topic and consumer group.
import { execFile, spawn } from 'node:child_process';
import { promisify } from 'node:util';
import type { ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { Pool } from 'pg';
import { createKafka, encodeEvent, loadKafkaClientConfig } from '@eventpulse/kafka-config';

export const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export async function waitFor(cond: () => Promise<boolean> | boolean, timeoutMs: number, what: string) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await cond()) return;
    await sleep(250);
  }
  throw new Error(`timed out after ${timeoutMs} ms waiting for ${what}`);
}

const kafka = () => createKafka({ ...loadKafkaClientConfig(process.env, 'sink-e2e-helper'), logLevel: 'error' });

export async function withAdmin<T>(fn: (admin: ReturnType<ReturnType<typeof kafka>['admin']>) => Promise<T>): Promise<T> {
  const admin = kafka().admin();
  await admin.connect();
  try {
    return await fn(admin);
  } finally {
    await admin.disconnect();
  }
}

export async function createTopic(partitions = 6): Promise<string> {
  const topic = `sink-e2e-${randomUUID()}`;
  await withAdmin((a) =>
    a.createTopics({
      topics: [{ topic, numPartitions: partitions, replicationFactor: 3, configEntries: [{ name: 'min.insync.replicas', value: '2' }] }],
    }),
  );
  return topic;
}

export const deleteTopic = (topic: string) => withAdmin((a) => a.deleteTopics({ topics: [topic] })).catch(() => undefined);

/**
 * Produces n events (encoded like the API does). Keyed by event_id so they spread over all partitions,
 * unless `key` is given: then all go to that key's partition (a "busy" partition).
 */
export async function produceEvents(topic: string, n: number, opts: { key?: string } = {}): Promise<string[]> {
  const producer = kafka().producer({ kafkaJS: { acks: -1, idempotent: true }, 'linger.ms': 0 } as any);
  await producer.connect();
  const ids: string[] = [];
  try {
    for (let i = 0; i < n; i += 500) {
      const messages = Array.from({ length: Math.min(500, n - i) }, () => {
        const event_id = randomUUID();
        ids.push(event_id);
        const e = encodeEvent({
          tenantId: 'sink_e2e_proc',
          event: { event_id, type: 'x', occurred_at: new Date().toISOString(), schema_version: 1, payload: { i } },
          receivedAt: new Date(),
          requestId: 'e2e',
          keyMode: 'event',
        });
        return { key: opts.key ?? e.key, value: e.value, headers: e.headers };
      });
      await producer.send({ topic, messages });
    }
  } finally {
    await producer.disconnect();
  }
  // Guard: the consumer tests below count rows against these ids, so first make sure the log really holds
  // every acknowledged record. If it doesn't, the problem is on the produce side, not in the sink.
  const logTotal = async () => (await withAdmin((a) => a.fetchTopicOffsets(topic))).reduce((s, e) => s + Number(e.high), 0);
  let total = await logTotal();
  for (let i = 0; i < 40 && total < n; i++) {
    await sleep(250);
    total = await logTotal();
  }
  if (total < n) throw new Error(`PRODUCE CHECK: producer acknowledged ${n} records but ${topic} holds ${total}`);
  return ids;
}

/** Per partition: [committed offset of the group, log end offset]. */
export async function groupOffsets(groupId: string, topic: string) {
  return withAdmin(async (a) => {
    const ends = await a.fetchTopicOffsets(topic);
    const committed = await a.fetchOffsets({ groupId, topics: [topic] });
    const byPart = new Map((committed[0]?.partitions ?? []).map((p: any) => [p.partition, Number(p.offset)]));
    // No commit yet (-1) means "start of the log": 0 for these fresh topics, so an empty partition counts as done.
    return ends.map((e) => ({ partition: e.partition, committed: Math.max(0, Number(byPart.get(e.partition) ?? -1)), end: Number(e.high) }));
  });
}

export const fullyCommitted = async (groupId: string, topic: string) =>
  (await groupOffsets(groupId, topic)).every((p) => p.committed === p.end);

export const totalCommitted = async (groupId: string, topic: string) =>
  (await groupOffsets(groupId, topic)).reduce((a, p) => a + Math.max(0, p.committed), 0);

export class Db {
  readonly pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 2 });
  async countRows(ids: string[]): Promise<number> {
    return Number((await this.pool.query('select count(*) from events where event_id = any($1::uuid[])', [ids])).rows[0].count);
  }
  end() {
    return this.pool.end();
  }
}

let nextPort = 18300 + Math.floor(Math.random() * 500);

export interface SinkProcess {
  proc: ChildProcess;
  port: number;
  log: string[];
  exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  stats(): Promise<any>;
  /** SIGTERM and wait for the exit. */
  stop(): Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
}

/** Starts `node dist/main.js` (run `pnpm build` first; pretest:e2e does). */
export function spawnSink(env: Record<string, string>): SinkProcess {
  const port = nextPort++;
  const log: string[] = [];
  const proc = spawn(process.execPath, ['dist/main.js'], {
    cwd: resolve(import.meta.dirname, '..'),
    env: { ...process.env, LOG_ENABLED: 'true', KAFKA_LOG_LEVEL: 'error', CONSUMER_FROM_BEGINNING: 'true', METRICS_PORT: String(port), ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  proc.stdout!.on('data', (d) => log.push(String(d)));
  proc.stderr!.on('data', (d) => log.push(String(d)));
  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((r) =>
    proc.once('exit', (code, signal) => r({ code, signal })),
  );
  return {
    proc,
    port,
    log,
    exited,
    stats: async () => (await fetch(`http://localhost:${port}/debug/stats`)).json(),
    stop: async () => {
      if (proc.exitCode === null && proc.signalCode === null) proc.kill('SIGTERM');
      return exited;
    },
  };
}

export async function waitReady(p: SinkProcess, timeoutMs = 30_000) {
  await waitFor(async () => {
    try {
      return (await fetch(`http://localhost:${p.port}/readyz`)).ok;
    } catch {
      return false;
    }
  }, timeoutMs, `sink on :${p.port} to join its group\n${p.log.join('')}`);
}

// --- Toxiproxy (ep-toxiproxy, proxy "pg": localhost:15434 -> postgres:5432) -----------------------------
const TOXI = process.env.TOXI_URL ?? 'http://localhost:8474';
export async function setPgProxy(enabled: boolean) {
  const r = await fetch(`${TOXI}/proxies/pg`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ enabled }) });
  if (!r.ok) throw new Error(`toxiproxy: ${r.status} ${await r.text()}`);
}
export function databaseUrlViaProxy(): string {
  const u = new URL(process.env.DATABASE_URL!);
  u.hostname = 'localhost';
  u.port = '15434';
  return u.toString();
}

export const createTopicNamed = (topic: string, partitions: number) =>
  withAdmin((a) =>
    a.createTopics({
      topics: [{ topic, numPartitions: partitions, replicationFactor: 3, configEntries: [{ name: 'min.insync.replicas', value: '2' }] }],
    }),
  ).then(() => topic);

/** scripts/produce-poison.sh --kind KIND --key KEY --topic TOPIC (runs the console producer in ep-kafka1). */
export async function producePoison(kind: string, key: string, topic: string): Promise<void> {
  await promisify(execFile)(resolve(import.meta.dirname, '../../../scripts/produce-poison.sh'), ['--kind', kind, '--key', key, '--topic', topic]);
}

export interface TopicRecord {
  partition: number;
  offset: string;
  key: string | null;
  value: Buffer | null;
  headers: Record<string, string>;
}

/** Reads a topic from the beginning with a throw-away group for up to timeoutMs (or until `until` is true). */
export async function readTopic(topic: string, timeoutMs: number, until: (r: TopicRecord[]) => boolean = () => false): Promise<TopicRecord[]> {
  const consumer = kafka().consumer({ kafkaJS: { groupId: `read-${randomUUID()}`, fromBeginning: true, autoCommit: false } });
  const out: TopicRecord[] = [];
  await consumer.connect();
  try {
    await consumer.subscribe({ topics: [topic] });
    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        const headers: Record<string, string> = {};
        for (const [k, v] of Object.entries(message.headers ?? {})) headers[k] = String(Array.isArray(v) ? v[0] : v);
        out.push({ partition, offset: message.offset, key: message.key?.toString() ?? null, value: message.value, headers });
      },
    });
    const deadline = Date.now() + timeoutMs;
    while (Date.now() < deadline && !until(out)) await sleep(200);
    return out;
  } finally {
    await consumer.disconnect();
  }
}
