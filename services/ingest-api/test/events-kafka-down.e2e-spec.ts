// S2-F5 end to end: Kafka unavailable -> 503 + Retry-After, bounded memory, recovery without an API restart.
// This file STOPS AND STARTS the shared broker containers (ep-kafka1..3) for several minutes: don't run it
// during a benchmark or while the dev pipeline matters. afterEach always brings all three brokers back.
import { spawn, execFileSync } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { createKafka, loadKafkaClientConfig, waitTopicReady } from '@eventpulse/kafka-config';
import { endOffsets, totalOf } from './kafka-helpers.js';

const BROKERS = ['ep-kafka1', 'ep-kafka2', 'ep-kafka3'];
const SEND_TIMEOUT_MS = 2000;
const MAX_QUEUE = 2000;
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const docker = (...args: string[]) => execFileSync('docker', args, { stdio: ['ignore', 'pipe', 'pipe'] }).toString().trim();
const stop = (names: string[]) => docker('stop', '-t', '1', ...names);
const start = (names: string[]) => docker('start', ...names);
const health = (n: string) => {
  try {
    return docker('inspect', '-f', '{{.State.Health.Status}}', n);
  } catch {
    return 'missing';
  }
};

async function waitBrokersHealthy(timeoutMs = 120_000) {
  const deadline = Date.now() + timeoutMs;
  while (BROKERS.some((b) => health(b) !== 'healthy')) {
    if (Date.now() > deadline) throw new Error('brokers not healthy');
    await sleep(1000);
  }
}

async function restoreCluster() {
  start(BROKERS);
  await waitBrokersHealthy();
  const admin = createKafka({ ...loadKafkaClientConfig(process.env, 'kafka-down-e2e'), logLevel: 'error' }).admin();
  await admin.connect();
  try {
    await waitTopicReady(admin, process.env.KAFKA_TOPIC!, 6, 3000, 90_000);
  } finally {
    await admin.disconnect();
  }
}

const PORT = 19100 + Math.floor(Math.random() * 400);
const base = `http://localhost:${PORT}`;
const ev = () => ({ event_id: randomUUID(), type: 'down.test', occurred_at: new Date().toISOString(), schema_version: 1, payload: {} });
const post = async () => {
  const t0 = performance.now();
  try {
    const r = await fetch(`${base}/v1/events`, {
      method: 'POST',
      headers: { authorization: 'Bearer key_a', 'content-type': 'application/json' },
      body: JSON.stringify(ev()),
    });
    const body: any = await r.json().catch(() => ({}));
    return { status: r.status, retryAfter: r.headers.get('retry-after'), reason: body.reason, ms: performance.now() - t0 };
  } catch {
    return { status: 0, retryAfter: null, reason: undefined, ms: performance.now() - t0 };
  }
};
const stats = async () => (await fetch(`${base}/debug/stats`)).json() as Promise<any>;
const readyz = async () => (await fetch(`${base}/readyz`)).status;

async function burst(n: number, concurrency = 100) {
  const out: Awaited<ReturnType<typeof post>>[] = [];
  let next = 0;
  await Promise.all(
    Array.from({ length: concurrency }, async () => {
      while (next++ < n) out.push(await post());
    }),
  );
  return out;
}
const p99 = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length * 0.99)] ?? 0;

let proc: ChildProcess | undefined;
const log: string[] = [];
function startApi(extra: Record<string, string> = {}) {
  proc = spawn(process.execPath, ['dist/main.js'], {
    cwd: resolve(import.meta.dirname, '..'),
    env: {
      ...process.env,
      PORT: String(PORT),
      LOG_ENABLED: 'false',
      KAFKA_SEND_TIMEOUT_MS: String(SEND_TIMEOUT_MS),
      KAFKA_BREAKER_FAILURES: '2',
      KAFKA_BREAKER_COOLDOWN_MS: '3000',
      KAFKA_STARTUP_WAIT_MS: '1000',
      KAFKA_RECONNECT_INITIAL_MS: '500',
      KAFKA_RECONNECT_MAX_MS: '2000',
      KAFKA_DELIVERY_TIMEOUT_MS: '4000',
      KAFKA_LOG_LEVEL: 'error',
      BATCH_MAX_QUEUE: String(MAX_QUEUE),
      RETRY_AFTER_S: '2',
      ...extra,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  proc.stdout!.on('data', (d) => log.push(String(d)));
  proc.stderr!.on('data', (d) => log.push(String(d)));
}
async function waitListening() {
  for (let i = 0; i < 120; i++) {
    try {
      if ((await fetch(`${base}/healthz`)).ok) return;
    } catch {
      /* not yet */
    }
    await sleep(250);
  }
  throw new Error(`API did not start\n${log.join('')}`);
}
async function waitReady(timeoutMs = 60_000) {
  const deadline = Date.now() + timeoutMs;
  while ((await readyz().catch(() => 0)) !== 200) {
    if (Date.now() > deadline) throw new Error(`not ready after ${timeoutMs} ms\n${log.join('')}`);
    await sleep(500);
  }
}
async function waitFor202(timeoutMs = 60_000) {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const r = await post();
    if (r.status === 202) return r;
    if (Date.now() > deadline) throw new Error(`no 202 after ${timeoutMs} ms (last ${r.status})\n${log.join('')}`);
    await sleep(500);
  }
}

describe('Kafka unavailable', () => {
  afterEach(async () => {
    if (proc && proc.exitCode === null) {
      proc.kill('SIGTERM');
      await new Promise((r) => proc!.once('exit', r));
    }
    proc = undefined;
    log.length = 0;
    await restoreCluster();
  }, 240_000);

  it('all brokers down: every request is 503 + Retry-After, fast once the breaker is open, readyz 503, liveness stays 200', async () => {
    startApi();
    await waitListening();
    await waitReady();
    expect((await post()).status).toBe(202);

    stop(BROKERS);
    const rs = await burst(1000);
    expect(rs.length).toBe(1000);
    expect(rs.filter((r) => r.status !== 503).length, `statuses: ${JSON.stringify(rs.filter((r) => r.status !== 503).slice(0, 5))}\n${log.join('')}`).toBe(0);
    for (const r of rs) {
      expect(r.retryAfter).toBe('2');
      expect(['kafka_unavailable', 'overloaded']).toContain(r.reason);
    }
    // After the breaker opened, answers are immediate, far below the send timeout.
    const later = await burst(300);
    expect(later.every((r) => r.status === 503)).toBe(true);
    expect(p99(later.map((r) => r.ms))).toBeLessThan(SEND_TIMEOUT_MS / 2);
    expect(await readyz()).toBe(503);
    expect((await fetch(`${base}/healthz`)).status).toBe(200);
    const s = await stats();
    expect(s.producer.breaker).not.toBe('closed');
    expect(s.http_503_by_reason.kafka_unavailable).toBeGreaterThan(0);
  }, 240_000);

  it('brokers come back: 202 again and the record lands in the log, with no API restart', async () => {
    startApi();
    await waitListening();
    await waitReady();
    stop(BROKERS);
    await burst(50);
    expect(await readyz()).toBe(503);

    const before = totalOf(await endOffsets().catch(() => new Map()));
    start(BROKERS);
    await waitBrokersHealthy();
    const t0 = Date.now();
    await waitFor202(90_000);
    console.log(`recovered to 202 in ${Date.now() - t0} ms after the brokers were healthy`);
    await waitReady(30_000);
    const after = totalOf(await endOffsets());
    expect(after - before).toBeGreaterThanOrEqual(1n);
  }, 300_000);

  it('API starts while Kafka is down (readyz 503, requests 503); when brokers start it serves 202', async () => {
    stop(BROKERS);
    startApi();
    await waitListening(); // boots without Kafka
    expect(await readyz()).toBe(503);
    const r = await post();
    expect(r.status).toBe(503);
    expect(r.retryAfter).toBe('2');

    start(BROKERS);
    await waitBrokersHealthy();
    await waitFor202(120_000);
    await waitReady(30_000);
  }, 300_000);

  it('2 of 3 brokers down (below min.insync.replicas): 503, never 202', async () => {
    startApi();
    await waitListening();
    await waitReady();
    stop(['ep-kafka2', 'ep-kafka3']);
    const rs = await burst(300, 30);
    expect(rs.filter((r) => r.status === 202).length).toBe(0);
    expect(rs.every((r) => r.status === 503)).toBe(true);
    expect(rs.every((r) => r.retryAfter === '2')).toBe(true);
  }, 240_000);

  it('memory stays flat under a sustained flood while Kafka is down; queue never exceeds BATCH_MAX_QUEUE', async () => {
    startApi();
    await waitListening();
    await waitReady();
    stop(BROKERS);

    const RATE = 500; // requests per second
    const SECONDS = 60;
    const heap: number[] = [];
    let maxDepth = 0;
    let sent = 0;
    let stopFlood = false;
    const flood = (async () => {
      const t0 = Date.now();
      while (!stopFlood) {
        const due = Math.floor(((Date.now() - t0) / 1000) * RATE);
        while (sent < due) {
          sent++;
          void post();
        }
        await sleep(10);
      }
    })();

    for (let i = 0; i < SECONDS; i++) {
      await sleep(1000);
      const s = await stats();
      heap.push(s.memory.heapUsed);
      maxDepth = Math.max(maxDepth, s.batcher.queueDepth);
    }
    stopFlood = true;
    await flood;

    const med = (xs: number[]) => [...xs].sort((a, b) => a - b)[Math.floor(xs.length / 2)];
    const early = med(heap.slice(10, 25));
    const late = med(heap.slice(-15));
    const MB = 1024 * 1024;
    console.log(`heap early median ${(early / MB).toFixed(1)} MB, late median ${(late / MB).toFixed(1)} MB, max queue depth ${maxDepth}, sent ${sent}`);
    expect(maxDepth).toBeLessThanOrEqual(MAX_QUEUE);
    expect(late - early).toBeLessThan(25 * MB); // flat: no upward trend
    expect(Math.max(...heap) - Math.min(...heap.slice(5))).toBeLessThan(150 * MB); // and a bounded band
  }, 300_000);

  it('readiness follows Kafka with NO traffic at all: down -> 503, back -> 200 (background health check)', async () => {
    startApi({ KAFKA_HEALTH_INTERVAL_MS: '1000', KAFKA_HEALTH_TIMEOUT_MS: '1000' });
    await waitListening();
    await waitReady();

    stop(BROKERS); // and send nothing
    const t0 = Date.now();
    while ((await readyz()) !== 503) {
      if (Date.now() - t0 > 15_000) throw new Error(`readyz still 200 15 s after Kafka went down\n${log.join('')}`);
      await sleep(250);
    }
    console.log(`readyz 503 ${Date.now() - t0} ms after the brokers stopped, without any request`);
    expect((await stats()).producer.breaker).toBe('open'); // requests would now fail fast

    start(BROKERS);
    await waitBrokersHealthy();
    const t1 = Date.now();
    await waitReady(60_000); // still no request sent
    console.log(`readyz 200 ${Date.now() - t1} ms after the brokers were healthy, without any request`);
    expect((await post()).status).toBe(202);
  }, 300_000);

  it('a request arriving during the SIGTERM drain gets 503 shutting_down + Retry-After (not 500)', async () => {
    // Long health interval so the breaker stays closed and request A really sits in flight during the drain.
    startApi({ KAFKA_HEALTH_INTERVAL_MS: '60000', KAFKA_BREAKER_FAILURES: '100', KAFKA_SEND_TIMEOUT_MS: '4000', SHUTDOWN_DRAIN_TIMEOUT_MS: '6000' });
    await waitListening();
    await waitReady();
    stop(BROKERS);

    const a = post(); // hangs in flight: Kafka is down
    await sleep(300);
    proc!.kill('SIGTERM'); // drain starts and waits for A
    await sleep(300);
    const b = await post(); // arrives while the drain is running
    expect(b.status, log.join('')).toBe(503);
    expect(b.reason).toBe('shutting_down');
    expect(b.retryAfter).toBe('2');
    const ra = await a;
    expect(ra.status).toBe(503); // A: answered by the send timeout / drain deadline, never left hanging
    expect(log.join('')).not.toContain('[unhandled]');
  }, 240_000);
});
