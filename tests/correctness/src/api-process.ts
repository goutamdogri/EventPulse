// Runs the built ingest-api (services/ingest-api/dist/main.js) as a child process, so tests can SIGKILL it at a
// crash point and restart it. Build it first (pretest:e2e of this package builds both services).
import { spawn } from 'node:child_process';
import type { ChildProcess } from 'node:child_process';
import { resolve } from 'node:path';

let nextPort = 18700 + Math.floor(Math.random() * 150);
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export interface ApiProcess {
  proc: ChildProcess;
  port: number;
  log: string[];
  exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  url(path: string): string;
  stats(): Promise<any>;
  stop(): Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
}

/** Test Redis database only: never the dev database 0 of a running API. */
export const TEST_REDIS_URL = process.env.TEST_REDIS_URL ?? 'redis://localhost:6379/15';

export function spawnApi(env: Record<string, string>): ApiProcess {
  const port = nextPort++;
  const log: string[] = [];
  const proc = spawn(process.execPath, ['dist/main.js'], {
    cwd: resolve(import.meta.dirname, '../../../services/ingest-api'),
    env: { ...process.env, LOG_ENABLED: 'true', KAFKA_LOG_LEVEL: 'error', REDIS_URL: TEST_REDIS_URL, PORT: String(port), ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  proc.stdout!.on('data', (d) => log.push(String(d)));
  proc.stderr!.on('data', (d) => log.push(String(d)));
  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((r) => proc.once('exit', (code, signal) => r({ code, signal })));
  return {
    proc,
    port,
    log,
    exited,
    url: (p) => `http://localhost:${port}${p}`,
    stats: async () => (await fetch(`http://localhost:${port}/debug/stats`)).json(),
    stop: async () => {
      if (proc.exitCode === null && proc.signalCode === null) proc.kill('SIGTERM');
      return exited;
    },
  };
}

export async function waitApiReady(p: ApiProcess, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (p.proc.exitCode !== null || p.proc.signalCode !== null) throw new Error(`api exited before ready\n${p.log.join('')}`);
    try {
      if ((await fetch(p.url('/readyz'))).ok) return;
    } catch {
      /* not listening yet */
    }
    await sleep(200);
  }
  throw new Error(`api on :${p.port} not ready after ${timeoutMs} ms\n${p.log.join('')}`);
}

export interface PostResult {
  status: number | 'network_error';
  body?: any;
  headers?: Headers;
}

export async function postEvent(p: ApiProcess, apiKey: string, body: object, idempotencyKey?: string): Promise<PostResult> {
  try {
    const r = await fetch(p.url('/v1/events'), {
      method: 'POST',
      headers: {
        authorization: `Bearer ${apiKey}`,
        'content-type': 'application/json',
        ...(idempotencyKey ? { 'idempotency-key': idempotencyKey } : {}),
      },
      body: JSON.stringify(body),
      signal: AbortSignal.timeout(15_000),
    });
    return { status: r.status, body: await r.json().catch(() => null), headers: r.headers };
  } catch {
    return { status: 'network_error' };
  }
}
