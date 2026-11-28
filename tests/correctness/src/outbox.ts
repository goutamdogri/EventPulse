// Helpers for the S3-F4 outbox tests: admin-api and outbox-relay as real child processes (so they can be SIGKILLed),
// an isolated "lane" per test (its own tenant, throw-away config.changed topic, relay limited to that topic with its
// own advisory lock key), and a consumer that reads the topic back and applies it with applyIfNewer.
// Build both services first (pretest:e2e of this package builds them). Test database only (setup-env.ts guard).
import { spawn } from 'node:child_process';
import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import type { ChildProcess } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import { Pool } from 'pg';
import { ConfigChangedSerde, VersionedView, enqueue } from '@eventpulse/outbox';
import type { ConfigChanged } from '@eventpulse/outbox';
import { RegistryClient } from '@eventpulse/schema';
import { createTopicNamed, deleteTopic, readTopic, sleep } from '../../../services/consumer-sink/test/process-helpers.js';
import type { TopicRecord } from '../../../services/consumer-sink/test/process-helpers.js';

export const ADMIN_KEY = 'correctness-admin-key-0123456789';

const MEASUREMENTS = resolve(import.meta.dirname, '../../../docs/generated/outbox-measurements.json');

/**
 * Keep what a test measured (order violations in multi mode, the Kafka-down backlog...) in
 * docs/generated/outbox-measurements.json, one key per test, so ADR 0007 and the delivery-guarantees document cite
 * generated numbers. Also printed to stderr.
 */
export function recordMeasurement(name: string, value: Record<string, unknown>) {
  process.stderr.write(`[outbox] ${name}: ${JSON.stringify(value)}\n`);
  const all = existsSync(MEASUREMENTS) ? JSON.parse(readFileSync(MEASUREMENTS, 'utf8')) : {};
  all[name] = { ...value, measured_at: new Date().toISOString() };
  mkdirSync(resolve(MEASUREMENTS, '..'), { recursive: true });
  writeFileSync(MEASUREMENTS, `${JSON.stringify(all, null, 2)}\n`);
}
let nextPort = 18900 + Math.floor(Math.random() * 300);

export interface NodeProcess {
  name: string;
  proc: ChildProcess;
  port: number;
  log: string[];
  exited: Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  url(path: string): string;
  json(path: string): Promise<any>;
  alive(): boolean;
  stop(): Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
  kill(): Promise<{ code: number | null; signal: NodeJS.Signals | null }>;
}

function spawnService(name: string, dir: string, env: Record<string, string>): NodeProcess {
  const port = nextPort++;
  const log: string[] = [];
  const proc = spawn(process.execPath, ['dist/main.js'], {
    cwd: resolve(import.meta.dirname, '../../../services', dir),
    env: { ...process.env, LOG_ENABLED: 'true', KAFKA_LOG_LEVEL: 'error', PORT: String(port), ...env },
    stdio: ['ignore', 'pipe', 'pipe'],
  });
  proc.stdout!.on('data', (d) => log.push(String(d)));
  proc.stderr!.on('data', (d) => log.push(String(d)));
  const exited = new Promise<{ code: number | null; signal: NodeJS.Signals | null }>((r) => proc.once('exit', (code, signal) => r({ code, signal })));
  const alive = () => proc.exitCode === null && proc.signalCode === null;
  return {
    name,
    proc,
    port,
    log,
    exited,
    alive,
    url: (p) => `http://localhost:${port}${p}`,
    json: async (p) => (await fetch(`http://localhost:${port}${p}`)).json(),
    stop: async () => {
      if (alive()) proc.kill('SIGTERM');
      return exited;
    },
    kill: async () => {
      if (alive()) proc.kill('SIGKILL');
      return exited;
    },
  };
}

export async function waitHttpReady(p: NodeProcess, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (!p.alive()) throw new Error(`${p.name} exited before ready\n${p.log.join('')}`);
    try {
      if ((await fetch(p.url('/readyz'))).ok) return;
    } catch {
      /* not listening yet */
    }
    await sleep(200);
  }
  throw new Error(`${p.name} on :${p.port} not ready after ${timeoutMs} ms\n${p.log.join('')}`);
}

/** One metric value from a Prometheus text page (sum over label sets). */
export async function metricValue(p: NodeProcess, name: string): Promise<number> {
  const text = await (await fetch(p.url('/metrics'))).text();
  let sum = 0;
  for (const line of text.split('\n')) {
    if (line.startsWith('#')) continue;
    const m = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(\{[^}]*\})? (\S+)$/);
    if (m && m[1] === name) sum += Number(m[3]);
  }
  return sum;
}

export interface DecodedChange extends ConfigChanged {
  partition: number;
  offset: string;
  outboxId: string;
  headers: Record<string, string>;
}

/** Order check over what a consumer saw for ONE tenant: a NEW event (not a republished copy) older than one already seen. */
export function orderViolations(changes: { eventId: string; version: number }[]): number {
  const seen = new Set<string>();
  let max = 0;
  let violations = 0;
  for (const c of changes) {
    if (seen.has(c.eventId)) continue; // a republished copy: a duplicate, not an ordering problem
    seen.add(c.eventId);
    if (c.version < max) violations++;
    max = Math.max(max, c.version);
  }
  return violations;
}

export class OutboxLane {
  readonly pool = new Pool({ connectionString: process.env.DATABASE_URL, max: 4 });
  readonly serde = new ConfigChangedSerde(new RegistryClient({ url: process.env.SCHEMA_REGISTRY_URL ?? 'http://localhost:8081', timeoutMs: 5000 }));
  readonly lockKey = 1_000_000 + Math.floor(Math.random() * 1_000_000_000);
  private readonly procs: NodeProcess[] = [];

  private constructor(readonly topic: string, readonly tenant: string) {
    this.pool.on('error', () => undefined);
  }

  static async create(partitions = 3): Promise<OutboxLane> {
    const topic = await createTopicNamed(`config-e2e-${randomUUID()}`, partitions);
    const lane = new OutboxLane(topic, `cfg_${randomUUID().slice(0, 8)}`);
    await lane.pool.query('INSERT INTO tenants (tenant_id, name) VALUES ($1, $1)', [lane.tenant]);
    await lane.serde.init(); // config.changed-value is registered by `make schemas` (tools/schema-check register)
    return lane;
  }

  admin(env: Record<string, string> = {}): NodeProcess {
    const p = spawnService('admin-api', 'admin-api', { ADMIN_API_KEY: ADMIN_KEY, CONFIG_TOPIC: this.topic, DATABASE_URL: process.env.DATABASE_URL!, ...env });
    this.procs.push(p);
    return p;
  }

  relay(env: Record<string, string> = {}): NodeProcess {
    const p = spawnService('outbox-relay', 'outbox-relay', {
      DATABASE_URL: process.env.DATABASE_URL!,
      RELAY_TOPICS: this.topic,
      RELAY_LOCK_KEY: String(this.lockKey),
      RELAY_POLL_MS: '50',
      RELAY_STANDBY_POLL_MS: '200',
      RELAY_BACKOFF_INITIAL_MS: '100',
      RELAY_BACKOFF_MAX_MS: '2000',
      OUTBOX_GAUGE_POLL_MS: '250',
      ...env,
    });
    this.procs.push(p);
    return p;
  }

  async put(admin: NodeProcess, config: Record<string, unknown>, ifMatch?: string, tenant = this.tenant): Promise<{ status: number | 'network_error'; body?: any; etag?: string | null }> {
    try {
      const r = await fetch(admin.url(`/v1/admin/tenants/${tenant}/config`), {
        method: 'PUT',
        headers: { authorization: `Bearer ${ADMIN_KEY}`, 'content-type': 'application/json', ...(ifMatch ? { 'if-match': ifMatch } : {}) },
        body: JSON.stringify({ config }),
        signal: AbortSignal.timeout(15_000),
      });
      return { status: r.status, body: await r.json().catch(() => null), etag: r.headers.get('etag') };
    } catch {
      return { status: 'network_error' };
    }
  }

  /**
   * n config changes written the way admin-api writes them (upsert + enqueue in one transaction each), without
   * HTTP: for the relay tests, which are about the relay. Returns the versions written.
   */
  async writeChanges(n: number): Promise<number[]> {
    const versions: number[] = [];
    const c = await this.pool.connect();
    try {
      for (let i = 0; i < n; i++) {
        await c.query('BEGIN');
        const config = { rate_limit_per_s: i + 1 };
        const up = await c.query(
          `INSERT INTO tenant_config (tenant_id, config, version) VALUES ($1, $2, 1)
           ON CONFLICT (tenant_id) DO UPDATE SET config = EXCLUDED.config, version = tenant_config.version + 1, updated_at = now()
           RETURNING version, updated_at`,
          [this.tenant, JSON.stringify(config)],
        );
        const version = Number(up.rows[0].version);
        const eventId = randomUUID();
        await enqueue(c, {
          aggregateId: this.tenant,
          topic: this.topic,
          key: this.tenant,
          payload: this.serde.encode({ eventId, tenantId: this.tenant, version, changedAt: up.rows[0].updated_at, config }),
          headers: { event_id: eventId, version: String(version) },
        });
        await c.query('COMMIT');
        versions.push(version);
      }
    } finally {
      c.release();
    }
    return versions;
  }

  async configRow(): Promise<{ version: number; config: any } | null> {
    const r = await this.pool.query('SELECT version, config FROM tenant_config WHERE tenant_id = $1', [this.tenant]);
    return r.rows[0] ? { version: Number(r.rows[0].version), config: r.rows[0].config } : null;
  }

  async outboxRows(): Promise<{ id: string; attempts: number; published_at: Date | null; headers: Record<string, string> }[]> {
    return (await this.pool.query('SELECT id::text, attempts, published_at, headers FROM outbox WHERE topic = $1 ORDER BY id', [this.topic])).rows;
  }

  async counts(): Promise<{ rows: number; published: number; maxAttempts: number }> {
    const r = await this.pool.query('SELECT count(*)::int AS rows, count(published_at)::int AS published, coalesce(max(attempts), 0)::int AS max FROM outbox WHERE topic = $1', [this.topic]);
    return { rows: r.rows[0].rows, published: r.rows[0].published, maxAttempts: r.rows[0].max };
  }

  /** Reads the lane's topic back (until `until`), decodes every message with the registered schema, in offset order. */
  async read(timeoutMs: number, until: (r: TopicRecord[]) => boolean): Promise<DecodedChange[]> {
    const recs = (await readTopic(this.topic, timeoutMs, until)).sort((a, b) => a.partition - b.partition || Number(a.offset) - Number(b.offset));
    const out: DecodedChange[] = [];
    for (const r of recs) out.push({ ...(await this.serde.decode(r.value!)), partition: r.partition, offset: r.offset, outboxId: r.headers.outbox_id, headers: r.headers });
    return out;
  }

  /** Apply what was read like a Stage 4 consumer would (applyIfNewer, dedup on event_id). */
  static apply(changes: DecodedChange[]): VersionedView<Record<string, unknown>> {
    const view = new VersionedView<Record<string, unknown>>();
    for (const c of changes) view.apply(c.tenantId, { eventId: c.eventId, version: c.version, value: c.config });
    return view;
  }

  async cleanup() {
    await Promise.all(this.procs.map((p) => p.kill()));
    await this.pool.query('DELETE FROM outbox WHERE topic = $1', [this.topic]).catch(() => undefined);
    await this.pool.query('DELETE FROM tenant_config WHERE tenant_id = $1', [this.tenant]).catch(() => undefined);
    await this.pool.query('DELETE FROM tenants WHERE tenant_id = $1', [this.tenant]).catch(() => undefined);
    await deleteTopic(this.topic);
    await this.pool.end();
  }
}
