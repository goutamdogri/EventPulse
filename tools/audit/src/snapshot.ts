// I/O side of the audit: snapshots of Kafka offsets, Postgres and the services' /debug/stats, plus the end-of-run
// facts (drain wait, run-scoped rows, DLQ records of the run). Read-only: nothing here writes anywhere.

import { readFileSync, existsSync } from 'node:fs';
import { resolve } from 'node:path';
import { randomUUID } from 'node:crypto';
import pg from 'pg';
import { createKafka, loadKafkaClientConfig } from '@eventpulse/kafka-config';
import { groupLag } from './checks.ts';
import type { ApiStats, ConsumerStats, IdemCounts, PartitionOffsets, RunFacts, Snapshot } from './types.ts';

export const REPO = resolve(import.meta.dirname, '../../..');

function envFileValue(path: string, key: string): string | undefined {
  if (!existsSync(path)) return undefined;
  const line = readFileSync(path, 'utf8').split('\n').find((l) => l.startsWith(`${key}=`));
  return line?.slice(key.length + 1).trim() || undefined;
}

export interface AuditConfig {
  topic: string;
  dlqTopic: string;
  group: string;
  databaseUrl: string;
  apiStatsUrl: string;
  consumerStatsUrls: string[];
  drainTimeoutS: number;
  resultsDir: string;
}

export function loadAuditConfig(env = process.env): AuditConfig {
  const databaseUrl = env.AUDIT_DATABASE_URL || envFileValue(resolve(REPO, 'services/consumer-sink/.env'), 'DATABASE_URL');
  if (!databaseUrl) throw new Error('No database URL: set AUDIT_DATABASE_URL or DATABASE_URL in services/consumer-sink/.env');
  return {
    topic: env.AUDIT_TOPIC || 'events.raw',
    dlqTopic: env.AUDIT_DLQ_TOPIC || 'events.dlq',
    group: env.AUDIT_GROUP || 'sink-postgres',
    databaseUrl,
    apiStatsUrl: env.API_STATS_URL || 'http://localhost:8000/debug/stats',
    consumerStatsUrls: (env.CONSUMER_STATS_URLS || 'http://localhost:8001/debug/stats,http://localhost:8002/debug/stats,http://localhost:8003/debug/stats')
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean),
    drainTimeoutS: Number(env.AUDIT_DRAIN_TIMEOUT_S || 120),
    resultsDir: env.AUDIT_RESULTS_DIR || resolve(REPO, 'loadtests/results'),
  };
}

/** The API key k6 should use: API_KEY, else the first key in services/ingest-api/.env. */
export function defaultApiKey(env = process.env): string | undefined {
  return env.API_KEY || envFileValue(resolve(REPO, 'services/ingest-api/.env'), 'API_KEYS')?.split(',')[0]?.split(':')[0]?.trim();
}

const kafka = () => createKafka({ ...loadKafkaClientConfig(process.env, 'eventpulse-audit'), logLevel: 'nothing' });

async function withAdmin<T>(fn: (a: ReturnType<ReturnType<typeof kafka>['admin']>) => Promise<T>): Promise<T> {
  const admin = kafka().admin();
  await admin.connect();
  try {
    return await fn(admin);
  } finally {
    await admin.disconnect();
  }
}

async function getJson(url: string, timeoutMs = 2000): Promise<any | null> {
  try {
    const r = await fetch(url, { signal: AbortSignal.timeout(timeoutMs) });
    return r.ok ? await r.json() : null;
  } catch {
    return null;
  }
}

async function kafkaState(cfg: AuditConfig) {
  return withAdmin(async (a) => {
    const toParts = (xs: any[]): PartitionOffsets[] =>
      xs.map((x) => ({ partition: x.partition, low: Number(x.low), high: Number(x.high) })).sort((p, q) => p.partition - q.partition);
    const raw = toParts(await a.fetchTopicOffsets(cfg.topic));
    const dlq = toParts(await a.fetchTopicOffsets(cfg.dlqTopic));
    const off = await a.fetchOffsets({ groupId: cfg.group, topics: [cfg.topic] });
    const committed = ((off[0]?.partitions ?? []) as any[]).map((p) => ({ partition: p.partition, offset: Number(p.offset) }));
    return { raw, dlq, committed };
  });
}

export async function takeSnapshot(cfg: AuditConfig): Promise<Snapshot> {
  const notes: string[] = [];
  const k = await kafkaState(cfg);
  const client = new pg.Client({ connectionString: cfg.databaseUrl });
  await client.connect();
  let rows_total = 0;
  try {
    rows_total = Number((await client.query('select count(*) from events')).rows[0].count);
  } finally {
    await client.end();
  }

  const a = await getJson(cfg.apiStatsUrl);
  const api: ApiStats | null = a
    ? {
        url: cfg.apiStatsUrl,
        uptime_s: a.uptime_s,
        acked: a.producer?.acked ?? 0,
        failed: a.producer?.failed ?? 0,
        http_202: a.http?.by_status?.['202'] ?? 0,
        by503: a.http?.['503_by_reason'] ?? {},
        ...(a.idempotency?.by_result ? { idem: idemCounts(a.idempotency.by_result) } : {}),
      }
    : null;
  if (!api) notes.push(`API stats not reachable at ${cfg.apiStatsUrl}`);

  const consumers: ConsumerStats[] = [];
  for (const url of cfg.consumerStatsUrls) {
    const c = await getJson(url);
    if (c) {
      const byCause = c.duplicates_by_cause;
      consumers.push({
        url,
        uptime_s: c.uptime_s,
        consumed: c.consumed,
        attempted: c.attempted,
        inserted: c.inserted,
        duplicates: c.duplicates,
        dlq: c.dlq,
        ...(byCause ? { dup_reprocessed: byCause.reprocessed ?? 0, dup_redelivered: byCause.redelivered ?? 0, dup_unknown: byCause.unknown ?? 0, conflicts: c.conflicting_duplicates ?? 0 } : {}),
      });
    }
  }
  if (consumers.length === 0) notes.push('no consumer /debug/stats reachable');

  return { taken_at: new Date().toISOString(), topic: cfg.topic, dlq_topic: cfg.dlqTopic, group: cfg.group, ...k, db: { rows_total }, api, consumers, notes };
}

function idemCounts(b: Record<string, number>): IdemCounts {
  const n = (k: string) => Number(b[k] ?? 0);
  return { new: n('new'), replayed: n('replayed'), in_progress: n('in_progress'), payload_mismatch: n('payload_mismatch'), fail_open: n('fail_open'), fail_closed: n('fail_closed'), disabled: n('disabled') };
}

/** Poll the group lag until it is 0 or the timeout passes. Every wait is bounded. */
export async function waitDrained(cfg: AuditConfig, log: (s: string) => void): Promise<{ drained: boolean; lag: number; waitedS: number }> {
  const t0 = Date.now();
  let lag = Infinity;
  let lastPrint = 0;
  while (Date.now() - t0 < cfg.drainTimeoutS * 1000) {
    const k = await kafkaState(cfg);
    lag = groupLag(k.raw, k.committed);
    if (lag === 0) break;
    if (Date.now() - lastPrint > 5000) {
      log(`  waiting for the consumer to drain: lag ${lag}`);
      lastPrint = Date.now();
    }
    await new Promise((r) => setTimeout(r, 1000));
  }
  return { drained: lag === 0, lag: Number.isFinite(lag) ? lag : -1, waitedS: Math.round((Date.now() - t0) / 1000) };
}

/** Distinct rows of this run (event_id prefix = run id) and their freshness, through the primary key index. */
export async function runRows(cfg: AuditConfig, runId: string) {
  const client = new pg.Client({ connectionString: cfg.databaseUrl });
  await client.connect();
  try {
    const r = await client.query(
      `select count(distinct event_id)::int as n,
              percentile_cont(0.5) within group (order by extract(epoch from stored_at - received_at)) as p50,
              percentile_cont(0.99) within group (order by extract(epoch from stored_at - received_at)) as p99
         from events
        where event_id between $1::uuid and $2::uuid`,
      [`${runId}-0000-0000-0000-000000000000`, `${runId}-ffff-ffff-ffff-ffffffffffff`],
    );
    const row = r.rows[0];
    const f = (x: unknown) => (x === null || x === undefined ? null : Number(Number(x).toFixed(3)));
    return { rows: row.n as number, p50: f(row.p50), p99: f(row.p99) };
  } finally {
    await client.end();
  }
}

/** DLQ records added between two snapshots: how many, and how many carry this run's event_id prefix. */
export async function dlqRecordsOfRun(cfg: AuditConfig, start: Snapshot, end: Snapshot, runId: string) {
  const total = end.dlq.reduce((a, p) => a + p.high, 0) - start.dlq.reduce((a, p) => a + p.high, 0);
  if (total <= 0) return { total: Math.max(0, total), run: 0 };
  const from = new Map(start.dlq.map((p) => [p.partition, p.high]));
  const to = new Map(end.dlq.map((p) => [p.partition, p.high]));
  const consumer = kafka().consumer({ kafkaJS: { groupId: `eventpulse-audit-${randomUUID()}`, fromBeginning: true, autoCommit: false } });
  let seen = 0;
  let run = 0;
  await consumer.connect();
  try {
    await consumer.subscribe({ topics: [cfg.dlqTopic] });
    const seeked = new Set<number>();
    await consumer.run({
      eachMessage: async ({ partition, message }) => {
        const off = Number(message.offset);
        if (off < (from.get(partition) ?? 0) || off >= (to.get(partition) ?? 0)) return;
        seen++;
        try {
          const id = JSON.parse(message.value?.toString() ?? '').event_id;
          if (typeof id === 'string' && id.toLowerCase().startsWith(`${runId}-`)) run++;
        } catch {
          /* not JSON (e.g. injected invalid-json poison): not from this run */
        }
      },
    });
    const deadline = Date.now() + 15_000;
    while (Date.now() < deadline && seen < total) {
      for (const tp of consumer.assignment()) {
        if (seeked.has(tp.partition)) continue;
        consumer.seek({ topic: cfg.dlqTopic, partition: tp.partition, offset: String(from.get(tp.partition) ?? 0) });
        seeked.add(tp.partition);
      }
      await new Promise((r) => setTimeout(r, 200));
    }
  } finally {
    await consumer.disconnect();
  }
  return { total, run };
}

export async function runFacts(cfg: AuditConfig, start: Snapshot, end: Snapshot, runId: string, drain: { drained: boolean; lag: number; waitedS: number }): Promise<RunFacts> {
  const rows = await runRows(cfg, runId);
  const dlq = await dlqRecordsOfRun(cfg, start, end, runId);
  return {
    run_id: runId,
    rows_distinct: rows.rows,
    freshness_p50_s: rows.p50,
    freshness_p99_s: rows.p99,
    dlq_new_total: dlq.total,
    dlq_run: dlq.run,
    lag_end: drain.lag,
    drained: drain.drained,
    drain_waited_s: drain.waitedS,
  };
}
