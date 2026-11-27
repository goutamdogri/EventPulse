// S2-F2 plan tests that were missing:
//   - E2E through the REAL API: POST -> ingest-api -> Kafka -> consumer-sink -> Postgres (bounded delay).
//   - Members beyond the partition count get no assignment and sit idle.
// Both run the built services as child processes on a throw-away topic, against the test database.
import { spawn } from 'node:child_process';
import { randomUUID } from 'node:crypto';
import { resolve } from 'node:path';
import {
  Db,
  createTopic,
  deleteTopic,
  fullyCommitted,
  produceEvents,
  sleep,
  spawnSink,
  waitFor,
  waitReady,
} from './process-helpers.js';
import type { SinkProcess } from './process-helpers.js';

describe('pipeline (real processes)', () => {
  const db = new Db();
  const topics: string[] = [];
  const procs: SinkProcess[] = [];
  afterEach(async () => {
    await Promise.all(procs.splice(0).map((p) => p.stop()));
  });
  afterAll(async () => {
    await Promise.all(topics.map(deleteTopic));
    await db.end();
  });

  it('POST 1000 events to the real API: all 1000 rows in Postgres within a bounded delay', async () => {
    const topic = await createTopic(6);
    topics.push(topic);
    const group = `sink-e2e-${randomUUID()}`;
    const port = 18950 + Math.floor(Math.random() * 40);

    const api = spawn(process.execPath, ['dist/main.js'], {
      cwd: resolve(import.meta.dirname, '../../ingest-api'),
      env: { ...process.env, PORT: String(port), API_KEYS: 'e2e_key:pipeline_e2e', KAFKA_TOPIC: topic, KAFKA_KEY_MODE: 'event', LOG_ENABLED: 'false' },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    const apiLog: string[] = [];
    api.stdout.on('data', (d) => apiLog.push(String(d)));
    api.stderr.on('data', (d) => apiLog.push(String(d)));
    const sink = spawnSink({ CONSUMER_TOPIC: topic, CONSUMER_GROUP_ID: group });
    procs.push(sink);

    try {
      await waitFor(async () => {
        try {
          return (await fetch(`http://localhost:${port}/readyz`)).ok;
        } catch {
          return false;
        }
      }, 30_000, `API ready\n${apiLog.join('')}`);
      await waitReady(sink);

      const ids = Array.from({ length: 1000 }, () => randomUUID());
      const t0 = Date.now();
      const statuses = await Promise.all(
        ids.map((id) =>
          fetch(`http://localhost:${port}/v1/events`, {
            method: 'POST',
            headers: { authorization: 'Bearer e2e_key', 'content-type': 'application/json' },
            body: JSON.stringify({ event_id: id, type: 'pipeline.e2e', occurred_at: new Date().toISOString(), schema_version: 1, payload: { ok: true } }),
          }).then((r) => r.status),
        ),
      );
      expect(statuses.every((s) => s === 202)).toBe(true);

      await waitFor(async () => (await db.countRows(ids)) === 1000, 10_000, '1000 rows within 10 s of the first POST');
      const visibleMs = Date.now() - t0;
      const r = (
        await db.pool.query(
          `select count(*) filter (where stored_at >= received_at) as fresh_ok,
                  count(*) filter (where tenant_id = 'pipeline_e2e') as tenant_ok,
                  max(extract(epoch from stored_at - received_at)) as max_fresh_s
             from events where event_id = any($1::uuid[])`,
          [ids],
        )
      ).rows[0];
      expect(Number(r.fresh_ok)).toBe(1000);
      expect(Number(r.tenant_ok)).toBe(1000); // tenant from the API key, through the envelope, into the row
      console.info(`[pipeline] 1000 events visible ${visibleMs} ms after the first POST, max freshness ${Number(r.max_fresh_s).toFixed(3)} s`);
      await waitFor(() => fullyCommitted(group, topic), 10_000, 'offsets committed');
    } finally {
      api.kill('SIGTERM');
    }
  });

  it('3 members on a 2-partition topic: exactly 2 get a partition, the third is idle, and delivery works', async () => {
    const topic = await createTopic(2);
    topics.push(topic);
    const group = `sink-e2e-${randomUUID()}`;
    const env = { CONSUMER_TOPIC: topic, CONSUMER_GROUP_ID: group, CONSUMER_SESSION_TIMEOUT_MS: '10000' };
    const members = [spawnSink(env), spawnSink(env), spawnSink(env)];
    procs.push(...members);
    await Promise.all(members.map((m) => waitReady(m, 60_000)));

    let counts: number[] = [];
    await waitFor(
      async () => {
        counts = (await Promise.all(members.map((m) => m.stats()))).map((s) => s.assigned_partitions.length).sort((x, y) => x - y);
        return counts.join(',') === '0,1,1';
      },
      45_000,
      `assignments 0,1,1 (last seen ${counts.join(',')})`,
    );

    const ids = await produceEvents(topic, 500);
    await waitFor(async () => (await db.countRows(ids)) === 500, 30_000, '500 rows');
    await sleep(500);
    const stats = await Promise.all(members.map((m) => m.stats()));
    const idle = stats.find((s) => s.assigned_partitions.length === 0)!;
    expect(idle.consumed).toBe(0); // the extra member did no work
  });

  it('S2-F6 consumer metrics after a run: lag 0, inserted = new rows, freshness samples; a 2nd member -> rebalances', async () => {
    const topic = await createTopic(6);
    topics.push(topic);
    const group = `sink-e2e-${randomUUID()}`;
    const env = { CONSUMER_TOPIC: topic, CONSUMER_GROUP_ID: group, LAG_POLL_MS: '500', CONSUMER_SESSION_TIMEOUT_MS: '10000' };
    const a = spawnSink(env);
    procs.push(a);
    await waitReady(a);

    const ids = await produceEvents(topic, 600);
    await waitFor(async () => (await db.countRows(ids)) === 600, 30_000, '600 rows');
    await waitFor(async () => (await a.stats()).lag?.total === 0, 15_000, 'polled group lag 0');
    const s = await a.stats();
    expect(s.inserted).toBe(600); // equals the new rows
    expect(s.consumed).toBe(s.inserted + s.duplicates + s.dlq); // the invariant, live
    expect(s.freshness_p99_s).not.toBeNull();
    expect(Object.keys(s.lag.by_partition)).toHaveLength(6);
    expect(s.batch.commit_p99_ms).not.toBeNull();
    const text = await (await fetch(`http://localhost:${a.port}/metrics`)).text();
    expect(text).toMatch(/ep_event_freshness_seconds_count 600/);
    expect(text).toContain('ep_consumer_lag{partition="0"} 0');
    expect(text).toContain('ep_consumer_assigned_partitions 6');

    const before = s.rebalances;
    const b = spawnSink(env);
    procs.push(b);
    await waitReady(b, 60_000);
    await waitFor(
      async () => {
        const [sa, sb] = [await a.stats(), await b.stats()];
        return sa.rebalances > before && sa.assigned_partitions.length > 0 && sb.assigned_partitions.length > 0 && sa.assigned_partitions.length + sb.assigned_partitions.length === 6;
      },
      45_000,
      'rebalance: both members own partitions',
    );
    expect((await a.stats()).assigned_partitions.length).toBeLessThan(6); // changed in both snapshots
  });
});
