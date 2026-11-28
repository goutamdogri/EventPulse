// S3-F4 end to end: admin-api + outbox-relay as real processes, against the test database and throw-away topics.
// The dual-write problem is gone: a committed change is published at least once, a rolled-back one never.
import { sleep, waitFor } from '../src/harness.js';
import { ADMIN_KEY, OutboxLane, metricValue, orderViolations, recordMeasurement, waitHttpReady } from '../src/outbox.js';

describe('S3-F4 admin-api: config change + outbox row in one transaction', () => {
  let lane: OutboxLane;
  beforeAll(async () => {
    lane = await OutboxLane.create();
  });
  afterAll(async () => {
    await lane.cleanup();
  });

  it('[G7.commit-published] PUT config: version + 1, exactly 1 outbox row, the relay publishes it, it decodes with the registered schema', async () => {
    const admin = lane.admin();
    const relay = lane.relay();
    await Promise.all([waitHttpReady(admin), waitHttpReady(relay)]);

    const r1 = await lane.put(admin, { rate_limit_per_s: 100, features: { beta_ui: true } });
    expect(r1.status).toBe(200);
    expect(r1.body).toMatchObject({ tenant_id: lane.tenant, version: 1, config: { rate_limit_per_s: 100, features: { beta_ui: true } } });
    expect(r1.etag).toBe('"1"');
    const r2 = await lane.put(admin, { rate_limit_per_s: 200 }, '"1"');
    expect(r2.status).toBe(200);
    expect(r2.body.version).toBe(2);

    expect(await lane.configRow()).toEqual({ version: 2, config: { rate_limit_per_s: 200 } });
    const rows = await lane.outboxRows();
    expect(rows).toHaveLength(2); // exactly one per change
    expect(rows.map((x) => x.headers.version)).toEqual(['1', '2']);

    const got = await lane.read(20_000, (rs) => rs.length >= 2);
    expect(got.map((c) => c.version)).toEqual([1, 2]);
    expect(got[1]).toMatchObject({ tenantId: lane.tenant, eventId: r2.body.event_id, config: { rate_limit_per_s: 200 }, outboxId: r2.body.outbox_id });
    expect(got[1].headers).toMatchObject({ event_id: r2.body.event_id, version: '2', 'content-type': 'application/vnd.eventpulse.config-changed+avro' });
    expect(new Set(got.map((c) => c.partition)).size).toBe(1); // key = tenant: one partition, order kept
    await waitFor(async () => (await lane.counts()).published === 2, 10_000, 'rows marked published');

    // GET reads the committed state with its ETag.
    const g = await fetch(admin.url(`/v1/admin/tenants/${lane.tenant}/config`), { headers: { authorization: `Bearer ${ADMIN_KEY}` } });
    expect(g.status).toBe(200);
    expect(g.headers.get('etag')).toBe('"2"');
    expect(await g.json()).toMatchObject({ version: 2, config: { rate_limit_per_s: 200 } });
    // Publish latency is measured.
    expect(await metricValue(relay, 'ep_outbox_publish_latency_seconds_count')).toBe(2);
    expect(await metricValue(relay, 'ep_outbox_relay_is_leader')).toBe(1);
    await relay.stop();
    await admin.stop();
  });

  it('[G7.no-enqueue-on-reject] optimistic concurrency and validation: 412 stale If-Match, 404 unknown tenant, 400 bad body, 401 without the admin key; none of them enqueues', async () => {
    const admin = lane.admin();
    await waitHttpReady(admin);
    const before = (await lane.counts()).rows;
    const cur = (await lane.configRow())!.version;

    // Two admins read version cur; the first write wins, the second gets 412 instead of silently overwriting.
    expect((await lane.put(admin, { retention_days: 7 }, `"${cur}"`)).status).toBe(200);
    const stale = await lane.put(admin, { retention_days: 9 }, `"${cur}"`);
    expect(stale.status).toBe(412);
    expect(stale.body.current_version).toBe(cur + 1);
    expect((await lane.configRow())!.config).toEqual({ retention_days: 7 });

    expect((await lane.put(admin, { retention_days: 7 }, undefined, 'no_such_tenant')).status).toBe(404);
    expect((await lane.put(admin, { retention_dayz: 7 })).status).toBe(400);
    expect((await lane.put(admin, { retention_days: 7 }, 'garbage')).status).toBe(400);
    const noAuth = await fetch(admin.url(`/v1/admin/tenants/${lane.tenant}/config`), { method: 'PUT', headers: { 'content-type': 'application/json' }, body: '{"config":{}}' });
    expect(noAuth.status).toBe(401);
    expect((await lane.counts()).rows).toBe(before + 1); // only the one successful PUT
    expect(await metricValue(admin, 'ep_admin_config_puts_total')).toBe(6);
    await admin.stop();
  });

  it('[G7.rollback-error] rolled back (injected error after the outbox insert, before COMMIT): no config change, no outbox row, nothing published', async () => {
    const admin = lane.admin({ DEBUG_FAULTS: 'true', FAULT_ERRORS: 'admin:after-update-before-commit=XX000x1' });
    await waitHttpReady(admin);
    const before = await lane.configRow();
    const rowsBefore = (await lane.counts()).rows;
    const r = await lane.put(admin, { rate_limit_per_s: 999 });
    expect(r.status).toBe(500);
    expect(await lane.configRow()).toEqual(before);
    expect((await lane.counts()).rows).toBe(rowsBefore);
    // The next PUT (fault used up) works and takes the next version: no gap, no ghost event.
    const ok = await lane.put(admin, { rate_limit_per_s: 1000 });
    expect(ok.body.version).toBe(before!.version + 1);
    await admin.stop();
  });

  it('[G7.rollback-crash] crash at admin:after-update-before-commit: the process dies with the transaction open, nothing commits, nothing is published', async () => {
    const relay = lane.relay();
    await waitHttpReady(relay);
    const published = async () => (await lane.counts()).published;
    await waitFor(async () => (await published()) === (await lane.counts()).rows, 10_000, 'backlog drained');
    const before = await lane.configRow();
    const rowsBefore = (await lane.counts()).rows;

    const crashing = lane.admin({ DEBUG_FAULTS: 'true', FAULTS: 'admin:after-update-before-commit' });
    await waitHttpReady(crashing);
    expect((await lane.put(crashing, { rate_limit_per_s: 31337 })).status).toBe('network_error');
    const exit = await crashing.exited;
    expect(exit.signal).toBe('SIGKILL');
    expect(crashing.log.join('')).toContain('CRASH_AT=admin:after-update-before-commit');

    await sleep(1000); // give the relay every chance to publish something it should not
    expect(await lane.configRow()).toEqual(before);
    expect((await lane.counts()).rows).toBe(rowsBefore);
    const got = await lane.read(5_000, () => false);
    expect(got.some((c) => c.config.rate_limit_per_s === 31337)).toBe(false);
    expect(got).toHaveLength(rowsBefore); // everything committed was published, nothing else
    await relay.stop();
  });
});

describe('S3-F4 outbox-relay: at least once, crash points', () => {
  it('[G7.relay-before-publish] crash at relay:before-publish: published after the restart, 0 lost, 0 duplicates sent', async () => {
    const lane = await OutboxLane.create();
    try {
      await lane.writeChanges(120);
      const crashing = lane.relay({ RELAY_BATCH_SIZE: '50', DEBUG_FAULTS: 'true', FAULTS: 'relay:before-publish@b2' });
      expect((await crashing.exited).signal).toBe('SIGKILL');
      const atCrash = await lane.counts();
      expect(atCrash.published).toBe(50); // batch 1 done, batch 2 claimed but never produced
      const relay = lane.relay({ RELAY_BATCH_SIZE: '50' });
      await waitFor(async () => (await lane.counts()).published === 120, 30_000, 'all published');
      const got = await lane.read(20_000, (rs) => rs.length >= 120);
      expect(got).toHaveLength(120);
      expect(new Set(got.map((c) => c.eventId)).size).toBe(120);
      expect(got.map((c) => c.version)).toEqual(Array.from({ length: 120 }, (_, i) => i + 1));
      // The claimed batch shows attempts 2 (one attempt never reached Kafka): an upper bound, not a duplicate.
      expect((await lane.counts()).maxAttempts).toBe(2);
      expect(await metricValue(relay, 'ep_outbox_republished_total')).toBe(50);
      await relay.stop();
    } finally {
      await lane.cleanup();
    }
  });

  it('[G7.relay-after-publish] crash at relay:after-publish-before-mark: the batch is published twice, attempts = 2, the consumer applies each change once', async () => {
    const lane = await OutboxLane.create();
    try {
      await lane.writeChanges(30);
      const crashing = lane.relay({ RELAY_BATCH_SIZE: '10', DEBUG_FAULTS: 'true', FAULTS: 'relay:after-publish-before-mark@b2' });
      expect((await crashing.exited).signal).toBe('SIGKILL');
      expect((await lane.counts()).published).toBe(10);
      const relay = lane.relay({ RELAY_BATCH_SIZE: '10' });
      await waitFor(async () => (await lane.counts()).published === 30, 30_000, 'all published');
      const got = await lane.read(20_000, (rs) => rs.length >= 40);
      expect(got).toHaveLength(40); // 30 + the 10 of batch 2 published again
      const rows = await lane.outboxRows();
      expect(rows.filter((r) => r.attempts === 2)).toHaveLength(10);
      expect(await metricValue(relay, 'ep_outbox_republished_total')).toBe(10);
      const view = OutboxLane.apply(got);
      expect(view.counts).toEqual({ applied: 30, duplicate: 10, stale: 0 });
      expect(view.get(lane.tenant)!.version).toBe(30);
      await relay.stop();
    } finally {
      await lane.cleanup();
    }
  });

  it('[G7.relay-after-mark] crash at relay:after-mark-before-commit: the mark rolls back with the connection, the batch is republished', async () => {
    const lane = await OutboxLane.create();
    try {
      await lane.writeChanges(30);
      const crashing = lane.relay({ RELAY_BATCH_SIZE: '10', DEBUG_FAULTS: 'true', FAULTS: 'relay:after-mark-before-commit@b2' });
      expect((await crashing.exited).signal).toBe('SIGKILL');
      expect((await lane.counts()).published).toBe(10); // the UPDATE of batch 2 never committed
      const relay = lane.relay({ RELAY_BATCH_SIZE: '10' });
      await waitFor(async () => (await lane.counts()).published === 30, 30_000, 'all published');
      const view = OutboxLane.apply(await lane.read(20_000, (rs) => rs.length >= 40));
      expect(view.counts).toEqual({ applied: 30, duplicate: 10, stale: 0 });
      await relay.stop();
    } finally {
      await lane.cleanup();
    }
  });
});

describe('S3-F4 two relays', () => {
  const N = 1000;

  it(`[G7.single-leader] single-leader mode, ${N} ordered changes of one tenant: version order on the topic; the standby takes over when the leader is killed`, async () => {
    const lane = await OutboxLane.create();
    try {
      const a = lane.relay({ RELAY_BATCH_SIZE: '20' });
      await waitHttpReady(a);
      await waitFor(async () => (await metricValue(a, 'ep_outbox_relay_is_leader')) === 1, 10_000, 'a leads');
      const b = lane.relay({ RELAY_BATCH_SIZE: '20' });
      await waitHttpReady(b);
      expect(await metricValue(b, 'ep_outbox_relay_is_leader')).toBe(0); // standby

      const writing = lane.writeChanges(N);
      await waitFor(async () => (await lane.counts()).published >= N / 3, 60_000, 'a third published by a');
      await a.kill(); // SIGKILL: its session dies, Postgres releases the advisory lock
      await waitFor(async () => (await metricValue(b, 'ep_outbox_relay_is_leader')) === 1, 10_000, 'b takes over');
      await writing;
      await waitFor(async () => (await lane.counts()).published === N, 60_000, 'all published by b');
      expect((await b.json('/debug/stats')).relay.published).toBeGreaterThan(0);

      const got = await lane.read(30_000, (rs) => rs.length >= N);
      expect(new Set(got.map((c) => c.eventId)).size).toBe(N); // nothing lost
      expect(orderViolations(got)).toBe(0);
      const view = OutboxLane.apply(got);
      expect(view.counts.stale).toBe(0);
      expect(view.get(lane.tenant)!.version).toBe(N);
      recordMeasurement('single_leader_failover', { changes: N, messages: got.length, republished_after_kill: got.length - N, order_violations: 0, published_by_standby: (await b.json('/debug/stats')).relay.published });
      await b.stop();
    } finally {
      await lane.cleanup();
    }
  });

  it(`[G7.multi-mode] RELAY_MODE=multi, ${N} changes: order violations measured (not asserted), the consumer still ends on the latest version`, async () => {
    const lane = await OutboxLane.create();
    try {
      await lane.writeChanges(N);
      const env = { RELAY_MODE: 'multi', RELAY_BATCH_SIZE: '5', RELAY_LISTEN: 'false' };
      const relays = [lane.relay(env), lane.relay(env)];
      await waitFor(async () => (await lane.counts()).published === N, 60_000, 'all published');
      const got = await lane.read(30_000, (rs) => rs.length >= N);
      const perRelay = await Promise.all(relays.map(async (r) => (await r.json('/debug/stats')).relay.published));
      const violations = orderViolations(got);
      const view = OutboxLane.apply(got);
      recordMeasurement('multi_mode_ordering', { changes: N, messages: got.length, order_violations: violations, published_per_relay: perRelay, consumer: view.counts, final_version: view.get(lane.tenant)!.version });
      expect(new Set(got.map((c) => c.eventId)).size).toBe(N);
      expect(view.get(lane.tenant)!.version).toBe(N); // the version field protects the consumer anyway
      expect(view.counts.applied + view.counts.stale + view.counts.duplicate).toBe(got.length);
      await Promise.all(relays.map((r) => r.stop()));
    } finally {
      await lane.cleanup();
    }
  });

  it('[G7.retention] graceful stop releases the lock at once and the backlog gauge reads 0 when drained; retention deletes old published rows', async () => {
    const lane = await OutboxLane.create();
    try {
      await lane.writeChanges(20);
      const a = lane.relay({ RELAY_RETENTION_MS: '1', RELAY_RETENTION_INTERVAL_MS: '100' });
      await waitHttpReady(a);
      // Retention of 1 ms deletes rows right after they are published: count on the relay, not on the table.
      await waitFor(async () => (await a.json('/debug/stats')).relay.published === 20, 20_000, 'published');
      await waitFor(async () => (await lane.counts()).rows === 0, 10_000, 'retention deleted the published rows');
      await waitFor(async () => (await metricValue(a, 'ep_outbox_unpublished')) === 0, 5_000, 'gauge 0');
      expect(await metricValue(a, 'ep_outbox_retention_deleted_total')).toBe(20);
      const b = lane.relay();
      await waitHttpReady(b);
      const t0 = Date.now();
      expect((await a.stop()).code).toBe(0);
      await waitFor(async () => (await metricValue(b, 'ep_outbox_relay_is_leader')) === 1, 5_000, 'b leads after a stopped');
      expect(Date.now() - t0).toBeLessThan(3_000);
      await b.stop();
    } finally {
      await lane.cleanup();
    }
  });
});

describe('S3-F4 publish latency (commit -> published), LISTEN vs polling only', () => {
  it.each([
    ['listen', { RELAY_LISTEN: 'true', RELAY_POLL_MS: '200' }],
    ['poll_200ms', { RELAY_LISTEN: 'false', RELAY_POLL_MS: '200' }],
  ] as const)('[G7.latency] %s: 100 changes 20 ms apart, latency from the outbox row itself', async (name, env) => {
    const lane = await OutboxLane.create();
    try {
      const relay = lane.relay(env);
      await waitHttpReady(relay);
      for (let i = 0; i < 100; i++) {
        await lane.writeChanges(1);
        await sleep(20);
      }
      await waitFor(async () => (await lane.counts()).published === 100, 20_000, 'published');
      const q = await lane.pool.query(
        `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM published_at - created_at)) * 1000 AS p50,
                percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM published_at - created_at)) * 1000 AS p99,
                max(extract(epoch FROM published_at - created_at)) * 1000 AS max
           FROM outbox WHERE topic = $1`,
        [lane.topic],
      );
      const ms = (v: unknown) => Math.round(Number(v) * 10) / 10;
      const r = { p50_ms: ms(q.rows[0].p50), p99_ms: ms(q.rows[0].p99), max_ms: ms(q.rows[0].max) };
      recordMeasurement(`publish_latency_${name}`, { changes: 100, ...r, note: 'created_at is the transaction start of the change, published_at the transaction start of the publishing cycle' });
      // Woken by NOTIFY, not by the 200 ms poll; also a NOTIFY that arrives during a cycle is not lost (p99).
      if (name === 'listen') expect(r.p99_ms).toBeLessThan(100);
      await relay.stop();
    } finally {
      await lane.cleanup();
    }
  });
});
