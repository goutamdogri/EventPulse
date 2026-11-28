import { Registry } from '@prometheus-io/client';
import { encodeEvent } from '@eventpulse/kafka-config';
import { handleBatch } from '../consumer/batch-handler.js';
import { FaultInjector } from '@eventpulse/fault-injector';
import type { EventRow, RawMessage } from '../sink/parse.js';
import { computeLag } from './lag.js';
import { SinkStats } from './sink-stats.js';

let n = 0;
const msg = (): RawMessage => {
  const e = encodeEvent({
    tenantId: 't',
    event: { event_id: `00000000-0000-4000-8000-${String(++n).padStart(12, '0')}`, type: 'x', occurred_at: '2026-11-12T10:00:00Z', schema_version: 1, payload: {} },
    receivedAt: new Date(Date.now() - 2000),
    requestId: 'r',
    keyMode: 'tenant',
  });
  return { partition: 0, offset: String(n), key: e.key, value: e.value, headers: e.headers };
};
const poison = (): RawMessage => ({ ...msg(), value: Buffer.from('{oops') });
const metric = (text: string, name: string) => {
  const line = text.split('\n').find((l) => l.startsWith(name + ' ') || l.startsWith(name + '{'));
  return line === undefined ? undefined : Number(line.split(' ').pop());
};

function deps(stats: SinkStats, dupesPerChunk: number) {
  return {
    writer: {
      writeChunk: async (rows: EventRow[]) => ({
        attempted: rows.length,
        inserted: rows.length - dupesPerChunk,
        durationMs: 3,
        connectMs: 1,
        freshnessS: Array.from({ length: rows.length - dupesPerChunk }, () => 2), // only inserted rows
      }),
    },
    stats,
    maxRows: 50,
    faults: new FaultInjector(undefined, () => undefined),
    processingDelayMs: 0,
    dlq: { publish: async () => [] },
    retry: { dlqEnabled: true, maxRetries: 3, baseMs: 1, maxMs: 1, transientWindowMs: 0, pauseMs: 1 },
  };
}
const ctx = (messages: RawMessage[]) => ({
  messages,
  resolveOffset: () => undefined,
  commitOffsetsIfNecessary: async () => undefined,
  heartbeat: async () => undefined,
  isRunning: () => true,
  isStale: () => false,
  pausePartition: () => undefined,
});

describe('consumer metrics (S2-F6)', () => {
  it('100 messages, 10 duplicates, 2 poison: consumed 100, inserted 88, duplicates 10, dlq 2, and the invariant holds', async () => {
    const registry = new Registry();
    const stats = new SinkStats(registry);
    const ms = [...Array.from({ length: 49 }, msg), poison(), ...Array.from({ length: 49 }, msg), poison()]; // 2 chunks of 50
    await handleBatch(ctx(ms), deps(stats, 5)); // 5 duplicates per chunk -> 10
    const snap = await stats.snapshot();
    expect(snap).toMatchObject({ consumed: 100, inserted: 88, duplicates: 10, dlq: 2, chunks: 2, batches: 1 });
    expect(snap.consumed).toBe(snap.inserted + snap.duplicates + snap.dlq); // the S2-F6 invariant

    const text = await registry.metrics();
    expect(metric(text, 'ep_consumer_messages_consumed_total')).toBe(100);
    expect(metric(text, 'ep_consumer_rows_inserted_total')).toBe(88);
    expect(metric(text, 'ep_consumer_duplicates_total')).toBe(10);
    expect(text).toContain('ep_consumer_dlq_total{error_class="PERMANENT_PARSE"} 2');
    expect(metric(text, 'ep_event_freshness_seconds_count')).toBe(88); // inserted rows only, never duplicates
    expect(snap.freshness_p99_s).toBeGreaterThan(1);
    expect(text).toContain('ep_consumer_batch_size_messages_count{kind="chunk"} 2');
    expect(text).toContain('ep_consumer_batch_size_messages_count{kind="batch"} 1');
    expect(metric(text, 'ep_consumer_commit_seconds_count')).toBe(2);
  });

  it('K8: revokes are counted and the gap runs from the revoke to the next assign', async () => {
    const registry = new Registry();
    const stats = new SinkStats(registry);
    stats.closeRebalanceGap(1000); // first join: no revoke before it, nothing recorded
    stats.countRevoke(6, 2000);
    stats.countRevoke(1, 2500); // a second revoke before the assign keeps the first timestamp
    stats.closeRebalanceGap(5000);
    const text = await registry.metrics();
    expect(metric(text, 'ep_consumer_revokes_total')).toBe(2);
    expect(metric(text, 'ep_consumer_partitions_revoked_total')).toBe(7);
    expect(metric(text, 'ep_consumer_rebalance_gap_seconds_count')).toBe(1);
    expect(metric(text, 'ep_consumer_rebalance_gap_seconds_sum')).toBe(3);
  });

  it('a failed chunk attempt counts as a chunk failure, not as consumed', async () => {
    const stats = new SinkStats();
    const d = deps(stats, 0);
    d.writer.writeChunk = async () => Promise.reject(Object.assign(new Error('down'), { code: 'ECONNREFUSED' }));
    await expect(handleBatch(ctx([msg(), msg()]), d)).rejects.toThrow();
    expect(stats).toMatchObject({ consumed: 0, chunkFailures: { db_unavailable: 1 } });
  });

  it('every series of the plan is registered', async () => {
    const registry = new Registry();
    const stats = new SinkStats(registry, () => ({ pgTotal: 2, pgIdle: 1, pgWaiting: 0 }));
    stats.setLag([{ partition: 0, end: 10, committed: 7, lag: 3 }]);
    const text = await registry.metrics();
    for (const name of [
      'ep_consumer_lag', 'ep_consumer_last_committed_offset', 'ep_consumer_messages_consumed_total', 'ep_consumer_rows_inserted_total',
      'ep_consumer_duplicates_total', 'ep_consumer_batch_size_messages', 'ep_consumer_batch_processing_seconds', 'ep_consumer_db_write_seconds',
      'ep_consumer_commit_seconds', 'ep_consumer_commit_errors_total', 'ep_consumer_rebalances_total', 'ep_consumer_assigned_partitions',
      'ep_consumer_errors_total', 'ep_consumer_retries_total', 'ep_consumer_dlq_total', 'ep_consumer_paused', 'ep_event_freshness_seconds',
      'ep_pg_pool_total', 'ep_pg_pool_idle', 'ep_pg_pool_waiting', 'ep_pg_pool_connect_wait_seconds',
    ]) expect(text, name).toContain(`# TYPE ${name} `);
    expect(text).toContain('ep_consumer_lag{partition="0"} 3');
  });
});

describe('computeLag', () => {
  it('end - committed per partition', () =>
    expect(computeLag([{ partition: 0, high: '100', low: '0' }, { partition: 1, high: 50, low: 0 }], [{ partition: 0, offset: '90' }, { partition: 1, offset: 50 }])).toEqual([
      { partition: 0, end: 100, committed: 90, lag: 10 },
      { partition: 1, end: 50, committed: 50, lag: 0 },
    ]));

  it('a partition never committed lags by everything still in the log (end - log start)', () =>
    expect(computeLag([{ partition: 2, high: 80, low: 30 }], [{ partition: 2, offset: -1 }])).toEqual([{ partition: 2, end: 80, committed: -1, lag: 50 }]));

  it('a partition missing from the committed list counts as never committed; output sorted by partition', () =>
    expect(computeLag([{ partition: 1, high: 5, low: 0 }, { partition: 0, high: 3, low: 0 }], [])).toEqual([
      { partition: 0, end: 3, committed: -1, lag: 3 },
      { partition: 1, end: 5, committed: -1, lag: 5 },
    ]));
});
