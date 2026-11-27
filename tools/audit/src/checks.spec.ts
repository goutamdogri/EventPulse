import { committedSum, consumerDeltas, evaluate, groupLag } from './checks.ts';
import { formatReport } from './format.ts';
import type { ConsumerStats, K6Summary, RunFacts, Snapshot } from './types.ts';

const parts = (highs: number[], lows: number[] = highs.map(() => 0)) => highs.map((high, partition) => ({ partition, low: lows[partition], high }));
const consumer = (o: Partial<ConsumerStats> = {}): ConsumerStats => ({ url: 'http://c1', uptime_s: 1000, consumed: 0, attempted: 0, inserted: 0, duplicates: 0, dlq: 0, ...o });
const snap = (o: Partial<Snapshot> = {}): Snapshot => ({
  taken_at: '2026-11-22T10:00:00.000Z',
  topic: 'events.raw',
  dlq_topic: 'events.dlq',
  group: 'sink-postgres',
  raw: parts([100, 100]),
  dlq: parts([0]),
  committed: [{ partition: 0, offset: 100 }, { partition: 1, offset: 100 }],
  db: { rows_total: 200 },
  api: { url: 'api', uptime_s: 1000, acked: 50, failed: 0, http_202: 50, by503: {} },
  consumers: [consumer({ consumed: 200, attempted: 200, inserted: 200 })],
  notes: [],
  ...o,
});
const END_AT = '2026-11-22T10:01:00.000Z'; // 60 s later
const k6 = (o: Partial<K6Summary> = {}): K6Summary => ({ sent: 1000, by_status: { '202': 1000, '4xx': 0, '503': 0, other: 0 }, network_errors: 0, ...o });
const facts = (o: Partial<RunFacts> = {}): RunFacts => ({
  run_id: 'a1b2c3d4', rows_distinct: 1000, freshness_p50_s: 0.2, freshness_p99_s: 1.1, dlq_new_total: 0, dlq_run: 0, lag_end: 0, drained: true, drain_waited_s: 1, ...o,
});

/** A clean run: 1000 sent, 1000 acked, 1000 new records, 1000 rows, no duplicates. */
function clean() {
  const start = snap();
  const end = snap({
    taken_at: END_AT,
    raw: parts([600, 600]),
    committed: [{ partition: 0, offset: 600 }, { partition: 1, offset: 600 }],
    api: { url: 'api', uptime_s: 1060, acked: 1050, failed: 0, http_202: 1050, by503: {} },
    consumers: [consumer({ uptime_s: 1060, consumed: 1200, attempted: 1200, inserted: 1200 })],
  });
  return { start, end };
}
const status = (r: ReturnType<typeof evaluate>, id: string) => r.checks.find((c) => c.id === id)?.status;

describe('evaluate (S2-F7 reconciliation)', () => {
  it('clean run: every check OK, RECONCILED, exit 0', () => {
    const { start, end } = clean();
    const r = evaluate(start, end, k6(), facts());
    expect(r.checks.map((c) => `${c.id}:${c.status}`)).toEqual(['A:ok', 'B:ok', 'C:ok', 'D:ok', 'E:ok', 'F:ok', 'G:ok', 'H:ok', 'I:ok', 'J:skipped', 'K:skipped']); // Stage 2 fixtures: no split
    expect(r).toMatchObject({ result: 'RECONCILED', exit_code: 0, findings: [] });
    expect(r.numbers).toMatchObject({ acked_202: 1000, kafka_records_delta: 1000, rows_distinct_run: 1000, duplicates_delta: 0 });
  });

  it('acked > Kafka offsets: check C fails as "acknowledged but not in the log", exit 1', () => {
    const { start, end } = clean();
    end.raw = parts([599, 600]); // one record short
    const r = evaluate(start, end, k6(), facts());
    expect(status(r, 'C')).toBe('fail');
    expect(r.exit_code).toBe(1);
    expect(r.findings.join()).toMatch(/ACKNOWLEDGED BUT NOT IN THE LOG: 1/);
  });

  it('rows < acked: check E fails as "acknowledged but missing", exit 1', () => {
    const { start, end } = clean();
    const r = evaluate(start, end, k6(), facts({ rows_distinct: 997 }));
    expect(status(r, 'E')).toBe('fail');
    expect(r.exit_code).toBe(1);
    expect(r.findings.join()).toMatch(/MISSING FROM POSTGRES: 3/);
  });

  it('rows > acked (503 but stored): reported as extra, still exit 0', () => {
    const { start, end } = clean();
    end.raw = parts([610, 610]); // 20 records written whose batch was answered 503
    const r = evaluate(start, end, k6({ sent: 1020, by_status: { '202': 1000, '4xx': 0, '503': 20, other: 0 } }), facts({ rows_distinct: 1020 }));
    expect(status(r, 'E')).toBe('finding');
    expect(status(r, 'D')).toBe('finding');
    expect(r).toMatchObject({ result: 'RECONCILED', exit_code: 0 });
    expect(r.numbers.written_not_acked).toBe(20);
  });

  it('dead-lettered events of the run are subtracted from the expected rows', () => {
    const { start, end } = clean();
    const r = evaluate(start, end, k6(), facts({ rows_distinct: 998, dlq_run: 2, dlq_new_total: 3 }));
    expect(status(r, 'E')).toBe('ok');
    expect(r.findings.join()).toMatch(/1 new record\(s\) not from this run/);
  });

  it('lag > 0 after the timeout: NOT DRAINED, exit 2 (and no critical failure)', () => {
    const { start, end } = clean();
    const r = evaluate(start, end, k6(), facts({ drained: false, lag_end: 120, rows_distinct: 1000 }));
    expect(status(r, 'I')).toBe('not_drained');
    expect(r).toMatchObject({ result: 'NOT DRAINED', exit_code: 2 });
  });

  it('not drained and rows still missing: E is "not final", NOT a false loss (exit 2, not 1)', () => {
    const { start, end } = clean();
    const r = evaluate(start, end, k6(), facts({ drained: false, lag_end: 500, rows_distinct: 500 }));
    expect(status(r, 'E')).toBe('finding');
    expect(r.findings.join()).toMatch(/not final: 500/);
    expect(r).toMatchObject({ result: 'NOT DRAINED', exit_code: 2 });
  });

  it('a critical failure that does not depend on the drain (C: not in the log) still wins (exit 1)', () => {
    const { start, end } = clean();
    end.raw = parts([590, 600]);
    const r = evaluate(start, end, k6(), facts({ drained: false, lag_end: 5, rows_distinct: 990 }));
    expect(status(r, 'C')).toBe('fail');
    expect(r.exit_code).toBe(1);
  });

  it('log start offset moved: retention finding printed', () => {
    const { start, end } = clean();
    end.raw = parts([600, 600], [40, 0]);
    const r = evaluate(start, end, k6(), facts());
    expect(status(r, 'H')).toBe('finding');
    expect(r.findings.join()).toMatch(/retention deleted 40 record\(s\) on partition\(s\) 0/);
  });

  it('API counter disagrees with the 202s: B fails (critical)', () => {
    const { start, end } = clean();
    end.api!.acked = 1040; // delta 990
    const r = evaluate(start, end, k6(), facts());
    expect(status(r, 'B')).toBe('fail');
    expect(r.exit_code).toBe(1);
  });

  it('API restarted during the run: B skipped, not failed', () => {
    const { start, end } = clean();
    end.api!.uptime_s = 10;
    end.api!.acked = 3;
    expect(status(evaluate(start, end, k6(), facts()), 'B')).toBe('skipped');
  });

  it('consumer crash + restart: duplicates reported, F checked on the new process, sums best-effort', () => {
    const { start, end } = clean();
    // The old process died; a new one (uptime 20 s) reprocessed 150 messages it could not know were stored.
    end.consumers = [consumer({ uptime_s: 20, consumed: 650, attempted: 650, inserted: 500, duplicates: 150 })];
    const r = evaluate(start, end, k6(), facts());
    expect(status(r, 'F')).toBe('ok');
    expect(status(r, 'G')).toBe('finding');
    expect(r.findings.join()).toMatch(/G: 150/);
    expect(r.findings.join()).toMatch(/restarted during the run/);
    expect(r.exit_code).toBe(0); // no loss: E and C still decide
  });

  it('F fails when a process breaks consumed = inserted + duplicates + dlq', () => {
    const { start, end } = clean();
    end.consumers = [consumer({ uptime_s: 1060, consumed: 1200, attempted: 1190, inserted: 1190 })]; // 10 unexplained
    expect(status(evaluate(start, end, k6(), facts()), 'F')).toBe('fail');
  });

  it('topic recreated (offsets went backwards): C fails', () => {
    const { start, end } = clean();
    end.raw = parts([10, 10]);
    expect(status(evaluate(start, end, k6(), facts()), 'C')).toBe('fail');
  });

  it('no k6 summary: the k6-based checks are skipped', () => {
    const { start, end } = clean();
    const r = evaluate(start, end, null, facts());
    expect(['A', 'B', 'C', 'E'].map((id) => status(r, id))).toEqual(['skipped', 'skipped', 'skipped', 'skipped']);
  });

  it('the report prints every number and the result line', () => {
    const { start, end } = clean();
    const text = formatReport(evaluate(start, end, k6(), facts()), '(test)');
    expect(text).toContain('RUN a1b2c3d4');
    expect(text).toContain('RESULT: RECONCILED   (exit 0)');
    expect(text).toMatch(/kafka records \(delta\)\s+1000/);
  });
});

describe('helpers', () => {
  it('groupLag: end - committed; never committed = everything in the log', () => {
    expect(groupLag(parts([100, 50], [0, 10]), [{ partition: 0, offset: 90 }, { partition: 1, offset: -1 }])).toBe(10 + 40);
  });

  it('consumerDeltas: a process gone at the end is noted', () => {
    const { notes } = consumerDeltas([consumer({ url: 'a' }), consumer({ url: 'b' })], [consumer({ url: 'a', uptime_s: 2000 })], 60);
    expect(notes.join()).toMatch(/b was gone/);
  });
});

// --- Stage 3 (S3-F3): API idempotency, duplicate causes, and the unexplained difference ----------------------------
const idem = (o: Partial<Record<string, number>> = {}) => ({ new: 0, replayed: 0, in_progress: 0, payload_mismatch: 0, fail_open: 0, fail_closed: 0, disabled: 0, ...o });
const c3 = (o: Partial<ConsumerStats> = {}) => consumer({ dup_reprocessed: 0, dup_redelivered: 0, dup_unknown: 0, conflicts: 0, ...o });

/** 1000 distinct events, 100 of them resent with the same key. */
function stage3(o: { resendsPassApi?: boolean } = {}) {
  const passed = o.resendsPassApi ? 100 : 0; // resends that reached Kafka (fail_open) instead of being replayed
  const start = snap({ api: { ...snap().api!, idem: idem() }, consumers: [c3({ consumed: 200, attempted: 200, inserted: 200 })] });
  const end = snap({
    taken_at: END_AT,
    raw: parts([600 + passed / 2, 600 + passed / 2]),
    committed: [{ partition: 0, offset: 600 + passed / 2 }, { partition: 1, offset: 600 + passed / 2 }],
    db: { rows_total: 1200 },
    api: {
      url: 'api', uptime_s: 1060, acked: 1050 + passed, failed: 0, http_202: 1150, by503: {},
      idem: o.resendsPassApi ? idem({ fail_open: 1100 }) : idem({ new: 1000, replayed: 100 }),
    },
    consumers: [c3({ uptime_s: 1060, consumed: 1200 + passed, attempted: 1200 + passed, inserted: 1200, duplicates: passed, dup_redelivered: passed })],
  });
  const summary = k6({ sent: 1100, by_status: { '202': 1100, '4xx': 0, '503': 0, other: 0 }, unique_202: 1000, replayed_202: o.resendsPassApi ? 0 : 100, resends_sent: 100 });
  return { start, end, summary };
}

describe('evaluate: Stage 3 duplicate accounting (S3-F3)', () => {
  it('resends stopped at the API: B counts replayed 202s, C/E use distinct events, J and K ok', () => {
    const { start, end, summary } = stage3();
    const r = evaluate(start, end, summary, facts());
    expect(r.checks.map((c) => `${c.id}:${c.status}`)).toEqual(['A:ok', 'B:ok', 'C:ok', 'D:ok', 'E:ok', 'F:ok', 'G:ok', 'H:ok', 'I:ok', 'J:ok', 'K:ok']);
    expect(r.numbers).toMatchObject({ accepted_unique: 1000, api_replayed_delta: 100, unexplained: 0, committed_delta: 1000, rows_new_total: 1000 });
    expect(r.checks.find((c) => c.id === 'B')!.detail).toBe('1100 == 1000 acked + 100 replayed');
  });

  it('Redis down (fail_open): resends reach Kafka and the sink; every extra copy is accounted for as redelivered', () => {
    const { start, end, summary } = stage3({ resendsPassApi: true });
    const r = evaluate(start, end, summary, facts());
    expect(status(r, 'D')).toBe('finding'); // 100 extra copies in the log
    expect(status(r, 'G')).toBe('finding');
    expect(status(r, 'J')).toBe('ok'); // 1100 committed - 1000 rows - 0 DLQ - 100 redelivered = 0
    expect(status(r, 'K')).toBe('finding'); // REDIS DEGRADED
    expect(r).toMatchObject({ result: 'RECONCILED', exit_code: 0 });
    expect(r.numbers).toMatchObject({ dup_redelivered: 100, unexplained: 0, written_not_acked: 100 });
  });

  it('a committed record that is neither stored, dead-lettered nor a counted duplicate: J fails (unexplained), exit 1', () => {
    const { start, end, summary } = stage3();
    end.db.rows_total = 1195; // 5 records committed but vanished
    const r = evaluate(start, end, summary, facts());
    expect(status(r, 'J')).toBe('fail');
    expect(r.numbers.unexplained).toBe(5);
    expect(r.exit_code).toBe(1);
    expect(r.findings.join()).toMatch(/UNEXPLAINED DIFFERENCE/);
  });

  it('reprocessed duplicates (consumer crash) need no new record, so J still balances', () => {
    const { start, end, summary } = stage3();
    end.consumers = [c3({ uptime_s: 1060, consumed: 1450, attempted: 1450, inserted: 1200, duplicates: 250, dup_reprocessed: 250 })];
    const r = evaluate(start, end, summary, facts());
    expect(status(r, 'J')).toBe('ok');
    expect(r.numbers.dup_reprocessed).toBe(250);
  });

  it('a restarted consumer makes J best-effort (finding), never a false failure', () => {
    const { start, end, summary } = stage3();
    end.consumers = [c3({ uptime_s: 10, consumed: 300, attempted: 300, inserted: 300 })]; // restarted: earlier counts lost
    end.db.rows_total = 1190;
    const r = evaluate(start, end, summary, facts());
    expect(status(r, 'J')).toBe('finding');
    expect(r.exit_code).not.toBe(1);
  });

  it('F also checks that the causes add up to the duplicates', () => {
    const { start, end, summary } = stage3();
    end.consumers = [c3({ uptime_s: 1060, consumed: 1210, attempted: 1210, inserted: 1200, duplicates: 10, dup_reprocessed: 3, dup_redelivered: 3 })];
    expect(status(evaluate(start, end, summary, facts()), 'F')).toBe('fail');
  });

  it('a Stage 2 k6 summary (no unique_202) falls back to the 202 count', () => {
    const { start, end } = stage3();
    const r = evaluate(start, end, k6({ sent: 1000 }), facts());
    expect(r.numbers.accepted_unique).toBe(1000);
  });

  it('committedSum: never-committed partitions count from the log start', () => {
    expect(committedSum(snap({ raw: parts([10, 10], [3, 0]), committed: [{ partition: 1, offset: 7 }] }))).toBe(3 + 7);
  });
});
