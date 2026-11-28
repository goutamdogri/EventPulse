// S3-F6: the DLQ replay tool re-injects dead letters safely, with filters and a dry run.
//
// Real sink processes and the real CLI (tools/dlq-replay, as a child process) on throw-away source/DLQ topics and
// the test database. Dead letters: 3 "fixable" (an injected PERMANENT_DB error that is gone after the sink restarts)
// and 2 still-poison records (not JSON).
import { mkdtempSync, readFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { StoreProbe, readTopic, waitFor } from '../src/harness.js';
import type { SinkProcess } from '../src/harness.js';
import { dlqLane, logTotal, runReplayCli } from '../src/dlq-replay.js';

describe('S3-F6 DLQ replay tool (real sink + CLI)', () => {
  const store = new StoreProbe();
  let lane: Awaited<ReturnType<typeof dlqLane>>;
  let sink: SinkProcess;
  let fixableIds: string[];
  const GOOD = 10;

  const replay = (args: string[], env: Record<string, string> = {}) => runReplayCli(['replay', '--dlq-topic', lane.dlq, '--json', '--rate', '0', ...args], env);
  const metaSnapshot = async () => (await lane.metaRows()).map((r) => ({ ...r, first_failed_at: r.first_failed_at.toISOString(), last_failed_at: r.last_failed_at.toISOString(), replayed_at: r.replayed_at?.toISOString() ?? null }));

  beforeAll(async () => {
    lane = await dlqLane(store);
    const s = await lane.seed({ fixable: 3, poison: 2, good: GOOD });
    sink = s.sink;
    fixableIds = s.fixable.map((e) => e.event_id);
  });

  afterAll(async () => {
    await lane?.cleanup();
    await store.end();
  });

  it('[setup] dead letters: 3 PERMANENT_DB with tenant and event id, 2 PERMANENT_PARSE without', async () => {
    const rows = await lane.metaRows();
    expect(rows.filter((r) => r.error_class === 'PERMANENT_DB').map((r) => r.event_id).sort()).toEqual([...fixableIds].sort());
    expect(rows.filter((r) => r.error_class === 'PERMANENT_DB').every((r) => r.tenant_id === lane.tenant && r.error_code === '22001')).toBe(true);
    expect(rows.filter((r) => r.error_class === 'PERMANENT_PARSE').map((r) => [r.tenant_id, r.error_code])).toEqual([[null, 'invalid_json'], [null, 'invalid_json']]);
    expect(await store.countTenant(lane.tenant)).toBe(GOOD);
  });

  it('[stats/list/show] triage commands see the dead letters', async () => {
    const st = await runReplayCli(['stats', '--dlq-topic', lane.dlq, '--json']);
    expect(st.code).toBe(0);
    expect(st.json).toMatchObject({ total: 5, byClass: [{ k: 'PERMANENT_DB', n: 3 }, { k: 'PERMANENT_PARSE', n: 2 }], byTenant: [{ k: lane.tenant, n: 3 }, { k: '(unparsed)', n: 2 }] });
    const [db] = (await lane.metaRows()).filter((r) => r.error_class === 'PERMANENT_DB');
    const show = await runReplayCli(['show', String(db.id)]);
    expect(show.code).toBe(0);
    expect(show.json.record.value).toMatchObject({ format: 'json', value: { event_id: db.event_id, tenant_id: lane.tenant } });
    expect(show.json.record.headers).toMatchObject({ 'dlq.error_class': 'PERMANENT_DB', 'dlq.original_topic': lane.source });
    expect(show.json.record.precheck).toEqual({ ok: true });
    const [poison] = (await lane.metaRows()).filter((r) => r.error_class === 'PERMANENT_PARSE');
    const showPoison = await runReplayCli(['show', String(poison.id)]);
    expect(showPoison.json.record.value.format).toBe('hex');
    expect(showPoison.json.record.precheck).toMatchObject({ ok: false, code: 'invalid_json' });
  });

  it('[G6.replay-filters] tenant, class, code, time and source offset range select only the matching rows', async () => {
    const rows = await lane.metaRows();
    const ids = (rs: any[]) => rs.map((r) => String(r.id)).sort();
    const list = async (...args: string[]) => ids((await runReplayCli(['list', '--dlq-topic', lane.dlq, '--json', ...args])).json);
    expect(await list()).toEqual(ids(rows));
    expect(await list('--tenant', lane.tenant)).toEqual(ids(rows.filter((r) => r.tenant_id === lane.tenant)));
    expect(await list('--error-class', 'PERMANENT_PARSE')).toEqual(ids(rows.filter((r) => r.error_class === 'PERMANENT_PARSE')));
    expect(await list('--error-code', '22001')).toEqual(ids(rows.filter((r) => r.error_code === '22001')));
    expect(await list('--event-id', fixableIds[0])).toEqual(ids(rows.filter((r) => r.event_id === fixableIds[0])));
    expect(await list('--since', '1h')).toEqual(ids(rows));
    expect(await list('--until', '1h')).toEqual([]);
    const first = rows[0];
    expect(await list('--source-partition', String(first.source_partition), '--source-offset-from', String(first.source_offset), '--source-offset-to', String(first.source_offset))).toEqual([String(first.id)]);
    expect(await list('--id', `${rows[1].id},${rows[2].id}`)).toEqual(ids([rows[1], rows[2]]));
    expect(await list('--limit', '2')).toHaveLength(2);
    const bad = await runReplayCli(['list', '--error-class', 'NOPE']);
    expect(bad.code).toBe(2);
  });

  it('[G6.replay-dry-run] the default: 0 records produced, dlq_meta unchanged, plan says 3 OK / 2 would fail', async () => {
    const before = await metaSnapshot();
    const inSource = await logTotal(lane.source);
    const r = await replay([]);
    expect(r.code, r.stderr).toBe(0);
    expect(r.json).toMatchObject({ dryRun: true, status: 'dry_run', counts: { selected: 5, replayed: 0, skipped: 2, failed: 0 }, plan: { replay: 3, wouldFail: 2, loopGuard: 0, missing: 0 } });
    expect(await logTotal(lane.source)).toBe(inSource);
    expect(await metaSnapshot()).toEqual(before);
    const book = (await store.pool.query('select * from dlq_replays where replay_id = $1', [r.json.replayId])).rows[0];
    expect(book).toMatchObject({ dry_run: true, status: 'dry_run', selected: 5, replayed: 0, skipped: 2, operator: 'correctness-tests' });
    expect(book.filters).toMatchObject({ dlqTopic: lane.dlq });
  });

  it('[G6.replay-execute] --execute --yes: the 3 fixable events stored once, their rows marked, 2 poison skipped, textfile written', async () => {
    const dir = mkdtempSync(join(tmpdir(), 'dlq-replay-'));
    const textfile = join(dir, 'dlq_replay.prom');
    const r = await replay(['--execute', '--yes'], { DLQ_REPLAY_TEXTFILE: textfile });
    expect(r.code, r.stderr).toBe(0);
    expect(r.json).toMatchObject({ dryRun: false, status: 'done', counts: { selected: 5, replayed: 3, skipped: 2, failed: 0 } });
    await waitFor(async () => (await store.countTenant(lane.tenant)) === GOOD + 3, 30_000, 'replayed events stored');
    const fp = await store.fingerprint(lane.tenant);
    expect(fp.rows).toBe(fp.distinct);
    const rows = await lane.metaRows();
    const db = rows.filter((r) => r.error_class === 'PERMANENT_DB');
    expect(db.every((x) => x.replayed_at !== null && x.replay_count === 1 && x.last_replay_id === r.json.replayId)).toBe(true);
    expect(rows.filter((x) => x.error_class === 'PERMANENT_PARSE').every((x) => x.replayed_at === null && x.replay_count === 0)).toBe(true);
    expect(rows).toHaveLength(5); // nothing came back to the DLQ
    // The re-injected records carry the original key/headers plus replay.*.
    const src = (await readTopic(lane.source, 10_000, (x) => x.filter((m) => m.headers['replay.id']).length >= 3)).filter((m) => m.headers['replay.id']);
    expect(src).toHaveLength(3);
    expect(src[0].headers).toMatchObject({ 'replay.id': r.json.replayId, 'replay.count': '1' });
    expect(src[0].headers['dlq.error_class']).toBeUndefined();
    expect(src[0].headers.received_at).toBeDefined();
    const book = (await store.pool.query('select * from dlq_replays where replay_id = $1', [r.json.replayId])).rows[0];
    expect(book).toMatchObject({ dry_run: false, status: 'done', selected: 5, replayed: 3, skipped: 2, failed: 0 });
    expect(book.finished_at).not.toBeNull();
    const prom = readFileSync(textfile, 'utf8');
    expect(prom).toContain('ep_dlq_replay_replayed 3\n');
    expect(prom).toContain('ep_dlq_replay_skipped 2\n');
  });

  it('[G3.replay-twice] the same replay again with --include-replayed: sink duplicates +3, stored rows unchanged', async () => {
    const before = await store.fingerprint(lane.tenant);
    const dup0 = (await sink.stats()).duplicates;
    const r = await replay(['--execute', '--yes', '--include-replayed']);
    expect(r.code, r.stderr).toBe(0);
    expect(r.json.counts).toEqual({ selected: 5, replayed: 3, skipped: 2, failed: 0 });
    await waitFor(async () => (await sink.stats()).duplicates >= dup0 + 3, 30_000, 'sink duplicates +3');
    expect((await sink.stats()).duplicates).toBe(dup0 + 3);
    expect(await store.fingerprint(lane.tenant)).toEqual(before);
    expect((await lane.metaRows()).filter((x) => x.error_class === 'PERMANENT_DB').map((x) => x.replay_count)).toEqual([2, 2, 2]);
  });

  it('[G6.replay-loop-guard] still-poison records go back to the DLQ with replay.count; after 3 rounds the guard skips them', async () => {
    const poisonRows = async () => (await lane.metaRows()).filter((x) => x.error_class === 'PERMANENT_PARSE');
    for (let round = 1; round <= 3; round++) {
      const r = await replay(['--execute', '--yes', '--include-failing', '--error-class', 'PERMANENT_PARSE']);
      expect(r.code, r.stderr).toBe(0);
      expect(r.json.counts).toMatchObject({ selected: 2, replayed: 2, skipped: 0 });
      expect(r.json.plan.forced).toBe(2);
      // Each round: the 2 replayed records fail again in the sink -> 2 new dead letters (new source offsets).
      await waitFor(async () => (await poisonRows()).length === 2 + 2 * round, 30_000, `round ${round}: back in the DLQ`);
    }
    const dlqRecords = await readTopic(lane.dlq, 10_000, (x) => x.length >= 5 + 6);
    const counts = dlqRecords.filter((m) => m.headers['dlq.error_class'] === 'PERMANENT_PARSE').map((m) => m.headers['replay.count'] ?? '0').sort();
    expect(counts).toEqual(['0', '0', '1', '1', '2', '2', '3', '3']);
    // Round 4: replay.count would be 4 > DLQ_MAX_REPLAYS=3.
    const r4 = await replay(['--execute', '--yes', '--include-failing', '--error-class', 'PERMANENT_PARSE']);
    expect(r4.code, r4.stderr).toBe(0);
    expect(r4.json).toMatchObject({ counts: { selected: 2, replayed: 0, skipped: 2 }, plan: { loopGuard: 2 } });
    await new Promise((res) => setTimeout(res, 2000));
    expect(await poisonRows()).toHaveLength(8);
  });

  it('[G6.dlq-reindex] dlq_meta rows deleted: reindex restores them from the DLQ topic with the same source offsets', async () => {
    const key = (r: any) => `${r.source_topic}|${r.source_partition}|${r.source_offset}|${r.dlq_partition}|${r.dlq_offset}|${r.error_class}|${r.error_code}|${r.tenant_id}|${r.event_id}|${r.attempts}`;
    const before = (await lane.metaRows()).map(key).sort();
    await store.pool.query('delete from dlq_meta where dlq_topic = $1', [lane.dlq]);
    expect(await lane.metaRows()).toEqual([]);
    const r = await runReplayCli(['reindex', '--topic', lane.dlq, '--json']);
    expect(r.code, r.stderr).toBe(0);
    expect(r.json).toMatchObject({ scanned: before.length, inserted: before.length, existing: 0, not_dead_letters: 0 });
    expect((await lane.metaRows()).map(key).sort()).toEqual(before);
    // Idempotent: a second reindex inserts nothing.
    expect((await runReplayCli(['reindex', '--topic', lane.dlq, '--json'])).json).toMatchObject({ inserted: 0, existing: before.length });
  });
});

describe('S3-F6 crash mid-replay (real sink + CLI)', () => {
  const store = new StoreProbe();
  let lane: Awaited<ReturnType<typeof dlqLane>>;

  afterAll(async () => {
    await lane?.cleanup();
    await store.end();
  });

  it('[G3.replay-crash] SIGKILL at replay:after-produce-before-mark, re-run the same command: all stored once, all marked', async () => {
    lane = await dlqLane(store);
    const { sink } = await lane.seed({ fixable: 4, poison: 0, good: 5 });
    const args = ['replay', '--dlq-topic', lane.dlq, '--json', '--rate', '0', '--execute', '--yes'];
    const crashed = await runReplayCli(args, { DEBUG_FAULTS: 'true', FAULTS: 'replay:after-produce-before-mark@2' });
    expect(crashed.signal).toBe('SIGKILL');
    expect(crashed.stderr).toContain('CRASH_AT=replay:after-produce-before-mark');
    expect((await lane.metaRows()).filter((r) => r.replayed_at !== null)).toHaveLength(1); // 2 produced, 1 marked
    const again = await runReplayCli(args);
    expect(again.code, again.stderr).toBe(0);
    expect(again.json.counts).toMatchObject({ selected: 3, replayed: 3 });
    const rows = await lane.metaRows();
    expect(rows.every((r) => r.replayed_at !== null && r.replay_count === 1)).toBe(true);
    await waitFor(async () => (await store.countTenant(lane.tenant)) === 9, 30_000, 'all stored');
    await waitFor(async () => (await sink.stats()).duplicates >= 1, 30_000, 'the produced-but-unmarked record is a sink duplicate');
    const fp = await store.fingerprint(lane.tenant);
    expect(fp).toMatchObject({ rows: 9, distinct: 9 });
    const running = (await store.pool.query("select status from dlq_replays where filters->>'dlqTopic' = $1 order by started_at", [lane.dlq])).rows.map((r) => r.status);
    expect(running).toEqual(['running', 'done']); // the crashed run stays "running": visible, not silently "done"
  });
});
