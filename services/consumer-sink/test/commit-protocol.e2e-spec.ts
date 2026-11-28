// S2-F3: offsets are committed only after the DB transaction commits.
// Proven with a real consumer-sink process that is SIGKILLed at each crash point, a database that is cut
// off through Toxiproxy, and a SIGTERM under load. Each test uses its own topic and consumer group.
import { randomUUID } from 'node:crypto';
import { LEGACY_CRASH_AT } from '@eventpulse/fault-injector';
import {
  Db,
  createTopic,
  databaseUrlViaProxy,
  deleteTopic,
  fullyCommitted,
  groupOffsets,
  produceEvents,
  setPgProxy,
  sleep,
  spawnSink,
  totalCommitted,
  waitFor,
  waitReady,
} from './process-helpers.js';
import type { SinkProcess } from './process-helpers.js';

const N = 5000;
const CHUNK = 500;

describe('S2-F3 commit protocol (real process)', () => {
  const db = new Db();
  const topics: string[] = [];
  const procs: SinkProcess[] = [];
  const start = (env: Record<string, string>) => {
    const p = spawnSink(env);
    procs.push(p);
    return p;
  };

  afterEach(async () => {
    await Promise.all(procs.splice(0).map((p) => p.stop()));
  });

  afterAll(async () => {
    await setPgProxy(true).catch(() => undefined);
    await Promise.all(topics.map(deleteTopic));
    await db.end();
  });

  /**
   * Crash at `point` on the 4th batch (the client's first batches are tiny; by the 4th they are full), restart cleanly, and return what the restarted process saw.
   * Batch 1 is fully committed, so the crash always happens with some progress already stored.
   */
  async function crashThenRecover(point: string) {
    const topic = await createTopic();
    topics.push(topic);
    const group = `sink-e2e-${randomUUID()}`;
    const ids = await produceEvents(topic, N);
    const common = { CONSUMER_TOPIC: topic, CONSUMER_GROUP_ID: group, SINK_MAX_ROWS: String(CHUNK), CONSUMER_SESSION_TIMEOUT_MS: '10000' };

    const crashing = start({ ...common, DEBUG_FAULTS: 'true', CRASH_AT: point, CRASH_AFTER_BATCHES: '4' });
    const exit = await Promise.race([crashing.exited, sleep(45_000).then(() => null)]);
    expect(exit, `process did not crash\n${crashing.log.join('')}`).toEqual({ code: null, signal: 'SIGKILL' });
    expect(crashing.log.join('')).toContain(`CRASH_AT=${LEGACY_CRASH_AT[point]}`); // Stage 2 env form, Stage 3 point name
    const rowsAtCrash = await db.countRows(ids);
    const committedAtCrash = await totalCommitted(group, topic);

    const recovered = start(common);
    try {
      await waitFor(async () => (await db.countRows(ids)) === N && (await fullyCommitted(group, topic)), 60_000, `all ${N} rows + offsets`);
    } catch (e) {
      const st = await recovered.stats().catch(() => null);
      throw new Error(`${(e as Error).message}\nrows=${await db.countRows(ids)} offsets=${JSON.stringify(await groupOffsets(group, topic))}\n` +
        `stats=${JSON.stringify(st && { consumed: st.consumed, inserted: st.inserted, commits: st.commits, commit_errors: st.commit_errors, batch_errors: st.batch_errors, last_error: st.last_error, rebalances: st.rebalances })}\n` +
        `log=${recovered.log.join('').slice(-3000)}`);
    }
    await sleep(500);
    const st = await recovered.stats();
    console.info(`[S2-F3] crash ${point}: rows at crash ${rowsAtCrash}, committed offsets at crash ${committedAtCrash}, ` +
      `restarted process consumed ${st.consumed}, duplicates ${st.duplicates}`);
    return { rowsAtCrash, committedAtCrash, stats: st };
  }

  it('crash after the DB commit, before the offset commit: no loss, duplicates bounded by one chunk', async () => {
    const r = await crashThenRecover('after_db_commit');
    // Rows of the crashed chunk were stored but their offsets not committed.
    expect(r.rowsAtCrash).toBeGreaterThan(r.committedAtCrash);
    expect(r.rowsAtCrash - r.committedAtCrash).toBeLessThanOrEqual(CHUNK);
    // The restarted process read that chunk again; ON CONFLICT absorbed it and it was counted.
    expect(r.stats.duplicates).toBe(r.rowsAtCrash - r.committedAtCrash);
    expect(r.stats.duplicates).toBeGreaterThanOrEqual(1);
    expect(r.stats.duplicates).toBeLessThanOrEqual(CHUNK);
  });

  it.each(['before_parse', 'after_parse'])('crash at %s: no loss and no duplicate rows', async (point) => {
    const r = await crashThenRecover(point);
    // Nothing of the crashed chunk reached Postgres: stored rows == committed offsets.
    expect(r.rowsAtCrash).toBe(r.committedAtCrash);
    expect(r.stats.duplicates).toBe(0);
  });

  it('committed offset = last processed offset + 1 (the next offset to read), exactly at the log end', async () => {
    const topic = await createTopic(1);
    topics.push(topic);
    const group = `sink-e2e-${randomUUID()}`;
    const ids = await produceEvents(topic, 1234); // offsets 0..1233 on the single partition
    start({ CONSUMER_TOPIC: topic, CONSUMER_GROUP_ID: group, SINK_MAX_ROWS: '500' });
    await waitFor(async () => (await db.countRows(ids)) === 1234, 60_000, '1234 rows');
    await waitFor(async () => (await groupOffsets(group, topic))[0].committed === 1234, 15_000, 'committed offset 1234');
    // Last processed offset is 1233; Kafka stores "next offset to read" = 1233 + 1. Not 1233 (would re-read the
    // last record on restart), not beyond the log end.
    expect(await groupOffsets(group, topic)).toEqual([{ partition: 0, committed: 1234, end: 1234 }]);
  });

  it('database unreachable: no offset advances while it is down; after it returns every row is stored', async () => {
    const topic = await createTopic();
    topics.push(topic);
    const group = `sink-e2e-${randomUUID()}`;
    const sink = start({
      CONSUMER_TOPIC: topic,
      CONSUMER_GROUP_ID: group,
      DATABASE_URL: databaseUrlViaProxy(),
      PG_CONN_TIMEOUT_MS: '1000',
      PG_STATEMENT_TIMEOUT_MS: '3000',
    });
    await waitReady(sink);

    try {
      await setPgProxy(false); // Postgres is now unreachable for the sink
      const ids = await produceEvents(topic, 500);
      await sleep(5000);
      expect(await totalCommitted(group, topic)).toBe(0); // nothing committed without a DB commit
      expect(await db.countRows(ids)).toBe(0);
      expect((await sink.stats()).errors_by_class.TRANSIENT_SYSTEMIC).toBeGreaterThan(0); // S2-F4: retried with backoff inside the batch first

      await setPgProxy(true);
      await waitFor(async () => (await db.countRows(ids)) === 500 && (await fullyCommitted(group, topic)), 60_000, '500 rows after the DB came back');
    } finally {
      await setPgProxy(true);
    }
  });

  it('SIGTERM under load: the current chunk finishes and commits, nothing is lost, nothing replayed', async () => {
    const topic = await createTopic();
    topics.push(topic);
    const group = `sink-e2e-${randomUUID()}`;
    const ids = await produceEvents(topic, 3000);
    const common = { CONSUMER_TOPIC: topic, CONSUMER_GROUP_ID: group, SINK_MAX_ROWS: '200' };

    const first = start({ ...common, SINK_PROCESSING_DELAY_MS: '300' }); // slow, so we stop it mid-way
    await waitFor(async () => (await db.countRows(ids)) >= 400, 60_000, 'some progress');
    const exit = await first.stop();
    expect(exit).toEqual({ code: 0, signal: null }); // a graceful exit with code 0 (S2-F2), not a crash
    const rowsAtStop = await db.countRows(ids);
    expect(rowsAtStop).toBeLessThan(3000);
    // Clean shutdown: every stored row has its offset committed.
    expect(await totalCommitted(group, topic)).toBe(rowsAtStop);

    const second = start(common);
    await waitFor(async () => (await db.countRows(ids)) === 3000 && (await fullyCommitted(group, topic)), 60_000, 'all 3000 rows');
    await sleep(500);
    const st = await second.stats();
    console.info(`[S2-F3] SIGTERM: rows at stop ${rowsAtStop}, restarted process consumed ${st.consumed}, duplicates ${st.duplicates}`);
    expect(st.duplicates).toBe(0);
    expect(st.consumed).toBe(3000 - rowsAtStop);
  });
});
