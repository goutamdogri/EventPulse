// The crash matrix (S3-F2, later the raw material of docs/delivery-guarantees.md).
// For every registered crash point: SIGKILL there, restart without the fault, drain, audit. Writes
// docs/generated/crash-matrix.json and .md, then asserts the guarantees row by row.
import { execFileSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { CRASH_POINTS } from '@eventpulse/fault-injector';
import { StoreProbe } from '../src/harness.js';
import { renderMarkdown } from '../src/crash-matrix/report.js';
import type { CrashResult } from '../src/crash-matrix/report.js';
import { CONSUMER_CHUNK, SCENARIOS } from '../src/crash-matrix/scenarios.js';

const OUT = resolve(import.meta.dirname, '../../../docs/generated');
// CRASH_MATRIX_ONLY=consumer:after-db-commit,... runs a subset (the files are only written for a full run).
const only = (process.env.CRASH_MATRIX_ONLY ?? '').split(',').filter(Boolean);
const points = CRASH_POINTS.filter((p) => only.length === 0 || only.includes(p.name));

/** What each point must show, beyond "nothing lost, nothing stored twice" (checked for every point). */
const EXPECT: Record<string, (r: CrashResult) => void> = {
  // Nothing of the crashed chunk reached Postgres: the restarted process writes it for the first time.
  'consumer:before-parse': (r) => expect(r.duplicatesProcessed).toBe(0),
  'consumer:after-parse': (r) => expect(r.duplicatesProcessed).toBe(0),
  'consumer:before-insert': (r) => expect(r.duplicatesProcessed).toBe(0),
  // The open transaction dies with the connection: rolled back, so still no duplicate.
  'consumer:after-insert-before-commit': (r) => expect(r.duplicatesProcessed).toBe(0),
  // Stored but not committed: exactly that chunk is processed again, absorbed by the key and counted.
  'consumer:after-db-commit': (r) => {
    expect(r.duplicatesProcessed).toBe(r.storedAtCrash - r.committedAtCrash);
    expect(r.duplicatesProcessed).toBeGreaterThanOrEqual(1);
    expect(r.duplicatesProcessed).toBeLessThanOrEqual(CONSUMER_CHUNK);
  },
  // Both committed: nothing to redo.
  'consumer:after-offset-commit': (r) => expect(r.duplicatesProcessed).toBe(0),
  // Key inflight, nothing produced: retries get 409 until the inflight TTL, then the event is produced ONCE.
  'api:before-produce': (r) => {
    expect(r.duplicatesProduced).toBe(0);
    expect(r.duplicatesProcessed).toBe(0);
  },
  // Produced but the key is still inflight: after the TTL the retry produces a SECOND copy (the gap of API
  // idempotency); the sink's key absorbs it and counts it.
  'api:after-produce-before-complete': (r) => {
    expect(r.duplicatesProduced).toBe(1);
    expect(r.duplicatesProcessed).toBe(1);
  },
  // Retry tier crash with the row's transaction open: rolled back, the retry record is processed again: no loss.
  'retry:before-insert': (r) => expect(r.duplicatesProduced).toBe(0),
  // Retry record acked, source offset not committed: the main consumer re-reads the chunk. Its marked rows go to
  // tier 1 again (or are stored directly): duplicates may be processed, never stored twice.
  'retry:after-retry-publish-before-commit': (r) => expect(r.duplicatesProduced).toBe(0),
  // DLQ record acked, no dlq_meta row: re-read, published again (a DLQ duplicate), indexed ONCE.
  'dlq:after-dlq-publish-before-meta': (r) => expect(r.duplicatesProduced).toBeGreaterThanOrEqual(1),
  // DLQ record + dlq_meta row, offset not committed: published again, the second index insert is a counted duplicate.
  'dlq:after-meta-before-commit': (r) => {
    expect(r.duplicatesProduced).toBeGreaterThanOrEqual(1);
    expect(r.notes).toMatch(/DLQ duplicates counted [1-9]/);
  },
  // Key done: the retry gets the original result (replayed 202), nothing new is produced.
  'api:after-complete-before-response': (r) => {
    expect(r.duplicatesProduced).toBe(0);
    expect(r.notes).toMatch(/1x replayed/);
  },
};

describe('crash matrix', () => {
  const store = new StoreProbe();
  const results: CrashResult[] = [];

  afterAll(async () => {
    await store.end();
    if (only.length > 0 || results.length !== CRASH_POINTS.length) return; // partial run: keep the last full table
    let commit = 'unknown';
    try {
      commit = execFileSync('git', ['rev-parse', '--short', 'HEAD'], { encoding: 'utf8' }).trim();
      if (execFileSync('git', ['status', '--porcelain'], { encoding: 'utf8' }).trim()) commit += '+dirty';
    } catch {
      /* not a git checkout */
    }
    const matrix = { generatedAt: new Date().toISOString(), commit, results };
    mkdirSync(OUT, { recursive: true });
    writeFileSync(resolve(OUT, 'crash-matrix.json'), `${JSON.stringify(matrix, null, 2)}\n`);
    writeFileSync(resolve(OUT, 'crash-matrix.md'), renderMarkdown(matrix));
  });

  it.each(points.map((p) => [p.name, p] as const))('[G2.crash-matrix] %s', async (_name, point) => {
    const scenario = SCENARIOS[point.component];
    expect(scenario, `no scenario for component ${point.component}`).toBeDefined();
    const r = await scenario(point, store);
    results.push(r);
    console.info(`[crash-matrix] ${JSON.stringify(r)}`);
    expect(r.crashed).toBe(true);
    expect(r.lost).toBe(0);
    expect(r.duplicatesStored).toBe(0);
    if (point.component === 'consumer') expect(r.duplicatesProduced).toBe(0); // consumer crashes never touch the log
    // retry / dlq rows: duplicatesProduced counts records in the source (retry) or dead letters published twice (dlq)
    EXPECT[point.name]?.(r);
  });
});
