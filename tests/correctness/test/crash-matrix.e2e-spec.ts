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
    expect(r.duplicatesProduced).toBe(0); // consumer crashes never touch the log
    EXPECT[point.name]?.(r);
  });
});
