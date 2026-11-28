// tools/guarantee-check (S3-F9): every claim in docs/delivery-guarantees.md maps to a passing automated test.
//
//   node tools/guarantee-check/src/cli.ts check
//       Reads docs/delivery-guarantees.md, docs/generated/test-reports/*.json and docs/generated/crash-matrix.json,
//       applies the rules in check.ts, prints one line per claim, exits 1 on any problem. Runs in CI (job
//       guarantees) on the COMMITTED reports, which must match the committed source.
//   node tools/guarantee-check/src/cli.ts stamp <suite> <vitest-json-file>
//       Turns a Vitest --reporter=json output into docs/generated/test-reports/<suite>.json, with the commit and the
//       source hash. Called by `make test-e2e` and `make correctness` after each suite (also when it failed).
import { existsSync, mkdirSync, readFileSync, readdirSync, rmSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { CRASH_POINTS } from '@eventpulse/fault-injector';
import { checkGuarantees } from './check.ts';
import type { CrashMatrixFile } from './check.ts';
import { testsOf } from './reports.ts';
import type { StampedReport } from './reports.ts';
import { fileHash, gitCommit, sourceHash } from './source-hash.ts';

const REPO = resolve(import.meta.dirname, '../../..');
const DOC = resolve(REPO, 'docs/delivery-guarantees.md');
const REPORTS = resolve(REPO, 'docs/generated/test-reports');
const MATRIX = resolve(REPO, 'docs/generated/crash-matrix.json');
export const POINTS_FILE = resolve(REPO, 'packages/fault-injector/src/points.ts');
export const REQUIRED_SUITES = ['consumer-sink-e2e', 'ingest-api-e2e', 'correctness'];

function check(): number {
  if (!existsSync(DOC)) {
    console.error(`missing ${DOC}`);
    return 1;
  }
  const reports: StampedReport[] = existsSync(REPORTS)
    ? readdirSync(REPORTS)
        .filter((f) => f.endsWith('.json'))
        .map((f) => JSON.parse(readFileSync(resolve(REPORTS, f), 'utf8')))
    : [];
  const r = checkGuarantees({
    doc: readFileSync(DOC, 'utf8'),
    reports,
    requiredSuites: REQUIRED_SUITES,
    sourceHash: sourceHash(REPO),
    matrix: existsSync(MATRIX) ? (JSON.parse(readFileSync(MATRIX, 'utf8')) as CrashMatrixFile) : null,
    pointNames: CRASH_POINTS.map((p) => p.name),
    pointsHash: fileHash(POINTS_FILE),
  });
  for (const c of r.claims) {
    console.log(`${c.ok ? 'ok  ' : 'FAIL'} ${c.id.padEnd(4)} ${c.title}`);
    for (const ref of c.refs) console.log(`       ${ref.status === 'ok' ? 'ok' : ref.status.toUpperCase().padEnd(7)} ${ref.id} (${ref.tests} test${ref.tests === 1 ? '' : 's'})`);
  }
  for (const rep of reports) console.log(`report ${rep.suite}: ${rep.tests.length} tests, made at ${rep.commit}${rep.dirty ? '+dirty' : ''} on ${rep.generatedAt}`);
  if (r.ok) {
    console.log(`\nguarantee-check: OK (${r.claims.length} claims, every one proved by passing tests; crash matrix complete, current, 0 lost, 0 stored twice)`);
    return 0;
  }
  console.error(`\nguarantee-check: ${r.errors.length} problem(s):`);
  for (const e of r.errors) console.error(`  - ${e}`);
  return 1;
}

function stamp(suite: string, file: string): number {
  if (!suite || !file) {
    console.error('usage: guarantee-check stamp <suite> <vitest-json-file>');
    return 2;
  }
  if (!existsSync(file)) {
    console.error(`stamp: ${file} not found (the suite did not get to write its report)`);
    return 1;
  }
  const raw = JSON.parse(readFileSync(file, 'utf8'));
  const { commit, dirty } = gitCommit(REPO);
  const out: StampedReport = {
    suite,
    generatedAt: new Date().toISOString(),
    commit,
    dirty,
    sourceHash: sourceHash(REPO),
    success: raw.success === true,
    numTotalTests: raw.numTotalTests ?? 0,
    tests: testsOf(raw, REPO),
  };
  mkdirSync(REPORTS, { recursive: true });
  writeFileSync(resolve(REPORTS, `${suite}.json`), `${JSON.stringify(out, null, 2)}\n`);
  rmSync(file, { force: true });
  console.log(`stamped docs/generated/test-reports/${suite}.json: ${out.tests.length} tests, success ${out.success}`);
  return 0;
}

const [cmd, ...args] = process.argv.slice(2);
const code = cmd === 'stamp' ? stamp(args[0], args[1]) : cmd === 'check' || cmd === undefined ? check() : (console.error('usage: guarantee-check check | stamp <suite> <file>'), 2);
process.exit(code);
