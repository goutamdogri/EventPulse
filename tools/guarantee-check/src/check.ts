// The rules of guarantee-check (S3-F9). Pure: every input is passed in, so the unit tests use fixtures.
//
// Fails (one clear message per problem) when:
//   - a claim misses one of the fixed fields, or has no [test: id] in "Proved by"
//   - a referenced id matches no test in the reports
//   - a referenced test failed, or was skipped / pending / todo (a skipped proof proves nothing)
//   - a required test report is missing, or STALE (made from other source than the current tree)
//   - the crash matrix is missing, stale (points.ts changed since it was generated), incomplete (a registered crash
//     point has no row), or has a row with a crash that did not happen, lost > 0 or stored twice > 0
import { FIELDS, parseDoc } from './doc.ts';
import { idsOf } from './reports.ts';
import type { StampedReport } from './reports.ts';

export interface CrashRow {
  point: string;
  crashed: boolean;
  lost: number;
  duplicatesStored: number;
}

export interface CrashMatrixFile {
  generatedAt: string;
  commit: string;
  pointsHash?: string;
  results: CrashRow[];
}

export interface CheckInput {
  doc: string;
  reports: StampedReport[];
  /** Suites that must have a report (a referenced id can then never be "missing" because a whole suite was not run). */
  requiredSuites: string[];
  sourceHash: string;
  matrix: CrashMatrixFile | null;
  pointNames: string[];
  pointsHash: string;
}

export interface ClaimRow {
  id: string;
  title: string;
  refs: { id: string; tests: number; status: 'ok' | 'missing' | 'failed' | 'not_run' }[];
  ok: boolean;
}

export interface CheckResult {
  ok: boolean;
  errors: string[];
  claims: ClaimRow[];
}

export function checkGuarantees(i: CheckInput): CheckResult {
  const errors: string[] = [];

  // Reports.
  const bySuite = new Map(i.reports.map((r) => [r.suite, r]));
  for (const s of i.requiredSuites) {
    const r = bySuite.get(s);
    if (!r) errors.push(`report ${s}: missing (run it: see docs/delivery-guarantees.md "How this is checked")`);
    else if (r.sourceHash !== i.sourceHash) errors.push(`report ${s}: STALE, made at ${r.commit}${r.dirty ? '+dirty' : ''} on ${r.generatedAt} from other source than the current tree: run the suite again`);
  }
  const testsById = new Map<string, { title: string; status: string; suite: string }[]>();
  for (const r of i.reports) {
    for (const t of r.tests) for (const id of idsOf(t.title)) (testsById.get(id) ?? testsById.set(id, []).get(id)!).push({ title: t.title, status: t.status, suite: r.suite });
  }

  // Claims.
  const parsed = parseDoc(i.doc);
  if (parsed.claims.length === 0) errors.push('document: no claim found (expected "### G<n> <title>" sections)');
  const seen = new Set<string>();
  const claims: ClaimRow[] = [];
  for (const c of parsed.claims) {
    if (seen.has(c.id)) errors.push(`${c.id} (line ${c.line}): claim id used twice`);
    seen.add(c.id);
    for (const f of FIELDS) if (!c.fields[f]) errors.push(`${c.id} (line ${c.line}): missing field "- ${f}:"`);
    if (c.refs.length === 0) errors.push(`${c.id} (line ${c.line}): no test reference in "Proved by" (every claim needs [test: <id>])`);
    const refs: ClaimRow['refs'] = [];
    for (const id of c.refs) {
      const ts = testsById.get(id) ?? [];
      if (ts.length === 0) {
        errors.push(`${c.id}: [test: ${id}] does not exist in any report (renamed or removed test?)`);
        refs.push({ id, tests: 0, status: 'missing' });
        continue;
      }
      const failed = ts.filter((t) => t.status === 'failed');
      const notRun = ts.filter((t) => t.status !== 'passed' && t.status !== 'failed');
      for (const t of failed) errors.push(`${c.id}: [test: ${id}] FAILED: ${t.suite} > ${t.title}`);
      for (const t of notRun) errors.push(`${c.id}: [test: ${id}] was ${t.status}, not run: ${t.suite} > ${t.title}`);
      refs.push({ id, tests: ts.length, status: failed.length ? 'failed' : notRun.length ? 'not_run' : 'ok' });
    }
    claims.push({ id: c.id, title: c.title, refs, ok: refs.length > 0 && refs.every((r) => r.status === 'ok') });
  }
  // A [test: id] outside a claim (e.g. in prose) must still exist.
  const inClaims = new Set(parsed.claims.flatMap((c) => c.refs));
  for (const r of parsed.allRefs) if (!inClaims.has(r.id) && !testsById.has(r.id)) errors.push(`line ${r.line}: [test: ${r.id}] does not exist in any report`);

  // Crash matrix.
  const m = i.matrix;
  if (!m) errors.push('crash matrix: docs/generated/crash-matrix.json missing (make correctness generates it)');
  else {
    if (m.pointsHash !== i.pointsHash) errors.push(`crash matrix: STALE, generated ${m.generatedAt} at ${m.commit} before the last change to packages/fault-injector/src/points.ts: run make correctness`);
    const rows = new Set(m.results.map((r) => r.point));
    for (const p of i.pointNames) if (!rows.has(p)) errors.push(`crash matrix: no row for crash point ${p}`);
    for (const r of m.results) {
      if (!r.crashed) errors.push(`crash matrix: ${r.point}: the process did not crash at the point`);
      if (r.lost > 0) errors.push(`crash matrix: ${r.point}: ${r.lost} event(s) LOST`);
      if (r.duplicatesStored > 0) errors.push(`crash matrix: ${r.point}: ${r.duplicatesStored} event(s) STORED TWICE`);
    }
  }
  return { ok: errors.length === 0, errors, claims };
}
