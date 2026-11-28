// Test reports the checker reads (S3-F9). `make test-e2e` and `make correctness` run Vitest with the JSON reporter;
// `guarantee-check stamp` turns that output into docs/generated/test-reports/<suite>.json: the result of every
// test plus WHICH SOURCE it ran against (a hash of the source and test files, see source-hash.ts), so a report
// that no longer matches the code is detected as stale, in CI too.

export type TestStatus = 'passed' | 'failed' | 'skipped' | 'pending' | 'todo' | string;

export interface ReportTest {
  file: string;
  title: string;
  status: TestStatus;
}

export interface StampedReport {
  suite: string;
  generatedAt: string;
  commit: string;
  dirty: boolean;
  sourceHash: string;
  success: boolean;
  numTotalTests: number;
  tests: ReportTest[];
}

/** Vitest --reporter=json output -> the tests (file relative to the repo, full title, status). Pure. */
export function testsOf(vitest: any, repoRoot: string): ReportTest[] {
  const out: ReportTest[] = [];
  for (const f of vitest?.testResults ?? []) {
    const file = String(f.name ?? '').startsWith(repoRoot) ? String(f.name).slice(repoRoot.length).replace(/^\/+/, '') : String(f.name ?? '');
    for (const a of f.assertionResults ?? []) out.push({ file, title: String(a.fullName ?? a.title), status: String(a.status) });
    // A file that failed before collecting any test (import error, beforeAll throw) has no assertions: keep it
    // visible as a failed pseudo-test so it cannot hide a referenced id.
    if ((f.assertionResults ?? []).length === 0 && f.status === 'failed') out.push({ file, title: `(file failed: ${String(f.message ?? '').slice(0, 200)})`, status: 'failed' });
  }
  return out;
}

/** "[G2.same-chunk-twice] ..." -> ["G2.same-chunk-twice"]. */
export const TEST_ID_RE = /\[(G\d+\.[A-Za-z0-9][A-Za-z0-9.-]*)\]/g;
export const idsOf = (title: string) => [...title.matchAll(TEST_ID_RE)].map((m) => m[1]);
