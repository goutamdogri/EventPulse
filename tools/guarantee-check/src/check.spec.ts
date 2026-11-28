import { checkGuarantees } from './check.ts';
import type { CheckInput } from './check.ts';
import { parseDoc } from './doc.ts';
import { hashFiles } from './source-hash.ts';
import { idsOf, testsOf } from './reports.ts';

const claim = (id: string, refs: string, extra = '') => `### ${id} Title of ${id}
- Claim: something holds.
- Conditions: defaults.
- Mechanism: a key.
- Proved by: ${refs}
- Not guaranteed: other things.${extra}
`;

const GOOD_DOC = `# Delivery guarantees

${claim('G1', '[test: G1.a]')}
${claim('G2', '[test: G2.a] [test: G2.crash]')}
## Not guaranteed
- Global ordering.
`;

const report = (suite: string, tests: [string, string][], hash = 'H') => ({
  suite,
  generatedAt: '2026-11-28T00:00:00Z',
  commit: 'abc1234',
  dirty: false,
  sourceHash: hash,
  success: true,
  numTotalTests: tests.length,
  tests: tests.map(([title, status]) => ({ file: 'test/x.e2e-spec.ts', title, status })),
});

function input(o: Partial<CheckInput> = {}): CheckInput {
  return {
    doc: GOOD_DOC,
    reports: [
      report('e2e', [
        ['suite [G1.a] works', 'passed'],
        ['suite [G2.a] works', 'passed'],
        ['crash matrix [G2.crash] consumer:x', 'passed'],
        ['crash matrix [G2.crash] api:y', 'passed'],
      ]),
    ],
    requiredSuites: ['e2e'],
    sourceHash: 'H',
    matrix: { generatedAt: '2026-11-28T00:00:00Z', commit: 'abc1234', pointsHash: 'P', results: [{ point: 'consumer:x', crashed: true, lost: 0, duplicatesStored: 0 }, { point: 'api:y', crashed: true, lost: 0, duplicatesStored: 0 }] },
    pointNames: ['consumer:x', 'api:y'],
    pointsHash: 'P',
    ...o,
  };
}

const errorsOf = (o: Partial<CheckInput>) => checkGuarantees(input(o)).errors.join('\n');

describe('guarantee-check', () => {
  it('a good document, current reports and a clean matrix pass', () => {
    const r = checkGuarantees(input());
    expect(r.errors).toEqual([]);
    expect(r.ok).toBe(true);
    expect(r.claims.map((c) => [c.id, c.ok, c.refs.map((x) => x.tests)])).toEqual([['G1', true, [1]], ['G2', true, [1, 2]]]);
  });

  it('a claim without a test reference fails', () => {
    expect(errorsOf({ doc: GOOD_DOC.replace('[test: G1.a]', 'see the code') })).toMatch(/G1 \(line \d+\): no test reference in "Proved by"/);
  });

  it('a missing field fails', () => {
    expect(errorsOf({ doc: GOOD_DOC.replace('- Mechanism: a key.\n', '') })).toMatch(/G1 \(line \d+\): missing field "- Mechanism:"/);
  });

  it('a referenced id that no test has (renamed or removed) fails', () => {
    expect(errorsOf({ doc: GOOD_DOC.replace('[test: G1.a]', '[test: G1.gone]') })).toMatch(/G1: \[test: G1.gone\] does not exist in any report/);
  });

  it('a failed test fails', () => {
    const reports = [report('e2e', [['suite [G1.a] works', 'failed'], ['suite [G2.a] works', 'passed'], ['m [G2.crash] consumer:x', 'passed']])];
    expect(errorsOf({ reports })).toMatch(/G1: \[test: G1.a\] FAILED: e2e > suite \[G1.a\] works/);
  });

  it('a skipped test fails (a skipped proof proves nothing), also when only ONE of the rows of an id is skipped', () => {
    const reports = [report('e2e', [['suite [G1.a] works', 'passed'], ['suite [G2.a] works', 'passed'], ['m [G2.crash] consumer:x', 'passed'], ['m [G2.crash] api:y', 'skipped']])];
    expect(errorsOf({ reports })).toMatch(/G2: \[test: G2.crash\] was skipped, not run: e2e > m \[G2.crash\] api:y/);
  });

  it('a stale report (made from other source) and a missing one fail', () => {
    expect(errorsOf({ sourceHash: 'OTHER' })).toMatch(/report e2e: STALE, made at abc1234 on .* from other source/);
    expect(errorsOf({ requiredSuites: ['e2e', 'correctness'] })).toMatch(/report correctness: missing/);
  });

  it('a stale crash matrix (points.ts changed since) fails', () => {
    expect(errorsOf({ pointsHash: 'NEW' })).toMatch(/crash matrix: STALE, generated .* before the last change to packages\/fault-injector\/src\/points.ts/);
  });

  it('an incomplete crash matrix (a registered point without a row) fails', () => {
    expect(errorsOf({ pointNames: ['consumer:x', 'api:y', 'relay:z'] })).toMatch(/crash matrix: no row for crash point relay:z/);
  });

  it('a crash matrix row with loss, a double store, or no real crash fails', () => {
    const matrix = { ...input().matrix!, results: [{ point: 'consumer:x', crashed: true, lost: 3, duplicatesStored: 0 }, { point: 'api:y', crashed: false, lost: 0, duplicatesStored: 2 }] };
    const e = errorsOf({ matrix });
    expect(e).toMatch(/consumer:x: 3 event\(s\) LOST/);
    expect(e).toMatch(/api:y: 2 event\(s\) STORED TWICE/);
    expect(e).toMatch(/api:y: the process did not crash at the point/);
    expect(errorsOf({ matrix: null })).toMatch(/crash-matrix.json missing/);
  });

  it('a [test: id] in prose outside a claim must exist too; a claim id used twice fails', () => {
    expect(errorsOf({ doc: `${GOOD_DOC}\nSee also [test: G9.nope].\n` })).toMatch(/\[test: G9.nope\] does not exist/);
    expect(errorsOf({ doc: `${GOOD_DOC}\n${claim('G1', '[test: G1.a]')}` })).toMatch(/G1 \(line \d+\): claim id used twice/);
  });
});

describe('parsing', () => {
  it('claims end at the next heading; continuation lines join the field; refs only from "Proved by"', () => {
    const d = parseDoc(`### G4 Ordering\n- Claim: per key.\n- Proved by: [test: G4.a]\n  [test: G4.b]\n- Not guaranteed: global [test: G5.x] order.\n### G5 Late\n- Claim: late.\n## Other\n- Proved by: [test: G9.z]\n`);
    expect(d.claims.map((c) => [c.id, c.refs])).toEqual([['G4', ['G4.a', 'G4.b']], ['G5', []]]);
    expect(d.allRefs.map((r) => r.id)).toEqual(['G4.a', 'G4.b', 'G5.x', 'G9.z']);
  });

  it('test ids from titles; vitest JSON to report tests (repo-relative files, failed files kept)', () => {
    expect(idsOf('suite [G2.crash-matrix] consumer:after-db-commit')).toEqual(['G2.crash-matrix']);
    expect(idsOf('[setup] no id here')).toEqual([]);
    const tests = testsOf(
      {
        testResults: [
          { name: '/repo/tests/a.e2e-spec.ts', status: 'passed', assertionResults: [{ fullName: 'd [G1.a] x', status: 'passed' }] },
          { name: '/repo/tests/b.e2e-spec.ts', status: 'failed', message: 'beforeAll threw', assertionResults: [] },
        ],
      },
      '/repo',
    );
    expect(tests).toEqual([{ file: 'tests/a.e2e-spec.ts', title: 'd [G1.a] x', status: 'passed' }, { file: 'tests/b.e2e-spec.ts', title: '(file failed: beforeAll threw)', status: 'failed' }]);
  });

  it('source hash: order of files does not matter, any content change does', () => {
    const a = hashFiles([{ path: 'x', content: '1' }, { path: 'y', content: '2' }]);
    expect(hashFiles([{ path: 'y', content: '2' }, { path: 'x', content: '1' }])).toBe(a);
    expect(hashFiles([{ path: 'x', content: '1' }, { path: 'y', content: '3' }])).not.toBe(a);
  });
});
