// Unit checks of the crash-matrix machinery (no infrastructure).
import { CRASH_POINTS } from '@eventpulse/fault-injector';
import { renderMarkdown, verdict } from '../src/crash-matrix/report.js';
import type { CrashResult } from '../src/crash-matrix/report.js';
import { SCENARIOS } from '../src/crash-matrix/scenarios.js';

const result = (o: Partial<CrashResult> = {}): CrashResult => ({
  point: 'consumer:after-db-commit',
  component: 'consumer',
  description: 'rows committed in Postgres, Kafka offset not committed',
  sent: 5000,
  duplicatesProduced: 0,
  storedAtCrash: 2000,
  committedAtCrash: 1500,
  duplicatesProcessed: 500,
  duplicatesStored: 0,
  lost: 0,
  recoveryMs: 4200,
  crashed: true,
  ...o,
});

describe('crash matrix coverage', () => {
  it('every registered crash point has a scenario for its component', () => {
    const missing = CRASH_POINTS.filter((p) => !SCENARIOS[p.component]).map((p) => p.name);
    expect(missing, 'add a scenario in tests/correctness/src/crash-matrix/scenarios.ts').toEqual([]);
  });
});

describe('report', () => {
  it('verdict: ok only if the crash happened and nothing was lost or stored twice', () => {
    expect(verdict(result())).toBe('ok');
    expect(verdict(result({ lost: 1 }))).toBe('FAIL');
    expect(verdict(result({ duplicatesStored: 1 }))).toBe('FAIL');
    expect(verdict(result({ crashed: false }))).toBe('FAIL');
  });

  it('renders one table row per result, with the generation header', () => {
    const md = renderMarkdown({ generatedAt: '2026-11-27T00:00:00Z', commit: 'abc1234', results: [result(), result({ point: 'consumer:before-parse', duplicatesProcessed: 0 })] });
    expect(md).toContain('commit `abc1234`');
    expect(md).toContain('| `consumer:after-db-commit` | rows committed in Postgres, Kafka offset not committed | 5000 | 0 | 2000 | 1500 | 500 | 0 | 0 | 4200 | ok |');
    expect(md.split('\n').filter((l) => l.startsWith('| `'))).toHaveLength(2);
  });
});
