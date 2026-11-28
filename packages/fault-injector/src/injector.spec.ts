import {
  FaultConfigError,
  FaultInjector,
  InjectedFaultError,
  loadFaultConfig,
  parseCrashSpecs,
  parseErrorSpecs,
} from './injector.js';
import { CRASH_POINTS, LEGACY_CRASH_AT, isCrashPoint, pointsOf } from './points.js';

const injector = (env: Record<string, string>) => {
  const kills: string[] = [];
  const lines: string[] = [];
  const f = new FaultInjector(loadFaultConfig({ DEBUG_FAULTS: 'true', ...env }), () => kills.push('kill'), (l) => lines.push(l));
  return { f, kills, lines };
};

describe('crash point registry', () => {
  it('names are unique, <component>:<step>, and grouped by component', () => {
    const names = CRASH_POINTS.map((p) => p.name);
    expect(new Set(names).size).toBe(names.length);
    for (const p of CRASH_POINTS) expect(p.name.startsWith(`${p.component}:`)).toBe(true);
    expect(pointsOf('consumer').length).toBeGreaterThanOrEqual(6);
  });

  it('every legacy CRASH_AT maps to a registered point', () => {
    for (const p of Object.values(LEGACY_CRASH_AT)) expect(isCrashPoint(p)).toBe(true);
  });
});

describe('config parsing', () => {
  it('nothing armed and disabled by default', () => {
    expect(loadFaultConfig({})).toEqual({ enabled: false, crashes: [], errors: [] });
  });

  it('anything armed needs DEBUG_FAULTS=true', () => {
    expect(() => loadFaultConfig({ FAULTS: 'consumer:after-parse' })).toThrow(/DEBUG_FAULTS/);
    expect(() => loadFaultConfig({ FAULT_ERRORS: 'consumer:before-insert=40001' })).toThrow(/DEBUG_FAULTS/);
    expect(() => loadFaultConfig({ CRASH_AT: 'after_db_commit' })).toThrow(/DEBUG_FAULTS/);
    expect(loadFaultConfig({ CRASH_AT: 'none' }).crashes).toEqual([]);
  });

  it('FAULTS: @N per hit, @bN per batch, default first hit, several entries', () => {
    expect(parseCrashSpecs('consumer:after-parse, consumer:after-db-commit@3,consumer:before-parse@b4')).toEqual([
      { point: 'consumer:after-parse', nth: 1, per: 'hit' },
      { point: 'consumer:after-db-commit', nth: 3, per: 'hit' },
      { point: 'consumer:before-parse', nth: 4, per: 'batch' },
    ]);
  });

  it('rejects unknown points and malformed entries', () => {
    expect(() => parseCrashSpecs('consumer:somewhere')).toThrow(/unknown crash point/);
    expect(() => parseCrashSpecs('consumer:after-parse@0')).toThrow(FaultConfigError);
    expect(() => parseCrashSpecs('after_parse')).toThrow(FaultConfigError);
    expect(() => parseErrorSpecs('consumer:before-insert=')).toThrow(FaultConfigError);
    expect(() => parseErrorSpecs('consumer:nowhere=40001')).toThrow(/unknown crash point/);
  });

  it('FAULT_ERRORS: code, times (default 1, * = always), optional marker', () => {
    expect(parseErrorSpecs('consumer:before-insert=40001x2:marker=retry-me,consumer:before-insert=ECONNRESETx*')).toEqual([
      { point: 'consumer:before-insert', code: '40001', times: 2, marker: 'retry-me' },
      { point: 'consumer:before-insert', code: 'ECONNRESET', times: Infinity },
    ]);
    expect(parseErrorSpecs('consumer:before-insert=23502')[0].times).toBe(1);
  });

  it('legacy CRASH_AT + CRASH_AFTER_BATCHES map to a per-batch spec', () => {
    expect(loadFaultConfig({ DEBUG_FAULTS: 'true', CRASH_AT: 'after_db_commit', CRASH_AFTER_BATCHES: '3' }).crashes).toEqual([
      { point: 'consumer:after-db-commit', nth: 3, per: 'batch' },
    ]);
    expect(() => loadFaultConfig({ DEBUG_FAULTS: 'true', CRASH_AT: 'somewhere' })).toThrow(/unknown CRASH_AT/);
    expect(() => loadFaultConfig({ DEBUG_FAULTS: 'true', CRASH_AT: 'after_parse', CRASH_AFTER_BATCHES: '0' })).toThrow();
  });
});

describe('crashes', () => {
  it('@N fires on exactly the Nth hit of that point, once, and logs the point and rows', () => {
    const { f, kills, lines } = injector({ FAULTS: 'consumer:after-db-commit@2' });
    f.hit('consumer:after-parse');
    f.hit('consumer:after-db-commit', { rows: 10 });
    expect(kills).toHaveLength(0);
    f.hit('consumer:after-db-commit', { rows: 500 });
    expect(kills).toHaveLength(1);
    expect(lines[0]).toContain('CRASH_AT=consumer:after-db-commit');
    expect(lines[0]).toContain('rows=500');
    f.hit('consumer:after-db-commit');
    expect(kills).toHaveLength(1); // fired already
  });

  it('@bN fires on the first hit during batch N (Stage 2 semantics)', () => {
    const { f, kills } = injector({ CRASH_AT: 'before_parse', CRASH_AFTER_BATCHES: '2' });
    f.startBatch();
    f.hit('consumer:before-parse');
    f.hit('consumer:before-parse'); // second chunk of batch 1
    expect(kills).toHaveLength(0);
    f.startBatch();
    f.hit('consumer:before-parse');
    expect(kills).toHaveLength(1);
  });

  it('does nothing when nothing is armed', () => {
    const kills: string[] = [];
    const f = new FaultInjector(undefined, () => kills.push('k'));
    f.startBatch();
    for (const p of CRASH_POINTS) f.hit(p.name);
    expect(kills).toHaveLength(0);
    expect(f.armed).toBe(false);
  });
});

describe('injected errors', () => {
  it('throws an error with the given code, the given number of times', () => {
    const { f } = injector({ FAULT_ERRORS: 'consumer:before-insert=40001x2' });
    const attempt = () => f.maybeFail('consumer:before-insert');
    expect(attempt).toThrow(InjectedFaultError);
    try {
      attempt();
    } catch (e) {
      expect((e as InjectedFaultError).code).toBe('40001');
    }
    expect(attempt).not.toThrow();
    expect(f.describe().errors[0]).toMatchObject({ remaining: 0, thrown: 2 });
  });

  it('with a marker, only throws when an item contains it (and does not consume a count otherwise)', () => {
    const { f } = injector({ FAULT_ERRORS: 'consumer:before-insert=23502x1:marker=bad-row' });
    expect(() => f.maybeFail('consumer:before-insert', () => ['{"a":1}', '{"b":2}'])).not.toThrow();
    expect(() => f.maybeFail('consumer:before-insert', () => ['{"a":1}', '{"tag":"bad-row"}'])).toThrow(/23502/);
    expect(() => f.maybeFail('consumer:before-insert', () => ['{"tag":"bad-row"}'])).not.toThrow();
  });

  it('x* throws every time', () => {
    const { f } = injector({ FAULT_ERRORS: 'consumer:before-insert=ECONNRESETx*' });
    for (let i = 0; i < 20; i++) expect(() => f.maybeFail('consumer:before-insert')).toThrow();
  });
});

describe('arm / disarm at runtime', () => {
  it('arm replaces what is armed and restarts hit counters; disarm clears', () => {
    const { f, kills } = injector({ FAULTS: 'consumer:after-parse@5' });
    f.hit('consumer:after-parse');
    f.arm({ faults: 'consumer:after-parse@2' });
    f.hit('consumer:after-parse');
    expect(kills).toHaveLength(0);
    f.hit('consumer:after-parse');
    expect(kills).toHaveLength(1);
    f.arm({ errors: 'consumer:before-insert=40001' });
    expect(f.armed).toBe(true);
    f.disarm();
    expect(f.armed).toBe(false);
    expect(() => f.maybeFail('consumer:before-insert')).not.toThrow();
  });

  it('arm is refused when DEBUG_FAULTS is off', () => {
    const f = new FaultInjector(loadFaultConfig({}), () => undefined);
    expect(() => f.arm({ faults: 'consumer:after-parse' })).toThrow(/disabled/);
  });
});
