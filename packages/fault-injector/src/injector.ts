// Debug-only fault injection: crash a process (SIGKILL) at an exact named step, or make a step throw a chosen
// error. Every Stage 3 failure test depends on this being exact and repeatable.
//
//   DEBUG_FAULTS=true                                   required for anything below (never on by accident)
//   FAULTS=<point>[@N | @bN][,...]                      crash: @N = on the Nth hit of that point in this process,
//                                                       @bN = on the first hit during the Nth batch (startBatch());
//                                                       no suffix = first hit
//   FAULT_ERRORS=<point>=<CODE>[x<TIMES>|x*][:marker=<TEXT>][,...]
//                                                       throw an error whose .code is CODE (a SQLSTATE such as
//                                                       40001, or a Node code such as ECONNRESET) TIMES times
//                                                       (default 1, * = always); with a marker, only when one of
//                                                       the items at that point contains TEXT
//   CRASH_AT=before_parse|after_parse|after_db_commit   Stage 2 form (K5), kept: same as FAULTS=consumer:<point>@bN
//   CRASH_AFTER_BATCHES=N                               with N = CRASH_AFTER_BATCHES (default 1)
//
// The crash is a real SIGKILL: no cleanup, no shutdown hooks, no offset commit. A line is written to stderr
// first (synchronously) so tests can see which point fired: "[fault] CRASH_AT=<point> ... rows=<n>".

import { CRASH_POINT_NAMES, LEGACY_CRASH_AT, isCrashPoint } from './points.js';
import type { CrashPointName } from './points.js';

export interface CrashSpec {
  point: CrashPointName;
  nth: number;
  per: 'hit' | 'batch';
}

export interface ErrorSpec {
  point: CrashPointName;
  code: string;
  /** Infinity = every time. */
  times: number;
  marker?: string;
}

export interface FaultConfig {
  enabled: boolean;
  crashes: CrashSpec[];
  errors: ErrorSpec[];
}

export const NO_FAULTS: FaultConfig = Object.freeze({ enabled: false, crashes: [], errors: [] }) as FaultConfig;

export class FaultConfigError extends Error {
  readonly name = 'FaultConfigError';
}

function point(name: string): CrashPointName {
  if (!isCrashPoint(name)) throw new FaultConfigError(`unknown crash point "${name}" (known: ${CRASH_POINT_NAMES.join(', ')})`);
  return name;
}

const split = (s: string | undefined) => (s ?? '').split(',').map((x) => x.trim()).filter(Boolean);

export function parseCrashSpecs(s: string | undefined): CrashSpec[] {
  return split(s).map((item) => {
    const m = item.match(/^([a-z-]+:[a-z-]+)(?:@(b?)(\d+))?$/);
    if (!m) throw new FaultConfigError(`bad FAULTS entry "${item}" (expected <point>[@N|@bN])`);
    const nth = m[3] === undefined ? 1 : Number(m[3]);
    if (nth < 1) throw new FaultConfigError(`bad FAULTS entry "${item}": N must be >= 1`);
    return { point: point(m[1]), nth, per: m[2] === 'b' ? 'batch' : 'hit' };
  });
}

export function parseErrorSpecs(s: string | undefined): ErrorSpec[] {
  return split(s).map((item) => {
    const m = item.match(/^([a-z-]+:[a-z-]+)=([0-9A-Z_]{4,20})(?:x(\d+|\*))?(?::marker=([A-Za-z0-9_.-]{1,64}))?$/);
    if (!m) throw new FaultConfigError(`bad FAULT_ERRORS entry "${item}" (expected <point>=<CODE>[xN|x*][:marker=TEXT])`);
    const times = m[3] === undefined ? 1 : m[3] === '*' ? Infinity : Number(m[3]);
    if (times < 1) throw new FaultConfigError(`bad FAULT_ERRORS entry "${item}": times must be >= 1`);
    return { point: point(m[1]), code: m[2], times, ...(m[4] ? { marker: m[4] } : {}) };
  });
}

const truthy = (v: string | undefined) => /^(1|true|yes|on)$/i.test(v ?? '');

/** Reads FAULTS / FAULT_ERRORS / CRASH_AT / CRASH_AFTER_BATCHES; throws if any is set without DEBUG_FAULTS=true. */
export function loadFaultConfig(env: Record<string, string | undefined>): FaultConfig {
  const enabled = truthy(env.DEBUG_FAULTS);
  const crashes = parseCrashSpecs(env.FAULTS);
  const errors = parseErrorSpecs(env.FAULT_ERRORS);
  const legacy = env.CRASH_AT;
  if (legacy && legacy !== 'none') {
    const p = LEGACY_CRASH_AT[legacy];
    if (!p) throw new FaultConfigError(`unknown CRASH_AT "${legacy}" (known: none, ${Object.keys(LEGACY_CRASH_AT).join(', ')})`);
    const n = Number(env.CRASH_AFTER_BATCHES ?? '1');
    if (!Number.isInteger(n) || n < 1) throw new FaultConfigError('CRASH_AFTER_BATCHES must be an integer >= 1');
    crashes.push({ point: p, nth: n, per: 'batch' });
  }
  if ((crashes.length > 0 || errors.length > 0) && !enabled) {
    throw new FaultConfigError('FAULTS / FAULT_ERRORS / CRASH_AT need DEBUG_FAULTS=true (fault injection is debug-only)');
  }
  return { enabled, crashes, errors };
}

export const describeCrash = (c: CrashSpec) => `${c.point}@${c.per === 'batch' ? 'b' : ''}${c.nth}`;
export const describeError = (e: ErrorSpec) =>
  `${e.point}=${e.code}x${e.times === Infinity ? '*' : e.times}${e.marker ? `:marker=${e.marker}` : ''}`;

export class InjectedFaultError extends Error {
  readonly name = 'InjectedFaultError';
  readonly injected = true;
  constructor(readonly code: string, readonly point: CrashPointName) {
    super(`injected fault ${code} at ${point}`);
  }
}

export interface HitContext {
  /** Size of the unit of work in flight (K5 reads it from the log to predict reprocessing). */
  rows?: number;
}

export class FaultInjector {
  private batchNo = 0;
  private readonly hits = new Map<string, number>();
  private crashes: { spec: CrashSpec; fired: boolean }[] = [];
  private errors: { spec: ErrorSpec; remaining: number; thrown: number }[] = [];

  constructor(
    private readonly cfg: FaultConfig = NO_FAULTS,
    private readonly kill: () => void = () => process.kill(process.pid, 'SIGKILL'),
    private readonly write: (line: string) => void = (l) => process.stderr.write(l),
  ) {
    this.load(cfg.crashes, cfg.errors);
  }

  get enabled() {
    return this.cfg.enabled;
  }

  /** Something is armed (tests and logs). */
  get armed() {
    return this.crashes.some((c) => !c.fired) || this.errors.some((e) => e.remaining > 0);
  }

  /** Call once at the start of every non-empty batch (for @bN specs). */
  startBatch(): void {
    this.batchNo++;
  }

  /** Crash here if an armed spec matches this point now. Cheap when nothing is armed. */
  hit(p: CrashPointName, ctx: HitContext = {}): void {
    if (this.crashes.length === 0) return;
    const n = (this.hits.get(p) ?? 0) + 1;
    this.hits.set(p, n);
    for (const c of this.crashes) {
      if (c.fired || c.spec.point !== p) continue;
      if (c.spec.per === 'hit' ? n !== c.spec.nth : this.batchNo !== c.spec.nth) continue;
      c.fired = true;
      // Sync write so the line is in the log before the process disappears.
      this.write(`[fault] CRASH_AT=${p} (${describeCrash(c.spec)}) hit ${n} batch ${this.batchNo}: SIGKILL pid ${process.pid} rows=${ctx.rows ?? 'n/a'}\n`);
      this.kill();
      return;
    }
  }

  /**
   * Throw an InjectedFaultError here if an armed error spec matches. `items` is only evaluated when a spec with a
   * marker is armed for this point (it is usually the payloads of the rows in flight).
   */
  maybeFail(p: CrashPointName, items?: () => readonly string[]): void {
    if (this.errors.length === 0) return;
    for (const e of this.errors) {
      if (e.remaining <= 0 || e.spec.point !== p) continue;
      if (e.spec.marker !== undefined && !(items?.() ?? []).some((s) => s.includes(e.spec.marker!))) continue;
      e.remaining--;
      e.thrown++;
      throw new InjectedFaultError(e.spec.code, p);
    }
  }

  /** Replace what is armed (POST /debug/faults). Hit counters restart from 0; the batch counter does not. */
  arm(spec: { faults?: string; errors?: string }): void {
    if (!this.cfg.enabled) throw new FaultConfigError('fault injection is disabled (DEBUG_FAULTS is not true)');
    this.load(parseCrashSpecs(spec.faults), parseErrorSpecs(spec.errors));
  }

  disarm(): void {
    this.load([], []);
  }

  describe() {
    return {
      enabled: this.cfg.enabled,
      batch: this.batchNo,
      crashes: this.crashes.map((c) => ({ spec: describeCrash(c.spec), fired: c.fired })),
      errors: this.errors.map((e) => ({
        spec: describeError(e.spec),
        remaining: e.remaining === Infinity ? '*' : e.remaining,
        thrown: e.thrown,
      })),
      hits: Object.fromEntries(this.hits),
    };
  }

  private load(crashes: CrashSpec[], errors: ErrorSpec[]) {
    this.hits.clear();
    this.crashes = crashes.map((spec) => ({ spec, fired: false }));
    this.errors = errors.map((spec) => ({ spec, remaining: spec.times, thrown: 0 }));
  }
}
