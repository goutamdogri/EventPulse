// Debug-only crash points (S2-F3, reused later by experiment K5).
//
//   CRASH_AT=before_parse|after_parse|after_db_commit   where to crash
//   CRASH_AFTER_BATCHES=N                               on the Nth batch this process handles (1-based)
//   DEBUG_FAULTS=true                                   required, so a crash point never runs by accident
//
// The crash is a real SIGKILL of this process: no cleanup, no shutdown hooks, no offset commit.

export const CRASH_POINTS = ['none', 'before_parse', 'after_parse', 'after_db_commit'] as const;
export type CrashPoint = (typeof CRASH_POINTS)[number];

export interface FaultConfig {
  crashAt: CrashPoint;
  crashAfterBatches: number;
}

export class FaultInjector {
  private batchNo = 0;
  private fired = false;

  constructor(
    private readonly cfg: FaultConfig,
    private readonly kill: () => void = () => process.kill(process.pid, 'SIGKILL'),
  ) {}

  /** Call once at the start of every non-empty batch. */
  startBatch(): void {
    this.batchNo++;
  }

  /** Crash here if this is the configured point on the configured batch. */
  hit(point: Exclude<CrashPoint, 'none'>): void {
    if (this.fired || this.cfg.crashAt !== point || this.batchNo !== this.cfg.crashAfterBatches) return;
    this.fired = true;
    // Sync write so the line is in the log before the process disappears.
    process.stderr.write(`[fault] CRASH_AT=${point} on batch ${this.batchNo}: SIGKILL pid ${process.pid}\n`);
    this.kill();
  }
}
