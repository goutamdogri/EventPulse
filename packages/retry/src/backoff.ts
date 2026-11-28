// Backoff with jitter (S3-F5).
//
// The cap doubles per attempt (base, 2*base, 4*base ... up to max); the delay is drawn below the cap:
//   equal jitter: [cap/2, cap]   (default: never retries "too early", still spreads clients apart)
//   full jitter:  [0, cap]       (spreads best, may retry almost at once)
// attempt starts at 1.

export const JITTERS = ['equal', 'full'] as const;
export type Jitter = (typeof JITTERS)[number];

export function backoffCap(attempt: number, baseMs: number, maxMs: number): number {
  return Math.min(maxMs, baseMs * 2 ** Math.max(0, attempt - 1));
}

export function backoffMs(attempt: number, baseMs: number, maxMs: number, rand: () => number = Math.random, jitter: Jitter = 'equal'): number {
  const cap = backoffCap(attempt, baseMs, maxMs);
  return Math.round(jitter === 'full' ? rand() * cap : cap / 2 + rand() * (cap / 2));
}

/** A fixed delay with jitter (retry tiers: the tier's delay is the cap). */
export function jittered(delayMs: number, rand: () => number = Math.random, jitter: Jitter = 'equal'): number {
  return Math.round(jitter === 'full' ? rand() * delayMs : delayMs / 2 + rand() * (delayMs / 2));
}
