// Simple circuit breaker for the Kafka producer (S2-F5). Plain class with an injectable clock.
//
//   closed    -> normal. `failureThreshold` consecutive failed sends open it.
//   open      -> enqueue() is rejected at once (fast 503, queue stays empty, memory flat) for `cooldownMs`.
//   half-open -> after the cooldown exactly ONE message is let through as a probe; everything else is still
//                rejected until the probe's batch settles. Success closes, failure re-opens for a new cooldown.
//
// failureThreshold = 0 disables the breaker (unbounded mode, K10).
//
// The background Kafka health check (kafka-health.ts) also drives it, so it doesn't depend on user traffic:
// a failed check calls forceOpen() (fail fast at once), a passing check while open/half-open calls onSuccess().

export type BreakerState = 'closed' | 'open' | 'half-open';

export class CircuitBreaker {
  private st: BreakerState = 'closed';
  private failures = 0;
  private openedAt = 0;
  private probeTaken = false;

  constructor(
    private readonly failureThreshold: number,
    private readonly cooldownMs: number,
    private readonly now: () => number = Date.now,
  ) {}

  get enabled() {
    return this.failureThreshold > 0;
  }

  state(): BreakerState {
    if (this.st === 'open' && this.now() - this.openedAt >= this.cooldownMs) return 'half-open';
    return this.st;
  }

  /** Ask before accepting a message. Taking the probe slot is part of the call. */
  tryAcquire(): boolean {
    if (!this.enabled) return true;
    const s = this.state();
    if (s === 'closed') return true;
    if (s === 'half-open') {
      this.st = 'half-open';
      if (this.probeTaken) return false;
      this.probeTaken = true;
      return true;
    }
    return false;
  }

  onSuccess(): void {
    this.failures = 0;
    this.probeTaken = false;
    this.st = 'closed';
  }

  /** Open now (the health check saw Kafka down). Each call restarts the cooldown. No-op when disabled. */
  forceOpen(): void {
    if (!this.enabled) return;
    this.st = 'open';
    this.openedAt = this.now();
    this.probeTaken = false;
  }

  /** Only call for "Kafka unavailable" failures: a bad message must not open the breaker. */
  onFailure(): void {
    if (!this.enabled) return;
    this.failures++;
    if (this.st === 'half-open' || this.failures >= this.failureThreshold) {
      this.st = 'open';
      this.openedAt = this.now();
      this.probeTaken = false;
    }
  }
}
