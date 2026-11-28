// Token bucket (S3-F5): bounds how many records per second may be sent to retry topics. When a bad deploy or a
// misclassified outage makes everything "retriable", the bucket runs dry and the consumer switches to pausing the
// partition (blocking) instead of flooding Kafka and Postgres with retries (C9).

export class TokenBucket {
  private tokens: number;
  private last: number;

  constructor(
    private readonly ratePerS: number,
    private readonly burst: number = Math.max(1, ratePerS),
    private readonly now: () => number = Date.now,
  ) {
    this.tokens = this.burst;
    this.last = now();
  }

  /** Take n tokens if available (all or nothing). rate 0 = unlimited. */
  tryTake(n = 1): boolean {
    if (this.ratePerS <= 0) return true;
    const t = this.now();
    this.tokens = Math.min(this.burst, this.tokens + ((t - this.last) / 1000) * this.ratePerS);
    this.last = t;
    if (this.tokens < n) return false;
    this.tokens -= n;
    return true;
  }
}
