// Where idempotency keys live. Three operations, each ATOMIC (one Lua script in Redis), so check-and-set can
// never race between two API instances or two requests of one instance:
//
//   acquire(key, value, ttl)            SET key value NX EX ttl; if the key exists, return its current value
//   replace(key, expected, value, ttl)  set value only if the current value is exactly `expected`
//                                       (complete: inflight -> done, only by the request that owns the inflight)
//   remove(key, expected)               delete only if the current value is exactly `expected`
//                                       (release after a failed produce, only by the owner)
//
// "Only if exactly `expected`" is the owner check: the inflight value contains a random owner token, so a slow
// request whose inflight key expired and was re-acquired by another request can't overwrite or delete it.

export type AcquireResult = { acquired: true } | { acquired: false; current: string | null };

export interface IdempotencyStore {
  acquire(key: string, value: string, ttlS: number): Promise<AcquireResult>;
  replace(key: string, expected: string, value: string, ttlS: number): Promise<boolean>;
  remove(key: string, expected: string): Promise<boolean>;
}

/**
 * In-memory store with the same semantics, for unit and property tests (injectable clock for TTLs).
 * Not for production: it is per process, so two API instances would not see each other's keys.
 */
export class MemoryStore implements IdempotencyStore {
  private readonly data = new Map<string, { value: string; expiresAt: number }>();

  constructor(private readonly now: () => number = Date.now) {}

  private live(key: string) {
    const e = this.data.get(key);
    if (e && e.expiresAt <= this.now()) {
      this.data.delete(key);
      return undefined;
    }
    return e;
  }

  async acquire(key: string, value: string, ttlS: number): Promise<AcquireResult> {
    const e = this.live(key);
    if (e) return { acquired: false, current: e.value };
    this.data.set(key, { value, expiresAt: this.now() + ttlS * 1000 });
    return { acquired: true };
  }

  async replace(key: string, expected: string, value: string, ttlS: number): Promise<boolean> {
    if (this.live(key)?.value !== expected) return false;
    this.data.set(key, { value, expiresAt: this.now() + ttlS * 1000 });
    return true;
  }

  async remove(key: string, expected: string): Promise<boolean> {
    if (this.live(key)?.value !== expected) return false;
    this.data.delete(key);
    return true;
  }

  /** Test helpers: what is stored, and losing everything (like a Redis FLUSHALL or restart). */
  peek(key: string): string | undefined {
    return this.live(key)?.value;
  }
  flush(): void {
    this.data.clear();
  }
  get size(): number {
    return this.data.size;
  }
}
