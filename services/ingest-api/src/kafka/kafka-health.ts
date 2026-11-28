// Background Kafka health check (S2-F5 fix, see notebook/fix/step-5.md 5.6).
//
// Why: readiness used to depend only on the breaker, and the breaker only learned about Kafka from user requests.
// With no traffic, /readyz said 200 while Kafka was down, and stayed 503 after Kafka came back until a request
// probed it. Behind a load balancer that only routes to ready instances, that is a deadlock.
//
// Every intervalMs: fetch the topic's metadata (bounded by timeoutMs) and require every partition to have a
// leader and at least minIsr in-sync replicas (what an acks=all write needs; 1 when acks is 0 or 1).
//   - check fails  -> unhealthy; breaker.forceOpen() so requests fail fast without waiting for send timeouts
//   - check passes -> healthy; close the breaker if Kafka just came back or its cooldown is over (the probe,
//                     no traffic needed)
// Readiness = producer connected AND a passing check no older than staleAfterMs AND breaker closed.
//
// The checker is a plain class (unit-tested with a fake admin); KafkaHealthService wires it into Nest.

import type { CircuitBreaker } from './breaker.js';

export interface PartitionMeta {
  leader: number;
  isr: unknown[];
}

export interface HealthAdmin {
  fetchTopicMetadata(o: { topics: string[]; timeout?: number }): Promise<{ partitions: PartitionMeta[] }[]>;
  /**
   * Throw this admin client away: the next check builds a fresh one. Called after EVERY failed check (S3-F9 fix):
   * when a broker is killed abruptly (docker kill, a crashed host) its TCP connection is not reset, and an admin
   * client that sent its metadata request there keeps timing out on that connection long after the cluster has
   * failed over. The check then forced the breaker open forever and the API answered 503 although 2 of 3 brokers
   * were fine (found by [G1.durability]). A fresh client bootstraps again and asks a live broker.
   */
  discard?(): void;
}

export interface HealthResult {
  ok: boolean;
  reason: string;
  at: number;
}

/** Pure decision on a metadata answer. */
export function evaluateMetadata(md: { partitions: PartitionMeta[] }[] | undefined, minIsr: number): { ok: boolean; reason: string } {
  const parts = md?.[0]?.partitions ?? [];
  if (parts.length === 0) return { ok: false, reason: 'no partition metadata' };
  const noLeader = parts.filter((p) => !(p.leader >= 0)).length;
  if (noLeader > 0) return { ok: false, reason: `${noLeader} partition(s) without a leader` };
  const lowIsr = parts.filter((p) => p.isr.length < minIsr).length;
  if (lowIsr > 0) return { ok: false, reason: `${lowIsr} partition(s) below ${minIsr} in-sync replicas` };
  return { ok: true, reason: 'ok' };
}

export interface HealthOptions {
  topic: string;
  minIsr: number;
  timeoutMs: number;
  staleAfterMs: number;
}

export class KafkaHealthChecker {
  private last?: HealthResult;

  constructor(
    private readonly getAdmin: () => Promise<HealthAdmin>,
    private readonly breaker: CircuitBreaker,
    private readonly opts: HealthOptions,
    private readonly now: () => number = Date.now,
  ) {}

  /** One check. Never throws. */
  async check(): Promise<HealthResult> {
    let r: { ok: boolean; reason: string };
    let admin: HealthAdmin | undefined;
    try {
      admin = await this.getAdmin();
      const md = await withTimeout(admin.fetchTopicMetadata({ topics: [this.opts.topic], timeout: this.opts.timeoutMs }), this.opts.timeoutMs);
      r = evaluateMetadata(md, this.opts.minIsr);
    } catch (e) {
      r = { ok: false, reason: `metadata request failed: ${String((e as Error)?.message ?? e).slice(0, 200)}` };
      admin?.discard?.(); // no answer at all: the client itself may be stuck on a dead broker's connection
    }
    const wasOk = this.last?.ok ?? false;
    this.last = { ...r, at: this.now() };
    if (!r.ok) this.breaker.forceOpen();
    // Close the breaker when Kafka just came back, or when its cooldown is over (half-open: this check is the
    // probe). Not while it is still cooling down after real send failures, which would make it flap.
    else if (!wasOk || this.breaker.state() === 'half-open') this.breaker.onSuccess();
    return this.last;
  }

  /** A passing check that is recent enough. */
  healthy(): boolean {
    return !!this.last && this.last.ok && this.now() - this.last.at <= this.opts.staleAfterMs;
  }

  lastResult(): HealthResult | undefined {
    return this.last;
  }
}

function withTimeout<T>(p: Promise<T>, ms: number): Promise<T> {
  let t: NodeJS.Timeout;
  return Promise.race([
    p.finally(() => clearTimeout(t)),
    new Promise<T>((_, reject) => (t = setTimeout(() => reject(new Error(`timed out after ${ms} ms`)), ms))),
  ]);
}
