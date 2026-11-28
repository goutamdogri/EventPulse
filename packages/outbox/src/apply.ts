// Consumer side of config.changed (S3-F4; Stage 4 cache invalidation will use it).
//
// The relay is at-least-once (a crash between publish and mark republishes) and, in RELAY_MODE=multi, may publish a
// tenant's changes out of order. Both are harmless to a consumer that keeps, per tenant, the highest version it has
// applied: an event is applied only if event.version > current.version. Because every event is a FULL snapshot,
// skipping an older one loses nothing. event_id tells a republished copy (duplicate) from a genuinely older change
// (stale), for metrics; correctness comes from the version alone.

export interface Versioned<T = unknown> {
  version: number;
  value: T;
  eventId?: string;
}

export interface VersionedEvent<T = unknown> {
  eventId: string;
  version: number;
  value: T;
}

export type ApplyOutcome = 'applied' | 'duplicate' | 'stale';

/** Pure: the new state and what happened. `seen` = event ids already handled (optional, for the duplicate label). */
export function applyIfNewer<T>(current: Versioned<T> | undefined, e: VersionedEvent<T>, seen?: ReadonlySet<string>): { state: Versioned<T> | undefined; outcome: ApplyOutcome } {
  if (seen?.has(e.eventId) || current?.eventId === e.eventId) return { state: current, outcome: 'duplicate' };
  if (current && e.version <= current.version) return { state: current, outcome: 'stale' };
  return { state: { version: e.version, value: e.value, eventId: e.eventId }, outcome: 'applied' };
}

/** Per-key view (tenant -> latest config) with counters; remembers the last `maxSeen` event ids. */
export class VersionedView<T = unknown> {
  private readonly state = new Map<string, Versioned<T>>();
  private readonly seen = new Set<string>();
  readonly counts: Record<ApplyOutcome, number> = { applied: 0, duplicate: 0, stale: 0 };

  constructor(private readonly maxSeen = 100_000) {}

  apply(key: string, e: VersionedEvent<T>): ApplyOutcome {
    const r = applyIfNewer(this.state.get(key), e, this.seen);
    if (r.state) this.state.set(key, r.state);
    this.counts[r.outcome]++;
    if (!this.seen.has(e.eventId)) {
      this.seen.add(e.eventId);
      if (this.seen.size > this.maxSeen) this.seen.delete(this.seen.values().next().value!);
    }
    return r.outcome;
  }

  get(key: string): Versioned<T> | undefined {
    return this.state.get(key);
  }
}
