// Approximate quantile from a Prometheus histogram's cumulative buckets (linear interpolation inside the
// bucket), the same idea as PromQL's histogram_quantile(). Used for the p50/p99 fields of /debug/stats.

/** Any prom-client histogram (labelled or not). */
export interface HistogramLike {
  get(): Promise<{ values: { value: number; labels: Record<string, unknown>; metricName?: string }[] }>;
}

export async function histQuantile(h: HistogramLike, q: number): Promise<number | null> {
  const values = (await h.get()).values;
  const buckets = values
    .filter((v) => v.metricName?.endsWith('_bucket'))
    .map((v) => ({ le: v.labels.le === '+Inf' ? Infinity : Number(v.labels.le), count: v.value }));
  // Sum bucket counts per `le` across label sets, then sort.
  const byLe = new Map<number, number>();
  for (const b of buckets) byLe.set(b.le, (byLe.get(b.le) ?? 0) + b.count);
  const sorted = [...byLe.entries()].sort((a, b) => a[0] - b[0]);
  const total = sorted.at(-1)?.[1] ?? 0;
  if (total === 0) return null;
  const rank = q * total;
  let prevLe = 0;
  let prevCount = 0;
  for (const [le, count] of sorted) {
    if (count >= rank) {
      if (le === Infinity) return prevLe; // above the last finite bucket: report its upper bound
      const inBucket = count - prevCount;
      return inBucket === 0 ? le : prevLe + ((le - prevLe) * (rank - prevCount)) / inBucket;
    }
    prevLe = le;
    prevCount = count;
  }
  return prevLe;
}

export const round = (x: number | null, digits = 3) => (x === null ? null : Number(x.toFixed(digits)));
