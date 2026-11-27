// Consumer lag (S2-F6): log end offset - committed offset, per partition, for the whole group.
// computeLag is pure (unit-tested); LagMonitor polls the admin API every LAG_POLL_MS and stores the result in
// SinkStats, so /debug/stats and /metrics only read cached values (no Kafka call inside an HTTP handler).

import { createKafka } from '@eventpulse/kafka-config';
import type { KafkaClientConfig } from '@eventpulse/kafka-config';
import type { PartitionLag, SinkStats } from './sink-stats.js';

export interface EndOffset {
  partition: number;
  high: string | number;
  low: string | number;
}
export interface CommittedOffset {
  partition: number;
  offset: string | number;
}

/**
 * Lag per partition. A partition the group never committed (offset -1) lags by everything still in the log
 * (end - log start), which is what a member starting from the beginning would have to read.
 */
export function computeLag(ends: EndOffset[], committed: CommittedOffset[]): PartitionLag[] {
  const byPart = new Map(committed.map((c) => [c.partition, Number(c.offset)]));
  return ends
    .map((e) => {
      const end = Number(e.high);
      const low = Number(e.low);
      const c = byPart.get(e.partition);
      const has = c !== undefined && c >= 0;
      return { partition: e.partition, end, committed: has ? c : -1, lag: Math.max(0, end - (has ? c : low)) };
    })
    .sort((a, b) => a.partition - b.partition);
}

const ERR__STATE = -172;

export class LagMonitor {
  private admin?: ReturnType<ReturnType<typeof createKafka>['admin']>;
  private timer?: NodeJS.Timeout;
  private stopped = false;

  constructor(
    private readonly kafka: KafkaClientConfig,
    private readonly groupId: string,
    private readonly topic: string,
    private readonly intervalMs: number,
    private readonly stats: SinkStats,
  ) {}

  start() {
    if (this.intervalMs <= 0) return;
    void this.loop();
  }

  private async loop() {
    while (!this.stopped) {
      await this.pollOnce();
      if (this.stopped) return;
      await new Promise<void>((r) => {
        this.timer = setTimeout(r, this.intervalMs);
        this.timer.unref();
      });
    }
  }

  async pollOnce() {
    try {
      if (!this.admin) {
        const admin = createKafka({ ...this.kafka, clientId: `${this.kafka.clientId}-lag`, logLevel: 'nothing' }).admin();
        try {
          await admin.connect();
        } catch (e) {
          await admin.disconnect().catch(() => undefined);
          throw e;
        }
        this.admin = admin;
      }
      const ends = await this.admin.fetchTopicOffsets(this.topic);
      const committed = await this.admin.fetchOffsets({ groupId: this.groupId, topics: [this.topic] });
      this.stats.setLag(computeLag(ends as EndOffset[], (committed[0]?.partitions ?? []) as CommittedOffset[]));
    } catch (e) {
      if ((e as { code?: number })?.code === ERR__STATE && this.admin) {
        void this.admin.disconnect().catch(() => undefined);
        this.admin = undefined;
      }
      this.stats.setLag(this.stats.lag?.byPartition ?? [], String((e as Error)?.message ?? e).slice(0, 200));
    }
  }

  async stop() {
    this.stopped = true;
    if (this.timer) clearTimeout(this.timer);
    const admin = this.admin;
    this.admin = undefined;
    if (admin) await Promise.race([admin.disconnect().catch(() => undefined), new Promise((r) => setTimeout(r, 2000))]);
  }
}
