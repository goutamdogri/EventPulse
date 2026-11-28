// Pause one partition for a while, then resume it (no sleeping inside eachBatch). Used for "the database is down"
// and, on the retry tiers, for "the head record is not due yet" (S3-F5). A partition that is already paused keeps
// its first timer: after the resume, the record read next decides again (and may pause again).

import type { Logger } from '@nestjs/common';
import type { Consumer } from '@eventpulse/kafka-config';

export class PartitionPauser {
  private readonly timers = new Set<NodeJS.Timeout>();

  constructor(
    private readonly consumer: () => Consumer,
    private readonly paused: Set<string>,
    private readonly log: Logger,
    private readonly running: () => boolean,
    private readonly keyPrefix = '',
    private readonly quiet = false,
  ) {}

  pause(topic: string, partition: number, ms: number, why: string) {
    const key = `${this.keyPrefix}${topic}:${partition}`;
    if (this.paused.has(key)) return;
    this.consumer().pause([{ topic, partitions: [partition] }]);
    this.paused.add(key);
    if (!this.quiet) this.log.warn(`paused ${topic}:${partition} for ${ms} ms (${why})`);
    const t = setTimeout(() => {
      this.timers.delete(t);
      this.paused.delete(key);
      if (!this.running()) return;
      try {
        this.consumer().resume([{ topic, partitions: [partition] }]);
        if (!this.quiet) this.log.log(`resumed ${topic}:${partition}`);
      } catch (e) {
        this.log.error(`resume ${topic}:${partition} failed: ${(e as Error).message}`);
      }
    }, ms);
    this.timers.add(t);
  }

  clear() {
    for (const t of this.timers) clearTimeout(t);
    this.timers.clear();
  }
}
