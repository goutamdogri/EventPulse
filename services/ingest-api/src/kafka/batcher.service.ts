import { BeforeApplicationShutdown, Inject, Injectable } from '@nestjs/common';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import { CircuitBreaker } from './breaker.js';
import { MicroBatcher } from './micro-batcher.js';
import type { BatchItem } from './micro-batcher.js';
import { ProducerService } from './producer.service.js';

/** Nest wrapper around MicroBatcher: one batcher per process, sending through the ProducerService. */
@Injectable()
export class BatcherService implements BeforeApplicationShutdown {
  private readonly batcher: MicroBatcher;
  readonly breaker: CircuitBreaker;
  /** Rejections by reason (overloaded, breaker_open, ...), for /debug/stats. S2-F6 turns this into metrics. */
  readonly rejected: Record<string, number> = {};

  constructor(
    @Inject(CONFIG) private readonly cfg: AppConfig,
    producer: ProducerService,
  ) {
    this.breaker = new CircuitBreaker(cfg.breaker.failures, cfg.breaker.cooldownMs);
    this.batcher = new MicroBatcher(
      producer.send,
      cfg.batcher,
      { onReject: (r) => (this.rejected[r] = (this.rejected[r] ?? 0) + 1) },
      this.breaker,
    );
  }

  enqueue(item: BatchItem): Promise<void> {
    return this.batcher.enqueue(item);
  }

  stats() {
    return { ...this.batcher.stats(), rejected: { ...this.rejected } };
  }

  // Before the producer disconnects (onApplicationShutdown): answer every queued request first,
  // but never wait longer than the deadline (Kafka may be down).
  async beforeApplicationShutdown() {
    await this.batcher.drain(this.cfg.shutdownDrainTimeoutMs);
  }
}
