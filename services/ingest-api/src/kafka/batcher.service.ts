import { BeforeApplicationShutdown, Inject, Injectable } from '@nestjs/common';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import { MicroBatcher } from './micro-batcher.js';
import type { BatchItem } from './micro-batcher.js';
import { ProducerService } from './producer.service.js';

/** Nest wrapper around MicroBatcher: one batcher per process, sending through the ProducerService. */
@Injectable()
export class BatcherService implements BeforeApplicationShutdown {
  private readonly batcher: MicroBatcher;

  constructor(@Inject(CONFIG) cfg: AppConfig, producer: ProducerService) {
    this.batcher = new MicroBatcher(producer.send, cfg.batcher);
  }

  enqueue(item: BatchItem): Promise<void> {
    return this.batcher.enqueue(item);
  }

  stats() {
    return this.batcher.stats();
  }

  // Before the producer disconnects (onApplicationShutdown): answer every queued request first.
  async beforeApplicationShutdown() {
    await this.batcher.drain();
  }
}
