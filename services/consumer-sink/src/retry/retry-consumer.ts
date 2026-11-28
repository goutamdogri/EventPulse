// The retry-tier consumer (S3-F5): its own consumer group (default sink-postgres-retry) on events.retry.1..N, in
// the same process as the main consumer (SINK_ROLE=both, default) or alone (SINK_ROLE=retry).
// Same parse + write + DLQ path as the main consumer; the difference is the not_before wait (retry-handler.ts).

import { BeforeApplicationShutdown, Inject, Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import type { Pool } from 'pg';
import { RdKafkaErrors, createKafka } from '@eventpulse/kafka-config';
import type { Consumer } from '@eventpulse/kafka-config';
import { FaultInjector } from '@eventpulse/fault-injector';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import { PG_POOL } from '../db/db.module.js';
import { SinkStats } from '../observability/sink-stats.js';
import { BatchWriter } from '../sink/batch-writer.js';
import type { BatchContext } from '../consumer/batch-handler.js';
import { FAULT_INJECTOR } from '../consumer/faults.controller.js';
import { PartitionPauser } from '../consumer/pauser.js';
import { SinkPipeline } from '../consumer/pipeline.js';
import { NotDueError, handleRetryBatch } from './retry-handler.js';

const COMMIT_RETRIES = 3;

@Injectable()
export class RetryConsumer implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly log = new Logger(RetryConsumer.name);
  private consumer?: Consumer;
  private readonly writer: BatchWriter;
  private readonly pauser: PartitionPauser;
  private running = false;
  joined = false;

  constructor(
    @Inject(CONFIG) private readonly cfg: AppConfig,
    @Inject(PG_POOL) pool: Pool,
    private readonly stats: SinkStats,
    private readonly pipeline: SinkPipeline,
    @Inject(FAULT_INJECTOR) private readonly faults: FaultInjector,
  ) {
    this.writer = new BatchWriter(pool, { faults, points: { beforeInsert: 'retry:before-insert' } });
    // Not-due waits are normal and frequent: not logged one by one (ep_retry_paused_partitions, retry.waits).
    this.pauser = new PartitionPauser(() => this.consumer!, stats.paused, this.log, () => this.running, 'retry:', true);
  }

  get enabled() {
    return this.cfg.retryTiers.enabled && this.cfg.role !== 'main';
  }

  async onApplicationBootstrap() {
    if (!this.enabled) return;
    const c = this.cfg.consumer;
    this.consumer = createKafka({ ...this.cfg.kafka, clientId: `${this.cfg.kafka.clientId}-retry` }).consumer({
      kafkaJS: {
        groupId: this.cfg.retryTiers.groupId,
        fromBeginning: true,
        autoCommit: false,
        sessionTimeout: c.sessionTimeoutMs,
        heartbeatInterval: c.heartbeatIntervalMs,
        rebalanceTimeout: c.rebalanceTimeoutMs,
        maxBytesPerPartition: c.maxBytesPerPartition,
        maxWaitTimeInMs: c.maxWaitMs,
        allowAutoTopicCreation: false,
      },
      'js.consumer.max.batch.size': 100,
      rebalance_cb: (err: { code: number }) => {
        if (err.code === RdKafkaErrors.ERR__ASSIGN_PARTITIONS) this.joined = true;
      },
    } as any);
    await this.consumer.connect();
    await this.consumer.subscribe({ topics: this.pipeline.policy.topics });
    await this.consumer.run({
      eachBatchAutoResolve: false,
      partitionsConsumedConcurrently: Math.max(1, Math.min(4, this.cfg.pgPoolMax - this.cfg.consumer.partitionsConcurrently)),
      eachBatch: async (payload) => {
        const { batch } = payload;
        const tier = this.pipeline.policy.topics.indexOf(batch.topic) + 1;
        const ctx: BatchContext = {
          messages: batch.messages.map((m) => ({ partition: batch.partition, offset: m.offset, key: m.key, value: m.value, headers: m.headers as Record<string, unknown> | undefined })),
          resolveOffset: (o) => payload.resolveOffset(o),
          commitOffsetsIfNecessary: () => payload.commitOffsetsIfNecessary(),
          heartbeat: () => payload.heartbeat(),
          isRunning: () => payload.isRunning(),
          isStale: () => payload.isStale(),
          pausePartition: (ms) => this.pauser.pause(batch.topic, batch.partition, ms, 'retry tier'),
        };
        try {
          await handleRetryBatch(ctx, {
            tier,
            topic: batch.topic,
            policy: this.pipeline.policy,
            writer: this.writer,
            stats: this.stats,
            dlq: this.pipeline.dlq,
            dlqMeta: this.pipeline.dlqMeta,
            retryOut: this.pipeline.retryOut,
            limiter: this.pipeline.limiter,
            classifier: this.pipeline.classifier,
            schema: { serde: this.pipeline.serde, acceptJson: this.cfg.schema.acceptJson },
            faults: this.faults,
            pauseMs: this.cfg.retry.pauseMs,
            commit: (x) => this.commit(x),
          });
        } catch (e) {
          // Not due: the partition is paused until not_before; the client seeks back to the unresolved record.
          if (!(e instanceof NotDueError)) {
            this.stats.batchErrors++;
            this.stats.lastError = { at: new Date().toISOString(), message: `retry tier ${tier}: ${(e as Error).message}` };
          }
          throw e;
        }
      },
    });
    this.running = true;
    this.log.log(`joined group ${this.cfg.retryTiers.groupId}, subscribed to ${this.pipeline.policy.topics.join(', ')}`);
  }

  private async commit(ctx: BatchContext) {
    for (let attempt = 0; ; attempt++) {
      try {
        await ctx.commitOffsetsIfNecessary();
        this.stats.commits++;
        return;
      } catch (e) {
        this.stats.countCommitError();
        if (attempt >= COMMIT_RETRIES || !ctx.isRunning() || ctx.isStale()) throw e;
        await new Promise((r) => setTimeout(r, 100 * 2 ** attempt));
      }
    }
  }

  async beforeApplicationShutdown() {
    if (!this.running) return;
    this.running = false;
    this.pauser.clear();
    await this.consumer?.disconnect();
    this.log.log('left the retry group');
  }
}
