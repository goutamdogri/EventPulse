import {
  BeforeApplicationShutdown,
  Inject,
  Injectable,
  Logger,
  OnApplicationBootstrap,
} from '@nestjs/common';
import type { Pool } from 'pg';
import {
  KafkaJS,
  RdKafkaErrors,
  TOPIC_EVENTS_RAW,
  createKafka,
} from '@eventpulse/kafka-config';
import type { Consumer } from '@eventpulse/kafka-config';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import { PG_POOL } from '../db/db.module.js';
import { SinkStats } from '../observability/sink-stats.js';
import { BatchWriter } from '../sink/batch-writer.js';
import { handleBatch } from './batch-handler.js';

const ASSIGNORS = {
  roundRobin: KafkaJS.PartitionAssigners.roundRobin,
  range: KafkaJS.PartitionAssigners.range,
  cooperativeSticky: KafkaJS.PartitionAssigners.cooperativeSticky,
} as const;

type TopicPartition = { topic: string; partition: number };

/**
 * Member of consumer group sink-postgres. Reads events.raw and writes each batch to Postgres.
 *
 * - autoCommit is off from the start: offsets are committed by the batch handler, after the DB commit.
 * - Bounded prefetch: maxBytesPerPartition / maxBytes cap what is fetched and held in memory, and a partition
 *   is not handed a new batch until its current eachBatch call returns. That is the backpressure.
 * - If eachBatch throws, the client seeks back to the first unresolved offset and the batch is retried.
 */
@Injectable()
export class SinkConsumer implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly log = new Logger(SinkConsumer.name);
  private readonly consumer: Consumer;
  private readonly writer: BatchWriter;
  private assignedOnce!: Promise<void>;
  private markAssigned!: () => void;
  private running = false;

  constructor(
    @Inject(CONFIG) private readonly cfg: AppConfig,
    @Inject(PG_POOL) pool: Pool,
    readonly stats: SinkStats,
  ) {
    this.writer = new BatchWriter(pool);
    this.assignedOnce = new Promise((r) => (this.markAssigned = r));
    const c = cfg.consumer;
    this.consumer = createKafka(cfg.kafka).consumer({
      kafkaJS: {
        groupId: c.groupId,
        fromBeginning: c.fromBeginning,
        autoCommit: false,
        sessionTimeout: c.sessionTimeoutMs,
        heartbeatInterval: c.heartbeatIntervalMs,
        rebalanceTimeout: c.rebalanceTimeoutMs,
        maxBytesPerPartition: c.maxBytesPerPartition,
        maxBytes: c.maxBytes,
        maxWaitTimeInMs: c.maxWaitMs,
        partitionAssigners: [ASSIGNORS[c.assignor]],
        allowAutoTopicCreation: false,
      },
      // librdkafka / client properties (outside the kafkaJS block):
      'js.consumer.max.batch.size': c.maxBatchSize,
      rebalance_cb: (err: { code: number }, assignment: TopicPartition[]) => this.onRebalance(err, assignment),
    } as any);
  }

  async onApplicationBootstrap() {
    await this.consumer.connect();
    await this.consumer.subscribe({ topics: [TOPIC_EVENTS_RAW] });
    await this.consumer.run({
      eachBatchAutoResolve: false,
      partitionsConsumedConcurrently: this.cfg.consumer.partitionsConcurrently,
      eachBatch: async (payload) => {
        const { batch } = payload;
        try {
          await handleBatch(
            {
              messages: batch.messages.map((m) => ({
                partition: batch.partition,
                offset: m.offset,
                key: m.key,
                value: m.value,
                headers: m.headers as Record<string, unknown> | undefined,
              })),
              resolveOffset: (offset) => payload.resolveOffset(offset),
              commitOffsetsIfNecessary: () => payload.commitOffsetsIfNecessary(),
            },
            { writer: this.writer, stats: this.stats, maxRows: this.cfg.sink.maxRows },
          );
        } catch (e) {
          this.stats.batchErrors++;
          this.stats.lastError = { at: new Date().toISOString(), message: (e as Error).message };
          throw e; // the client seeks back: the batch is retried
        }
      },
    });
    this.running = true;
    this.log.log(`joined group ${this.cfg.consumer.groupId}, subscribed to ${TOPIC_EVENTS_RAW}`);
  }

  /** Resolves when this member received its first partition assignment (used by tests). */
  waitForAssignment(): Promise<void> {
    return this.assignedOnce;
  }

  assignment(): TopicPartition[] {
    return this.running ? this.consumer.assignment() : [];
  }

  // Before the pool closes (onApplicationShutdown): leave the group, letting the current batch finish.
  async beforeApplicationShutdown() {
    if (!this.running) return;
    this.running = false;
    await this.consumer.disconnect();
    this.log.log('left the group');
  }

  private onRebalance(err: { code: number }, assignment: TopicPartition[]) {
    const parts = assignment.map((tp) => `${tp.topic}:${tp.partition}`);
    if (err.code === RdKafkaErrors.ERR__ASSIGN_PARTITIONS) {
      this.stats.rebalances++;
      parts.forEach((p) => this.stats.assigned.add(p));
      this.log.log(`assigned ${parts.join(', ') || '(none)'}; now own ${[...this.stats.assigned].sort().join(', ') || '(none)'}`);
      if (assignment.length > 0) this.markAssigned();
    } else if (err.code === RdKafkaErrors.ERR__REVOKE_PARTITIONS) {
      parts.forEach((p) => this.stats.assigned.delete(p));
      this.log.log(`revoked ${parts.join(', ') || '(none)'}`);
    }
    // Returning nothing keeps the client's default assign/unassign behavior.
  }
}
