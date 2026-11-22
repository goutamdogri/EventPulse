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
  createKafka,
} from '@eventpulse/kafka-config';
import type { Consumer } from '@eventpulse/kafka-config';
import { CONFIG } from '../config.js';
import type { AppConfig } from '../config.js';
import { PG_POOL } from '../db/db.module.js';
import { LagMonitor } from '../observability/lag.js';
import { SinkStats } from '../observability/sink-stats.js';
import { BatchWriter } from '../sink/batch-writer.js';
import { DlqPublisher } from '../sink/dlq-publisher.js';
import { handleBatch } from './batch-handler.js';
import { FaultInjector } from './fault-injector.js';

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
  private readonly faults: FaultInjector;
  private readonly dlq: DlqPublisher;
  private readonly lag: LagMonitor;
  private readonly resumeTimers = new Set<NodeJS.Timeout>();
  private assignedOnce!: Promise<void>;
  private markAssigned!: () => void;
  private running = false;

  constructor(
    @Inject(CONFIG) private readonly cfg: AppConfig,
    @Inject(PG_POOL) pool: Pool,
    readonly stats: SinkStats,
  ) {
    this.writer = new BatchWriter(pool, { dedup: cfg.sink.dedup, table: cfg.sink.dedupTable });
    this.faults = new FaultInjector(cfg.faults);
    this.lag = new LagMonitor(cfg.kafka, cfg.consumer.groupId, cfg.consumer.topic, cfg.lagPollMs, stats);
    this.dlq = new DlqPublisher(cfg.kafka, cfg.retry.dlqTopic, { sourceTopic: cfg.consumer.topic, groupId: cfg.consumer.groupId });
    if (!cfg.retry.dlqEnabled) {
      this.log.warn('SINK_DLQ_ENABLED=false: a poison record will block its partition (S2-F3 behavior)');
    }
    if (!cfg.sink.dedup) {
      this.log.warn(`DEBUG: SINK_DEDUP=false, plain INSERT into ${cfg.sink.dedupTable} (no ON CONFLICT): reprocessed rows are stored twice`);
    }
    if (cfg.faults.crashAt !== 'none') {
      this.log.warn(`DEBUG FAULT ARMED: SIGKILL at ${cfg.faults.crashAt} on batch ${cfg.faults.crashAfterBatches}`);
    }
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
    await this.dlq.connect();
    await this.consumer.connect();
    await this.consumer.subscribe({ topics: [this.cfg.consumer.topic] });
    await this.consumer.run({
      eachBatchAutoResolve: false,
      partitionsConsumedConcurrently: this.cfg.consumer.partitionsConcurrently,
      eachBatch: async (payload) => {
        const { batch } = payload;
        {
          // Local lag of this partition after this batch (cheap, this member only; the group lag is polled).
          const lag = Number(batch.offsetLag());
          this.stats.setLocalLag(batch.partition, lag);
        }
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
              heartbeat: () => payload.heartbeat(),
              isRunning: () => payload.isRunning(),
              isStale: () => payload.isStale(),
              pausePartition: (ms) => this.pausePartition(batch.topic, batch.partition, ms),
            },
            {
              writer: this.writer,
              stats: this.stats,
              maxRows: this.cfg.sink.maxRows,
              faults: this.faults,
              processingDelayMs: this.cfg.sink.processingDelayMs,
              dlq: this.dlq,
              retry: this.cfg.retry,
            },
          );
        } catch (e) {
          this.stats.batchErrors++;
          this.stats.lastError = { at: new Date().toISOString(), message: (e as Error).message };
          throw e; // the client seeks back: the batch is retried
        }
      },
    });
    this.running = true;
    this.lag.start();
    this.log.log(`joined group ${this.cfg.consumer.groupId}, subscribed to ${this.cfg.consumer.topic}`);
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
    for (const t of this.resumeTimers) clearTimeout(t);
    this.resumeTimers.clear();
    await this.consumer.disconnect();
    await this.dlq.disconnect();
    await this.lag.stop();
    this.log.log('left the group');
  }

  /**
   * The DB is down: stop fetching this partition for ms, then resume. Its unresolved chunk is read again after
   * the resume, so nothing is lost, and the consumer doesn't spin on a hot retry loop meanwhile.
   */
  private pausePartition(topic: string, partition: number, ms: number) {
    const key = `${topic}:${partition}`;
    if (this.stats.paused.has(key)) return;
    this.consumer.pause([{ topic, partitions: [partition] }]);
    this.stats.paused.add(key);
    this.log.warn(`paused ${key} for ${ms} ms (database unavailable)`);
    const t = setTimeout(() => {
      this.resumeTimers.delete(t);
      this.stats.paused.delete(key);
      if (!this.running) return;
      try {
        this.consumer.resume([{ topic, partitions: [partition] }]);
        this.log.log(`resumed ${key}`);
      } catch (e) {
        this.log.error(`resume ${key} failed: ${(e as Error).message}`);
      }
    }, ms);
    this.resumeTimers.add(t);
  }

  private onRebalance(err: { code: number }, assignment: TopicPartition[]) {
    const parts = assignment.map((tp) => `${tp.topic}:${tp.partition}`);
    if (err.code === RdKafkaErrors.ERR__ASSIGN_PARTITIONS) {
      this.stats.countRebalance();
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
