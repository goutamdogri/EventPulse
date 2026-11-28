// Dead letters (S2-F4, extended in S3-F5).
//
// - Value = the ORIGINAL raw bytes of the record (not re-encoded), key = the original key, so what failed can be
//   inspected and replayed exactly.
// - Headers = the original headers (retry.* stripped) + dlq.* headers describing where it came from and why it
//   failed. "Where it came from" is the FIRST place it was consumed (events.raw partition/offset), even when it
//   failed for the last time on a retry tier (that one is dlq.failed_topic).
// - Dedicated idempotent producer, acks=all, one send per dead letter so each one's DLQ offset is known.
// - DlqMetaStore: one dlq_meta row per SOURCE record (UNIQUE source topic/partition/offset): a dead letter
//   re-published after a crash is a counted duplicate, never a second row.

import type { Pool } from 'pg';
import { createKafka } from '@eventpulse/kafka-config';
import type { KafkaClientConfig, Producer } from '@eventpulse/kafka-config';
import { DLQ_HEADERS, originalHeaders, truncate } from '@eventpulse/retry';
import type { ErrorClass, Headers, Origin } from '@eventpulse/retry';
import type { RawMessage } from './parse.js';

export interface DeadLetter {
  raw: RawMessage;
  errorClass: ErrorClass;
  errorCode: string;
  errorMessage: string;
  /** Retry-tier attempts made before it was dead-lettered (0 = straight from the main consumer). */
  attempts: number;
  /** Where it was first consumed and when it first failed (from the retry headers); default: raw + now. */
  origin?: Origin;
  /** Topic it failed on the last time; default: the consumer's source topic. */
  failedTopic?: string;
  tenantId?: string;
  eventId?: string;
}

export interface DlqContext {
  sourceTopic: string;
  groupId: string;
}

// Header names live in packages/retry (shared with tools/dlq-replay).
export { DLQ_HEADERS };

export function originOf(d: DeadLetter, ctx: DlqContext, now: Date): Origin {
  return d.origin ?? { topic: ctx.sourceTopic, partition: d.raw.partition, offset: d.raw.offset, firstFailedAt: now.toISOString() };
}

/** One DLQ record. headers: the original ones (any name, replay.* included) + the dlq.* set. */
export interface DlqMessage {
  key: RawMessage['key'];
  value: Buffer | null;
  headers: Headers;
}

/** Builds the DLQ record for one dead letter (pure, unit-tested). */
export function toDlqMessage(d: DeadLetter, ctx: DlqContext, now = new Date()): DlqMessage {
  const origin = originOf(d, ctx, now);
  return {
    key: d.raw.key ?? null,
    value: d.raw.value ?? null,
    headers: {
      ...originalHeaders(d.raw.headers),
      [DLQ_HEADERS.originalTopic]: origin.topic,
      [DLQ_HEADERS.originalPartition]: String(origin.partition),
      [DLQ_HEADERS.originalOffset]: origin.offset,
      [DLQ_HEADERS.failedTopic]: d.failedTopic ?? ctx.sourceTopic,
      [DLQ_HEADERS.consumerGroup]: ctx.groupId,
      [DLQ_HEADERS.errorClass]: d.errorClass,
      [DLQ_HEADERS.errorCode]: d.errorCode,
      [DLQ_HEADERS.errorMessage]: truncate(d.errorMessage, 1024),
      [DLQ_HEADERS.attempts]: String(d.attempts),
      [DLQ_HEADERS.firstFailedAt]: origin.firstFailedAt,
      [DLQ_HEADERS.failedAt]: now.toISOString(),
    },
  };
}

/** A dead letter after Kafka acknowledged it: where it sits in the DLQ topic. */
export interface PublishedLetter {
  letter: DeadLetter;
  origin: Origin;
  failedTopic: string;
  dlqTopic: string;
  dlqPartition: number;
  dlqOffset: string;
  failedAt: Date;
}

export interface DlqSink {
  /** Resolves only when every dead letter is acknowledged by Kafka (acks=all). */
  publish(letters: DeadLetter[]): Promise<PublishedLetter[]>;
}

export interface DlqRecorder {
  /** Index published dead letters in dlq_meta; returns how many were new (the rest are DLQ duplicates). */
  record(published: PublishedLetter[]): Promise<{ inserted: number; duplicates: number }>;
}

export class DlqPublisher implements DlqSink {
  private readonly producer: Producer;

  constructor(
    kafka: KafkaClientConfig,
    readonly dlqTopic: string,
    private readonly ctx: DlqContext,
  ) {
    this.producer = createKafka({ ...kafka, clientId: `${kafka.clientId}-dlq` }).producer({
      kafkaJS: { acks: -1, idempotent: true, allowAutoTopicCreation: false },
      'linger.ms': 0,
      'message.timeout.ms': 15_000,
    } as any);
  }

  connect() {
    return this.producer.connect();
  }

  disconnect() {
    return this.producer.disconnect();
  }

  async publish(letters: DeadLetter[]): Promise<PublishedLetter[]> {
    if (letters.length === 0) return [];
    const now = new Date();
    return Promise.all(
      letters.map(async (letter) => {
        const md = await this.producer.send({ topic: this.dlqTopic, messages: [toDlqMessage(letter, this.ctx, now)] as any });
        const m = md[0] as { partition: number; offset?: string; baseOffset?: string };
        return {
          letter,
          origin: originOf(letter, this.ctx, now),
          failedTopic: letter.failedTopic ?? this.ctx.sourceTopic,
          dlqTopic: this.dlqTopic,
          dlqPartition: m.partition,
          dlqOffset: String(m.offset ?? m.baseOffset ?? -1),
          failedAt: now,
        };
      }),
    );
  }
}

export const DLQ_META_INSERT = `
  INSERT INTO dlq_meta (source_topic, source_partition, source_offset, failed_topic, dlq_topic, dlq_partition, dlq_offset,
                        tenant_id, event_id, error_class, error_code, error_message, attempts, first_failed_at, last_failed_at)
  SELECT * FROM unnest($1::text[], $2::int[], $3::bigint[], $4::text[], $5::text[], $6::int[], $7::bigint[],
                       $8::text[], $9::uuid[], $10::text[], $11::text[], $12::text[], $13::int[], $14::timestamptz[], $15::timestamptz[])
  ON CONFLICT (source_topic, source_partition, source_offset) DO NOTHING
  RETURNING id`;

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function dlqMetaColumns(ps: PublishedLetter[]): unknown[][] {
  return [
    ps.map((p) => p.origin.topic),
    ps.map((p) => p.origin.partition),
    ps.map((p) => p.origin.offset),
    ps.map((p) => p.failedTopic),
    ps.map((p) => p.dlqTopic),
    ps.map((p) => p.dlqPartition),
    ps.map((p) => p.dlqOffset),
    ps.map((p) => p.letter.tenantId ?? null),
    ps.map((p) => (p.letter.eventId && UUID_RE.test(p.letter.eventId) ? p.letter.eventId : null)),
    ps.map((p) => p.letter.errorClass),
    ps.map((p) => p.letter.errorCode),
    ps.map((p) => truncate(p.letter.errorMessage, 1024)),
    ps.map((p) => p.letter.attempts),
    ps.map((p) => p.origin.firstFailedAt),
    ps.map((p) => p.failedAt.toISOString()),
  ];
}

export class DlqMetaStore implements DlqRecorder {
  constructor(private readonly pool: Pick<Pool, 'query'>) {}

  async record(published: PublishedLetter[]): Promise<{ inserted: number; duplicates: number }> {
    if (published.length === 0) return { inserted: 0, duplicates: 0 };
    const r = await this.pool.query(DLQ_META_INSERT, dlqMetaColumns(published));
    const inserted = r.rowCount ?? r.rows.length;
    return { inserted, duplicates: published.length - inserted };
  }

  /** For the ep_dlq_size / ep_dlq_oldest_unreplayed_age_seconds gauges. */
  async sizes(): Promise<{ unreplayed: number; oldestAgeS: number | null }> {
    const r = await this.pool.query<{ n: string; age: string | null }>(
      'SELECT count(*) AS n, extract(epoch FROM now() - min(first_failed_at)) AS age FROM dlq_meta WHERE replayed_at IS NULL',
    );
    return { unreplayed: Number(r.rows[0].n), oldestAgeS: r.rows[0].age === null ? null : Number(r.rows[0].age) };
  }
}
