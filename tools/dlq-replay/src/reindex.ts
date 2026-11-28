// `reindex` (S3-F6): rebuild missing dlq_meta rows from the events.dlq topic. Kafka is the durable quarantine and
// every dead letter carries its own metadata in dlq.* headers, so the index can always be rebuilt (for example when
// the dlq_meta insert failed after the DLQ publish, or the table was lost). Existing rows are left alone
// (ON CONFLICT on the source record); a DLQ duplicate of a record that already has a row is not a second row.
//
// Replay state (replayed_at, replay_count) is not in the topic: a rebuilt row starts unreplayed. Replaying it again
// is harmless (the sink dedups), and the replay.count header still drives the loop guard.

import { DLQ_HEADERS, headerValue } from '@eventpulse/retry';
import type { ScannedRecord } from '@eventpulse/kafka-config';

export interface MetaRow {
  sourceTopic: string;
  sourcePartition: number;
  sourceOffset: string;
  failedTopic: string;
  dlqTopic: string;
  dlqPartition: number;
  dlqOffset: string;
  tenantId: string | null;
  eventId: string | null;
  errorClass: string;
  errorCode: string | null;
  errorMessage: string | null;
  attempts: number;
  firstFailedAt: string;
  lastFailedAt: string;
}

const isTime = (s: string | undefined): s is string => !!s && !Number.isNaN(Date.parse(s));

/**
 * The dlq_meta row for one DLQ record, from its dlq.* headers; null if the record does not carry them (not a dead
 * letter written by the sink). tenant/event come from the parsed value when it parses.
 */
export function metaFromRecord(r: ScannedRecord, parsed: { tenantId: string; eventId: string } | null, now: Date = new Date()): MetaRow | null {
  const h = (n: string) => headerValue(r.headers, n);
  const topic = h(DLQ_HEADERS.originalTopic);
  const partition = Number(h(DLQ_HEADERS.originalPartition));
  const offset = h(DLQ_HEADERS.originalOffset);
  const errorClass = h(DLQ_HEADERS.errorClass);
  if (!topic || !Number.isInteger(partition) || !offset || !/^\d+$/.test(offset) || !errorClass) return null;
  const failedAt = h(DLQ_HEADERS.failedAt);
  const lastFailedAt = isTime(failedAt) ? failedAt : r.timestamp ? new Date(r.timestamp).toISOString() : now.toISOString();
  const first = h(DLQ_HEADERS.firstFailedAt);
  const attempts = Number(h(DLQ_HEADERS.attempts));
  return {
    sourceTopic: topic,
    sourcePartition: partition,
    sourceOffset: offset,
    failedTopic: h(DLQ_HEADERS.failedTopic) || topic,
    dlqTopic: r.topic,
    dlqPartition: r.partition,
    dlqOffset: r.offset,
    tenantId: parsed?.tenantId ?? null,
    eventId: parsed?.eventId ?? null,
    errorClass,
    errorCode: h(DLQ_HEADERS.errorCode) ?? null,
    errorMessage: h(DLQ_HEADERS.errorMessage) ?? null,
    attempts: Number.isInteger(attempts) && attempts >= 0 ? attempts : 0,
    firstFailedAt: isTime(first) ? first : lastFailedAt,
    lastFailedAt,
  };
}
