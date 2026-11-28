// Header names shared by the sink's DLQ writer (S3-F5) and the DLQ replay tool (S3-F6), so both sides agree.
//
//   dlq.*      written by the sink on every dead letter (where it came from, why it failed)
//   replay.*   written by tools/dlq-replay on every re-injected record. They travel with the record: when a replayed
//              record fails again, the sink keeps them on the new dead letter (originalHeaders keeps every header
//              except retry.*), so replay.count survives rounds and the replay loop guard (DLQ_MAX_REPLAYS) works.

export const DLQ_HEADERS = {
  originalTopic: 'dlq.original_topic',
  originalPartition: 'dlq.original_partition',
  originalOffset: 'dlq.original_offset',
  failedTopic: 'dlq.failed_topic',
  consumerGroup: 'dlq.consumer_group',
  errorClass: 'dlq.error_class',
  errorCode: 'dlq.error_code',
  errorMessage: 'dlq.error_message',
  attempts: 'dlq.attempts',
  firstFailedAt: 'dlq.first_failed_at',
  failedAt: 'dlq.failed_at',
} as const;

export const REPLAY_HEADERS = {
  /** dlq_replays.replay_id of the run that re-injected the record. */
  id: 'replay.id',
  /** How many times this event has been re-injected from the DLQ so far, this time included (1, 2, ...). */
  count: 'replay.count',
  /** Where the dead letter it was replayed from sat in the DLQ topic. */
  dlqPartition: 'replay.dlq_partition',
  dlqOffset: 'replay.dlq_offset',
} as const;

/** One header as a string (Kafka gives Buffer, string or an array of them); undefined when absent. */
export function headerValue(h: Record<string, unknown> | undefined, name: string): string | undefined {
  const v = h?.[name];
  const one = Array.isArray(v) ? v[0] : v;
  if (one === undefined || one === null) return undefined;
  return Buffer.isBuffer(one) ? one.toString('utf8') : String(one);
}
