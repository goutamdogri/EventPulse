-- Up Migration
-- Stage 3 (S3-F5): one row per dead-lettered SOURCE record, with what is needed to diagnose and replay it.
--
-- events.dlq (Kafka) is the durable quarantine: it holds the original bytes. dlq_meta is the index on top of it:
-- searchable by tenant, class, time, and it points at the exact DLQ record (dlq_partition, dlq_offset) for replay
-- (tools/dlq-replay, Stage 3 step 6).
--
-- Write order for a dead letter: publish to events.dlq and wait for the ack -> INSERT here ON CONFLICT DO NOTHING ->
-- commit the source offset. A crash in between re-publishes the record (a DLQ duplicate); the UNIQUE key below
-- keeps ONE row per source record and the sink counts the duplicate (ep_dlq_duplicates_total).
-- If this table is ever lost, `tools/dlq-replay reindex` rebuilds it from the topic.
CREATE TABLE dlq_meta (
  id               bigserial   PRIMARY KEY,
  source_topic     text        NOT NULL,  -- where the record was first consumed (events.raw), even if it failed on a retry tier
  source_partition int         NOT NULL,
  source_offset    bigint      NOT NULL,
  failed_topic     text        NOT NULL,  -- where it failed the last time (events.raw or events.retry.N)
  dlq_topic        text        NOT NULL,
  dlq_partition    int         NOT NULL,
  dlq_offset       bigint      NOT NULL,
  tenant_id        text,                  -- null when the record could not be parsed
  event_id         uuid,
  error_class      text        NOT NULL,
  error_code       text,
  error_message    text,
  attempts         int         NOT NULL,  -- retry-tier attempts made before it was dead-lettered
  first_failed_at  timestamptz NOT NULL,
  last_failed_at   timestamptz NOT NULL,
  created_at       timestamptz NOT NULL DEFAULT now(),
  replayed_at      timestamptz,           -- set by the replay tool (step 6)
  replay_count     int         NOT NULL DEFAULT 0,
  last_replay_id   uuid,
  UNIQUE (source_topic, source_partition, source_offset)
);
CREATE INDEX dlq_meta_unreplayed_idx ON dlq_meta (first_failed_at) WHERE replayed_at IS NULL;
CREATE INDEX dlq_meta_class_idx ON dlq_meta (error_class, tenant_id);

-- Down Migration
DROP TABLE dlq_meta;
