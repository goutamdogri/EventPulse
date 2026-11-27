-- Up Migration
-- Stage 3 (S3-F2): the sink is idempotent on (tenant_id, event_id), not on event_id alone.
--
-- 1. Primary key (tenant_id, event_id): two tenants may use the same event_id without colliding, and one tenant
--    can no longer probe or block another tenant's ids. event_id was the key before, so no duplicate can exist;
--    check before running on old data anyway:
--      SELECT event_id FROM events GROUP BY event_id HAVING count(*) > 1;   -- must return nothing
--    On a large production table this would be done without a long lock:
--      CREATE UNIQUE INDEX CONCURRENTLY events_tenant_event_uidx ON events (tenant_id, event_id);
--      ALTER TABLE events DROP CONSTRAINT events_pkey,
--                         ADD CONSTRAINT events_pkey PRIMARY KEY USING INDEX events_tenant_event_uidx;
--    For the lab the plain form below is fine.
--
-- 2. content_hash: sha256 of the canonical event (packages/idempotency canonical JSON). The sink compares it when
--    a duplicate key arrives, to tell a true duplicate from a reused id with different content (a producer bug,
--    counted as a conflict, never overwritten). Nullable: rows written before Stage 3 have none.
--
-- 3. sink_offsets: the highest Kafka offset stored per (group, topic, partition), written in the SAME transaction
--    as the rows. Used to tell reprocessed messages (offset already stored once) from redelivered ones (a new
--    offset carrying an existing key), and the working prototype of "offsets in the database" (ADR 0002).
--
-- events_tenant_time_idx (tenant_id, occurred_at) stays. Lookups by event_id alone (audit run ranges, tests)
-- no longer have an index of their own; see ADR 0002 for why none is added.
ALTER TABLE events DROP CONSTRAINT events_pkey;
ALTER TABLE events ADD PRIMARY KEY (tenant_id, event_id);
ALTER TABLE events ADD COLUMN content_hash bytea;

CREATE TABLE sink_offsets (
  consumer_group text        NOT NULL,
  topic          text        NOT NULL,
  partition      int         NOT NULL,
  max_offset     bigint      NOT NULL,
  updated_at     timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer_group, topic, partition)
);

-- Down Migration
-- Fails (on purpose) if two tenants now share an event_id: that data cannot go back to a single-column key.
DROP TABLE sink_offsets;
ALTER TABLE events DROP COLUMN content_hash;
ALTER TABLE events DROP CONSTRAINT events_pkey;
ALTER TABLE events ADD PRIMARY KEY (event_id);
