-- Up Migration
-- Stage 2 (S2-F2): when the sink wrote the row. received_at now comes from the Kafka header (API receipt
-- time), so stored_at - received_at is the end-to-end freshness ("eventual visibility") of every row.
-- now() is STABLE, so Postgres adds the column without rewriting the table; rows that already exist get
-- the time of this migration.
ALTER TABLE events ADD COLUMN stored_at timestamptz NOT NULL DEFAULT now();

-- Down Migration
ALTER TABLE events DROP COLUMN stored_at;
