-- Up Migration
-- Stage 3 (S3-F4): tenant config + transactional outbox.
--
-- services/admin-api changes tenant_config and inserts the outbox row announcing it IN THE SAME TRANSACTION
-- (packages/outbox enqueue). services/outbox-relay publishes unpublished rows to Kafka (config.changed) and marks them
-- published_at after the ack: a committed change is published at least once, a rolled-back one never.
CREATE TABLE tenants (
  tenant_id  text        PRIMARY KEY,
  name       text        NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE tenant_config (
  tenant_id  text        PRIMARY KEY REFERENCES tenants,
  config     jsonb       NOT NULL,
  -- 1 on the first change, +1 on every change (no-op PUTs included). Consumers apply "if version > mine".
  version    bigint      NOT NULL CHECK (version >= 1),
  updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE outbox (
  id           bigserial   PRIMARY KEY,
  aggregate_id text        NOT NULL,
  topic        text        NOT NULL,
  msg_key      text        NOT NULL,
  payload      bytea       NOT NULL,
  headers      jsonb       NOT NULL DEFAULT '{}',
  created_at   timestamptz NOT NULL DEFAULT now(),
  -- Publish attempts by the relay (incremented in its own committed transaction BEFORE producing), so attempts > 1
  -- is durable evidence that the row may have been published more than once. Not in the PRD table: added so
  -- republishing is measurable (ep_outbox_republished_total).
  attempts     int         NOT NULL DEFAULT 0,
  published_at timestamptz
);
-- The relay's poll and the ep_outbox_unpublished gauge only touch unpublished rows: a small partial index.
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
-- Retention deletes published rows older than RELAY_RETENTION.
CREATE INDEX outbox_published_at_idx ON outbox (published_at) WHERE published_at IS NOT NULL;

-- Down Migration
DROP TABLE outbox;
DROP TABLE tenant_config;
DROP TABLE tenants;
