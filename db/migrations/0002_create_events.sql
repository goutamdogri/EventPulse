-- Up Migration
CREATE TABLE events (
  event_id       uuid        PRIMARY KEY,
  tenant_id      text        NOT NULL,
  type           text        NOT NULL,
  occurred_at    timestamptz NOT NULL,
  received_at    timestamptz NOT NULL DEFAULT now(),
  schema_version int         NOT NULL DEFAULT 1,
  payload        jsonb       NOT NULL
);

CREATE INDEX events_tenant_time_idx ON events (tenant_id, occurred_at);

-- Down Migration
DROP TABLE events;