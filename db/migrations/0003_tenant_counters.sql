-- Up Migration
-- B4: one counter row per tenant. With COUNTER_MODE=hot every insert also increments its tenant's row.
CREATE TABLE tenant_counters (
  tenant_id text   PRIMARY KEY,
  n         bigint NOT NULL DEFAULT 0
);

-- Down Migration
DROP TABLE tenant_counters;
