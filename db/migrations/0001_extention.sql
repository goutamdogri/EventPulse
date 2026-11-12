-- Up Migration
CREATE EXTENSION IF NOT EXISTS pg_stat_statements;

-- Down Migration
DROP EXTENSION IF EXISTS pg_stat_statements;