-- Up Migration
-- Stage 3 (S3-F6): bookkeeping of DLQ replays (tools/dlq-replay). One row per `replay` run, dry run or executed:
-- who ran it, with which filters, what was selected and what happened. dlq_meta.last_replay_id points here.
CREATE TABLE dlq_replays (
  replay_id   uuid        PRIMARY KEY,
  started_at  timestamptz NOT NULL DEFAULT now(),
  finished_at timestamptz,
  operator    text        NOT NULL,
  filters     jsonb       NOT NULL,
  dry_run     boolean     NOT NULL,
  selected    int         NOT NULL DEFAULT 0,
  replayed    int         NOT NULL DEFAULT 0,
  skipped     int         NOT NULL DEFAULT 0,
  failed      int         NOT NULL DEFAULT 0,
  status      text        NOT NULL CHECK (status IN ('running', 'done', 'dry_run', 'aborted', 'failed'))
);
CREATE INDEX dlq_replays_started_idx ON dlq_replays (started_at DESC);

-- Down Migration
DROP TABLE dlq_replays;
