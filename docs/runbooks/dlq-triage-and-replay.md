# Runbook: DLQ triage and replay (S3-F6)

**When:** `ep_dlq_size` is above 0 (Grafana *Stage 3 Correctness*, row *Retry tiers and DLQ*), or someone asks
"where did my events go?". **Tool:** `tools/dlq-replay` (`pnpm dlq-replay <cmd>` or `make dlq-replay ARGS="..."`).

Dead letters live in two places:

- **`events.dlq` (Kafka)** is the quarantine. It holds the original key, bytes and headers, plus `dlq.*` headers
  (where it came from, why it failed).
- **`dlq_meta` (Postgres)** is the index on top of it. There is one row per source record, which you can search by
  class, tenant and time, and it points at the exact DLQ record (`dlq_partition`, `dlq_offset`).

## 1. Triage: what is in the DLQ?

```bash
pnpm dlq-replay stats                       # unreplayed dead letters by class, class+code, tenant, failed topic, age
pnpm dlq-replay stats --since 2h            # only the recent ones
```

Read the class first. It tells you who has to fix something:

| Class | Typical cause | Fix before replaying |
| --- | --- | --- |
| `PERMANENT_PARSE` | bytes that are not JSON/Avro, missing fields | Usually a producer bug. Fix the producer. These records often stay poison (see step 4). |
| `PERMANENT_SCHEMA` | writer schema id unknown to the registry | Register the schema (`make schemas`, see `schema-change.md`), then replay. |
| `PERMANENT_DB` | a row Postgres rejects (22xxx/23xxx) | Fix the data rule or the code that wrote the bad value, then replay. |
| `TRANSIENT_RECORD` / `UNKNOWN` | failed on every retry tier (3 attempts) | Find out why it kept failing (locks, timeouts on one key) and fix that, then replay. |

`TRANSIENT_SYSTEMIC` / `SYSTEMIC_CONFIG` never reach the DLQ. An outage pauses the partition instead.

## 2. Look at a few

```bash
pnpm dlq-replay list --error-class PERMANENT_DB --limit 20
pnpm dlq-replay show 1234                   # metadata, headers, decoded value (Avro/JSON, hex if unparseable), pre-check
```

The `precheck` field of `show` runs the sink's own parser on the bytes, so it tells you now whether the record would
pass that step again.

## 3. Fix the cause

Ship the code fix, register the schema, or correct the data. **Do not replay before the fix is live.** Otherwise the
records just come back with `replay.count` one higher, and the loop guard (`DLQ_MAX_REPLAYS`, default 3) eventually
stops them for good.

## 4. Dry run (the default)

```bash
pnpm dlq-replay replay --error-class PERMANENT_DB
```

This prints the plan and changes nothing: no record is produced and `dlq_meta` stays the same. The plan shows:

- how many records will be replayed
- how many **would fail again**: the pre-check runs the sink's parse/deserialize step. Records that fail it are
  skipped unless you pass `--include-failing`.
- how many are over the **loop guard** (replayed `DLQ_MAX_REPLAYS` times already)
- how many are **missing** from the DLQ topic (retention). `reindex` cannot bring these back either.
- a breakdown by class and tenant, 5 samples, the target topic and the rate.

The dry run is recorded in `dlq_replays` with `dry_run = true`.

Narrow the selection with filters: `--tenant`, `--event-id`, `--error-class`, `--error-code`, `--failed-topic`,
`--source-partition`, `--source-offset-from/--to`, `--since/--until` (ISO time or an age such as `2h`), `--id 1,2,3`,
`--limit`. Add `--include-replayed` to also select rows that were already replayed.

## 5. Execute

```bash
pnpm dlq-replay replay --error-class PERMANENT_DB --execute            # asks for confirmation
pnpm dlq-replay replay --error-class PERMANENT_DB --execute --yes      # scripts
pnpm dlq-replay replay --tenant acme --execute --rate 20               # gentler on a busy pipeline (default 100/s)
```

For each selected record, in order:

1. Read the exact DLQ record at (`dlq_partition`, `dlq_offset`).
2. Produce it to its `source_topic` (`events.raw`) with the **original key, value and headers**. The `dlq.*` and
   `retry.*` headers are dropped, and `replay.id`, `replay.count`, `replay.dlq_partition` and `replay.dlq_offset` are
   added. The producer is idempotent and uses `acks=all`.
3. **Only after the ack**, mark the row: `replayed_at`, `replay_count + 1`, `last_replay_id`.

The run is recorded in `dlq_replays` with its counts and status. The last executed run is also written to
`infra/compose/textfile/dlq_replay.prom`, which feeds the dashboard row *DLQ replay*. Exit code: 0 ok, 1 something
failed, 2 aborted or bad arguments.

## 6. Verify

```bash
make audit                                  # reconciliation, including the DLQ terms
pnpm dlq-replay stats                       # the replayed class should be gone
```

On the dashboard:

- `ep_sink_duplicates_total` rises if you replayed records that were in fact stored already. That is expected and
  harmless.
- *Replay success rate* shows replayed / selected for the last run.

If records came back to the DLQ, `stats` shows them again as new rows, and their DLQ records carry `replay.count`.

## Why replay is safe

- **Replaying a stored event changes nothing.** The sink dedups on `(tenant_id, event_id)`, so replaying a record
  that was stored meanwhile, or replaying twice (`--include-replayed`), only adds a counted sink duplicate.
- **A crash mid-replay loses nothing.** Some rows are marked and some are not. Run the same command again and it
  replays the unmarked rest. A record that was produced but not marked yet (crash point
  `replay:after-produce-before-mark`) is produced a second time and is absorbed as a sink duplicate. Proven by
  `tests/correctness/test/dlq-replay.e2e-spec.ts` and the crash matrix.
- **A replay does not refill the DLQ.** The pre-check skips records that would fail to parse again. Records that still
  fail (forced, or `PERMANENT_DB`) come back with `replay.count`, and the loop guard stops them after
  `DLQ_MAX_REPLAYS` rounds.
- **The run is bounded.** Rate limit `--rate`, and the run stops at the first Kafka or Postgres error (status
  `failed`) instead of hammering a dead dependency. A crashed run stays `running` in `dlq_replays`, so you can see it.

## If `dlq_meta` is missing rows

The sink publishes to `events.dlq` before it inserts into `dlq_meta`. If the insert failed, for example because the
database went down right after the publish, or if the table was lost, rebuild the index from the topic:

```bash
pnpm dlq-replay reindex                     # or --topic <dlq topic>
```

`reindex` scans the topic without a consumer group and without commits, and inserts the missing rows. Existing rows
are not touched (`ON CONFLICT` on the source record), and a DLQ duplicate never becomes a second row. A rebuilt row
starts unreplayed, because replay state is not in the topic. Replaying it again is harmless, and the `replay.count`
header still drives the loop guard.

## Configuration

| Env | Default | Meaning |
| --- | --- | --- |
| `DLQ_REPLAY_DATABASE_URL` / `DATABASE_URL` | `services/consumer-sink/.env` | Postgres with `dlq_meta` |
| `KAFKA_BROKERS` | `localhost:9092,9094,9096` | |
| `DLQ_TOPIC` | `events.dlq` | topic `reindex` scans |
| `DLQ_MAX_REPLAYS` | 3 | loop guard |
| `DLQ_REPLAY_RATE` | 100 | default `--rate` (records/s, 0 = unlimited) |
| `SCHEMA_REGISTRY_URL`, `SCHEMA_SUBJECT`, `SINK_ACCEPT_JSON` | as the sink | pre-check |
| `DLQ_REPLAY_TEXTFILE` | `infra/compose/textfile/dlq_replay.prom` | `off` = do not write |
| `DLQ_REPLAY_OPERATOR` | `$USER` | `dlq_replays.operator` |
