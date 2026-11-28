# ADR 0007: Transactional outbox with a polling relay (single leader), not CDC, for tenant config changes

- **Status:** Accepted
- **Date:** 2026-11-28
- **Author:** Goutam Dogri
- **Stage:** 3 (Correctness), requirement S3-F4
- **Affects:** `db/migrations/0008_tenant_config_outbox.sql`, `packages/outbox`, `services/admin-api`, `services/outbox-relay`, `schemas/config/v1.avsc`, topic `config.changed`

## Context

A tenant config change must do two things: update `tenant_config` in Postgres, and tell the rest of the system (Stage 4 cache invalidation) through a `config.changed` event on Kafka. Doing both directly is the **dual-write problem**:

- **Write then publish:** a crash between the two loses the event. Caches keep the old config forever.
- **Publish then write:** the change may roll back (412, constraint, crash), and the event announces something that never happened.

No ordering of two independent writes is safe. The event has to commit **with** the change, in the same database transaction, and be published from there.

## Options considered

| | Polling outbox (this ADR) | CDC (Debezium on the WAL) | Kafka transactions / 2PC |
| --- | --- | --- | --- |
| Atomic with the change | yes (same transaction) | yes (reads the committed WAL) | no: Postgres cannot join a Kafka transaction |
| Extra infrastructure | none (one small process) | Kafka Connect, Debezium, a replication slot | none, but it does not solve the problem |
| Load on Postgres | a poll of a partial index every `RELAY_POLL_MS`; LISTEN/NOTIFY makes it mostly idle | WAL decoding; a stuck slot retains WAL and can fill the disk | n/a |
| Latency | measured: p50 7.4 ms, p99 16.7 ms with LISTEN; p50 119 ms, p99 225 ms polling only (200 ms) | sub-second, no polling | n/a |
| Ordering | id order with ONE active relay; several relays can reorder (measured below) | commit order, per table | n/a |
| Delivery | at least once (publish, then mark) | at least once (offsets in Connect) | n/a |
| Operational risk | a stuck relay: visible as the oldest unpublished age | slot lag, connector restarts, schema of the WAL events | n/a |

## Decision

- **Outbox table** (`outbox`) written by `packages/outbox` `enqueue()` with the **caller's transaction client**. `enqueue` refuses to run unless `txid_current_if_assigned()` is not null: in autocommit, or before the transaction has written the change it announces, it throws. The dual-write bug cannot come back by accident.
- **admin-api** (a separate service, so `ingest-api` keeps zero dependency on Postgres) does the following in **one** transaction: lock the tenant row (`FOR UPDATE OF tenants`), check `If-Match`, upsert `tenant_config` with `version + 1`, and enqueue a `config.changed` row. The event carries the **full config snapshot**, a new `event_id` and the version. A no-op PUT still bumps the version and publishes: simpler than comparing documents, and harmless.
- **Polling relay**, `packages/outbox` `PollingRelay` in `services/outbox-relay`:
  1. **Claim:** in its own committed transaction, `attempts = attempts + 1` on the oldest `RELAY_BATCH_SIZE` unpublished rows (`FOR UPDATE SKIP LOCKED`).
  2. **Lock:** in the publish transaction, re-select the claimed rows that are still unpublished, with `FOR UPDATE SKIP LOCKED`.
  3. **Produce** with the idempotent producer, `acks=all`, adding an `outbox_id` header.
  4. **Mark** `published_at`, then `COMMIT`.

  Publish-then-mark is **at-least-once**. `attempts` is committed *before* producing, so a crash leaves `attempts > 1` as durable evidence. That is where `ep_outbox_republished_total` comes from: an upper bound on duplicates, because a crash before producing also leaves `attempts = 2`.
- **Single active relay** (`RELAY_MODE=single`, the default): the relay holding `pg_try_advisory_lock(RELAY_LOCK_KEY)` on a dedicated session publishes. Others stand by and take over when that session dies. `RELAY_MODE=multi` (no lock) exists only for experiment C8.
- **LISTEN/NOTIFY:** `enqueue` calls `pg_notify('outbox_new')`, which is delivered at commit. The relay wakes up on it, and a notification that arrives while a cycle is running is kept for the next sleep. Polling stays as the fallback.
- **Kafka down:** the produce fails and the publish transaction rolls back. Rows stay unpublished, and the relay backs off with jitter (`packages/retry`). Nothing is dropped.
- **Retention:** published rows older than `RELAY_RETENTION_MS` (7 days) are deleted in batches of `RELAY_RETENTION_BATCH`, so the table stays bounded.
- **Consumers** use `applyIfNewer` (`packages/outbox`): apply only if `event.version > current.version`, and dedup on `event_id`. Duplicates and reordering cannot leave a consumer on an old config.
- **Topic** `config.changed`: 3 partitions, RF 3, `min.insync.replicas=2`, `cleanup.policy=compact`. The key is the tenant, and every message is a full snapshot, so compaction keeps the latest config of every tenant.

## Consequences

- **Ordering in single mode:** a tenant's changes are published in version order, and a failover republishes at most the last in-flight batch. Proven: 1,000 changes, leader SIGKILLed after about a third, **0 order violations**, 5 republished messages, and the standby published the rest (`docs/generated/outbox-measurements.json`, `single_leader_failover`).
- **Ordering in multi mode is not guaranteed:** 1,000 changes, two relays with batches of 5, gave **45 order violations**. Each relay published 500. The consumer still ended on version 1,000 and skipped 45 stale events (`multi_mode_ordering`). The version field is what protects consumers. The single leader is what keeps the topic itself ordered.
- **Kafka down is a delay, not a loss.** In a 30 s outage, 715 changes kept committing. The backlog peaked at 662 rows, the oldest unpublished age reached 30 s, and 10 cycles failed and were rolled back. The backlog was drained by the time the brokers reported healthy, with 0 lost, 0 republished and 0 order violations (`kafka_down_30s`).
- **Throughput** is bounded by one relay. That is fine for config changes (rare); it would not be for high-volume events.
- **The relay is a new thing that can be stuck.** It is visible: `ep_outbox_oldest_unpublished_age_seconds`, `ep_outbox_unpublished`, `ep_outbox_relay_is_leader`, and audit check L. See `docs/runbooks/outbox-stuck.md`.
- **Duplicates on `config.changed` are expected** after relay crashes and Kafka errors. Every consumer must dedup by `event_id` or apply by version. This is the documented contract of the topic.

## Proof

- `tests/correctness/test/outbox.e2e-spec.ts`:
  - **PUT:** version + 1, exactly 1 outbox row, published, and the message decodes with the registered schema.
  - **Rejected requests:** 412 on a stale `If-Match`, 404, 400 and 401, and none of them enqueues anything.
  - **Rollback** (injected error, and a crash at `admin:after-update-before-commit`): no config change, no outbox row, nothing published.
  - **Relay crash points:** `before-publish` gives 0 duplicates; `after-publish-before-mark` and `after-mark-before-commit` republish the batch with `attempts = 2`, and the consumer applies each change once.
  - **Two relays:** single-leader failover and multi mode, as described under Consequences.
  - **Retention, graceful lock release, publish latency.**
- `tests/correctness/test/outbox-kafka-down.e2e-spec.ts`: Kafka down for 30 s while changes keep committing. The backlog and the oldest age grow, nothing is lost, and everything is published in order after recovery (`kafka_down_30s`).
- Crash matrix rows `relay:*` and `admin:*` in `docs/generated/crash-matrix.md`.

## Rejected alternatives

- **Dual write (DB, then Kafka):** loses events on a crash. This is the problem itself.
- **CDC with Debezium now:** the right answer at scale (no polling, commit order, no relay to run). It needs Kafka Connect and a replication slot to operate, and a stuck slot can fill the Postgres disk. For a few config changes a minute, a polling relay plus LISTEN is simpler and fast enough. It is the planned upgrade if outbox volume grows; the table layout stays the same (Debezium's outbox event router reads this shape).
- **Several active relays with SKIP LOCKED:** more throughput, but measured reordering (45 in 1,000). Kept only as `RELAY_MODE=multi` for C8.
- **Counting republishes at the consumer only (one transaction for claim and publish):** simpler, but a crash then leaves no trace in the database. Committing `attempts` before producing makes republishing measurable at the source.
