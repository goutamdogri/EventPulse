# outbox-relay (Stage 3, S3-F4)

Publishes committed `outbox` rows to Kafka. It is a small `node:http` process around `PollingRelay` from
[`packages/outbox`](../../packages/outbox/src/relay.ts). Design and measurements:
[ADR 0007](../../docs/adr/0007-polling-outbox-vs-cdc.md). When it is stuck:
[runbook](../../docs/runbooks/outbox-stuck.md).

## One cycle

1. **Claim** (its own committed transaction): `attempts + 1` on the oldest `RELAY_BATCH_SIZE` unpublished rows, `FOR UPDATE SKIP LOCKED`.
2. **Lock** the claimed rows in the publish transaction. Crash point `relay:before-publish`.
3. **Produce** with the idempotent producer, `acks=all`, adding an `outbox_id` header, and wait for every ack. Crash point `relay:after-publish-before-mark`.
4. **Mark** `published_at`. Crash point `relay:after-mark-before-commit`. Then `COMMIT`.

The guarantees that follow:

- **At least once.** A crash after the produce republishes the batch. `attempts > 1` stays as evidence, counted by `ep_outbox_republished_total`.
- **Nothing is ever dropped.** If Kafka or Postgres is down, the cycle rolls back and the relay backs off with jitter.

## Single active relay

`RELAY_MODE=single` (the default): the relay holding `pg_try_advisory_lock(RELAY_LOCK_KEY)` publishes. Run a second one with `PORT=8022 make relay-start`: it stands by and takes over when the leader's session dies (SIGKILL, lost connection) or when the leader stops. That is what keeps a tenant's changes in version order on the topic.

`RELAY_MODE=multi` removes the lock, for experiment C8 only. It measured 45 order violations in 1,000 changes.

## Run

```bash
cp .env.example .env            # DATABASE_URL (same database as admin-api)
make relay-start                # :8021; make up starts it when .env exists
make outbox                     # leader, backlog, published / republished, last error
```

Every variable is described in [`src/config.ts`](src/config.ts). The main ones:

| Variable | Default | What |
| --- | --- | --- |
| `RELAY_MODE` | `single` | `multi` = no lock (C8) |
| `RELAY_BATCH_SIZE` | 100 | Rows per cycle |
| `RELAY_POLL_MS` | 200 | Idle poll. `LISTEN outbox_new` wakes it at commit (p50 7.4 ms, p99 16.7 ms) |
| `RELAY_SEND_TIMEOUT_MS` | 10000 | librdkafka `message.timeout.ms` |
| `RELAY_RETENTION_MS` | 7 days | Published rows older than this are deleted |
| `RELAY_TOPICS` | (all) | Only these topics (tests) |
| `OUTBOX_RELAY_ENABLED` | true | `false` = serve health and metrics, publish nothing |

## Endpoints and metrics

Endpoints: `/healthz`, `/readyz` (Postgres and the producer; a standby is ready), `/metrics`, `/debug/stats`, and `/debug/faults` (needs `DEBUG_FAULTS=true`).

| Metric | What |
| --- | --- |
| `ep_outbox_unpublished` | Unpublished rows (partial index count, every `OUTBOX_GAUGE_POLL_MS`) |
| `ep_outbox_oldest_unpublished_age_seconds` | The stuck signal |
| `ep_outbox_published_total{topic}` | Rows published |
| `ep_outbox_republished_total{topic}` | Rows published with attempts > 1: an upper bound on duplicates |
| `ep_outbox_publish_latency_seconds` | `created_at` to `published_at` |
| `ep_outbox_relay_is_leader` | 1 on the active relay |
| `ep_outbox_relay_cycle_failures_total` | Rolled-back cycles |
| `ep_outbox_retention_deleted_total` | Rows removed by retention |

## Tests

- Unit: config parsing.
- End to end (`tests/correctness`):
  - `outbox.e2e-spec.ts`: crash points, two relays (failover and multi mode), retention, lock release, latency.
  - `outbox-kafka-down.e2e-spec.ts`: 30 s outage.
- The crash matrix covers the `relay:*` rows.
