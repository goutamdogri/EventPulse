# Runbook: the outbox is stuck (S3-F4)

**When:**

- `ep_outbox_oldest_unpublished_age_seconds` keeps rising. It is the *Oldest unpublished age* panel in Grafana *Stage 3 Correctness*, row *Transactional outbox*.
- `make audit` reports check L as a finding.
- Someone says "I changed the tenant config but nothing picked it up".

**What it means:** config changes are committed in Postgres (`tenant_config` and its `outbox` row), but `config.changed` has not received them. Nothing is lost. The rows wait in `outbox` until a relay publishes them. What suffers is freshness: consumers keep the old config until the relay catches up.

## 1. Look

```bash
make outbox                                   # leader?, backlog, published/republished, failed cycles, last error
curl -s localhost:8021/metrics | grep ep_outbox_
```

| Signal | Meaning |
| --- | --- |
| `ep_outbox_relay_is_leader` = 0 on every relay | Nobody publishes. No relay is running, or none holds the lock (step 2). |
| `ep_outbox_relay_cycle_failures_total` rising, `last_error` set | The leader runs but every cycle fails: usually Kafka (step 3), sometimes Postgres. |
| Leader = 1, no failures, age still rising | Rows the relay does not select: check `RELAY_TOPICS` (step 5). |
| `ep_outbox_unpublished` high but the age is low and falling | A burst being drained. Not stuck. |

## 2. Is a relay running, and who holds the lock?

```sql
-- the relay's advisory lock (RELAY_LOCK_KEY, default 4281004) and the session holding it
SELECT l.pid, a.application_name, a.client_addr, a.state, a.backend_start, a.state_change
FROM pg_locks l JOIN pg_stat_activity a USING (pid)
WHERE l.locktype = 'advisory' AND l.objid = 4281004;
```

- **No row:** no relay holds the lock. Start one (`make relay-start`, or `make up`). A standby takes it within `RELAY_STANDBY_POLL_MS`.
- **A row, but that relay is not publishing** (hung process, frozen VM): its session still holds the lock and the standbys wait. Restart that relay. If it is unreachable, end its session: `SELECT pg_terminate_backend(<pid>);`. The lock is released and a standby takes over. This is safe: anything it had produced but not marked is published again, which is a duplicate that consumers absorb.

## 3. Is Kafka accepting writes?

```bash
scripts/lag.sh                                # brokers answer?
docker ps --filter name=ep-kafka              # all three healthy?
```

The relay needs `acks=all` with `min.insync.replicas=2`, so it needs at least 2 of the 3 brokers. While Kafka is down the relay backs off (up to `RELAY_BACKOFF_MAX_MS`) and retries forever. Once the brokers are back, it drains without a restart. The 30 s outage test drained the whole backlog right after the brokers were healthy (`docs/generated/outbox-measurements.json`, `kafka_down_30s`).

## 4. Rows with many attempts

```sql
SELECT id, aggregate_id, topic, attempts, created_at, now() - created_at AS age
FROM outbox WHERE published_at IS NULL ORDER BY attempts DESC, id LIMIT 20;
```

`attempts` grows by one per failed cycle. A high value on every row means the dependency is down (step 3). A high value on **one** row while later rows get published cannot happen in single mode (rows go in id order). If you see it, the batch containing that row fails every time. Read `last_error`: the usual cause is a topic that does not exist (`allowAutoTopicCreation` is off) or a message over the size limit.

## 5. Topics the relay does not select

`RELAY_TOPICS` limits a relay to some topics (tests use it). A dev relay started with a test's `RELAY_TOPICS` ignores `config.changed`:

```sql
SELECT topic, count(*), min(created_at) FROM outbox WHERE published_at IS NULL GROUP BY topic;
```

## 6. After the fix

- `ep_outbox_oldest_unpublished_age_seconds` drops back to about 0, and `ep_outbox_unpublished` to about 0.
- `ep_outbox_republished_total` may go up: rows published again after a crash or a timeout. That is expected. Consumers dedup by `event_id` and apply by version (`applyIfNewer`), so a duplicate or an old event never overwrites a newer config.
- `make audit`: check L is ok.

## Don'ts

- **Don't set `published_at` by hand** to "unstick" rows. That event is then never published, and caches keep the old config. Fix the relay instead.
- **Don't run several relays with `RELAY_MODE=multi`** to go faster. A tenant's changes can then be published out of order (measured: 45 of 1,000). Consumers survive it thanks to the version, but the topic order is lost. Multi mode is for experiment C8 only.
- **Don't delete unpublished rows.** Retention deletes only rows published more than `RELAY_RETENTION_MS` ago.
