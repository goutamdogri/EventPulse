# EventPulse: PRD and Stage Plan

*Distributed real-time event processing platform. Product requirements, per-stage build plan, tests, and break-it experiments.*

## 1. Purpose

EventPulse ingests events from external applications, streams them through a distributed pipeline, aggregates them for analytics, triggers rule-based actions, and exposes dashboards and query APIs.

The project has two jobs: be a credible production-style system, and be the evidence that its builder can measure, break, and fix distributed systems. Every claim must be backed by a number, a chart, or a test.

## 2. Goals and Non-Goals

**Goals**

- Prove per-node throughput, linear scaling to 4-8 nodes, and failure behavior. Extrapolate to 1M rps with a cost model.
- Define and test explicit delivery guarantees.
- Operate it like production: SLOs, alerts, runbook, postmortem.
- Keep cloud cost low by running test sessions only and destroying infrastructure afterward.

**Non-goals**

- Actually sustaining 1M rps (it is a modeled claim, not a measured one).
- A polished frontend (Stage 9 is deliberately light).
- Adding a technology without a measured problem that demands it.

## 3. Users

- **Producer app:** sends events via API or SDK.
- **Tenant admin:** defines rules, limits, and dashboards.
- **Analyst:** queries aggregates and raw events.
- **Operator (you):** deploys, monitors, and debugs the platform.

## 4. Functional Requirements

| ID | Requirement | Stage |
| --- | --- | --- |
| FR1 | Accept single and batch events over HTTP, authenticate with tenant API key, return 202 quickly | 1-2 |
| FR2 | Validate events against a versioned schema at the edge | 2-3 |
| FR3 | Durably queue events before acknowledging | 2 |
| FR4 | Process each event effectively once (at-least-once delivery plus idempotent processing) | 3 |
| FR5 | Route failed events to a dead-letter queue with retry policy | 3 |
| FR6 | Aggregate events over tumbling and sliding windows, handling late and out-of-order data | 4 |
| FR7 | Store events and aggregates for fast analytical queries | 4 |
| FR8 | Enforce per-tenant rate limits | 5 |
| FR9 | Evaluate tenant rules on the stream and trigger notifications idempotently with retries | 5 |
| FR10 | Isolate tenants so one cannot degrade others | 5-6 |
| FR11 | Query API for aggregates and raw events with tenant scoping | 4-6 |
| FR12 | Live dashboard with throttled real-time updates | 9 |

## 5. Non-Functional Targets

These are hypotheses to validate. Replace each with the measured value as you go.

| Area | Target |
| --- | --- |
| Ingest latency | p99 under 50 ms at rated load |
| Throughput | 50k-150k events/sec on a small cluster; per-node figure measured |
| Durability | No acknowledged event lost under single broker failure |
| Delivery | At-least-once, with idempotent processing; no double counting in tests |
| Analytics freshness | Under 5 s from ingest to queryable aggregate |
| Query latency | Dashboard queries under 200 ms over hundreds of millions of rows |
| Recovery | Consumer or pod failure recovers without manual action |
| Cost | Cost per million events and projected monthly cost at 1M rps documented |

## 6. Target Architecture

```
Producers -> LB (Nginx/Envoy) -> Ingest API (stateless, N nodes)
                                   | validate, auth, rate limit (Redis)
                                   v
                                 Kafka (partitioned by tenant/event key)
                         +---------+-----------+
                         v                     v
                 Stream processor        Rule engine
                 (windows, state)        (Redis cache, idempotent actions)
                         v                     v
                    ClickHouse         Notifications (retries, DLQ)
                         v
                   Query API -> Dashboard (WebSocket/SSE)

Postgres: tenant metadata only. Prometheus + Grafana + OpenTelemetry across all.
```

## 7. Core Interfaces

**Ingest**

```
POST /v1/events
Authorization: Bearer <tenant_api_key>
Idempotency-Key: <uuid>        (optional, recommended)
{ "event_id": "uuid", "type": "order.created",
  "occurred_at": "2026-10-05T10:00:00Z",
  "schema_version": 1, "payload": { ... } }
-> 202 Accepted { "event_id": "..." }

POST /v1/events:batch   (up to N events per request)
```

**Errors:** 400 invalid schema, 401 bad key, 413 payload too large, 429 rate limited (with Retry-After), 503 shedding load.

**Query:** `GET /v1/aggregates?type=...&from=...&to=...&interval=1m` and `GET /v1/events?...` scoped by tenant.

## 8. How Every Stage Is Run

1. Write a hypothesis in the lab notebook before building.
2. Build the smallest version that tests it.
3. Load test and record a baseline.
4. Break it with the listed experiments.
5. Observe with metrics, logs, and profiles, then fix.
6. Re-run the same test and record the delta.
7. Write the artifact. Only then move on.

**Lab notebook entry template:** hypothesis, setup (hardware, versions, config), method, result (numbers and charts), explanation, what changed next.

**Test types used throughout:** unit, integration (docker-compose), contract/schema, load (k6, wrk2), soak (hours), chaos, and correctness (replay, ordering).

**Definition of done for a stage:** all exit criteria met, break-it experiments documented, artifact written, repo runs with one command, numbers recorded.

## 9. Stage Plan

### Stage 0: Foundations (2-3 weeks)

**Objective:** understand how a server behaves under load before building the platform.

**Build:** a minimal HTTP server (Node/Fastify) with a health route and a CPU-bound route. A reusable k6 and wrk2 harness. Metrics for latency percentiles.

**Tests:**

- Load at stepped rates to find the knee of the latency curve.
- Compare wrk2 vs k6 results to see coordinated omission.
- Compare average vs p50/p99/p999.

**Break it:**

- Saturate the CPU with the blocking route and watch the event loop stall.
- Hit the open-file and connection limits; lower and raise `ulimit`.
- Move CPU work to worker threads and re-measure.

**Record:** requests/sec at saturation, latency percentiles, event-loop lag, fd usage.

**Exit criteria:** you can explain, with tool output (`ss`, `strace`, profiler), exactly why the server saturated where it did.

**Why Stage 1:** you now have a measurement method. Next you need a realistic workload to measure.

### Stage 1: Naive Baseline (2 weeks)

**Objective:** build the simplest correct system and record how it fails. This is the permanent "before."

**Build:** NestJS (Fastify adapter) `POST /events` that validates and inserts into Postgres synchronously. API key auth. Basic schema and migrations.

**Tests:**

- Unit tests for validation.
- Integration test: event in, row in Postgres.
- Ramp load test to collapse.

**Break it:**

- Exhaust the connection pool and observe queueing.
- Create lock contention with hot rows or indexes.
- Slow Postgres artificially (disk throttle or `pg_sleep`) and watch request latency.
- Run a long soak to see table and index growth effects.

**Record:** max sustainable rps, p99 vs load curve, pool wait time, DB CPU and IO, a flame graph of the API process.

**Exit criteria:** a baseline report where every collapse is traced to a specific cause.

**Why Stage 2:** the DB is in the request path, so API latency equals DB latency and one slow dependency stops ingestion. You need decoupling.

### Stage 2: Decouple with a Queue (3 weeks)

**Objective:** make ingest fast and independent of storage speed.

**Build:** API validates and produces to Kafka (3 brokers locally, or KRaft), returns 202. A consumer group writes batches to Postgres. Metrics for producer latency, consumer lag, and batch sizes.

**Tests:**

- Rerun the Stage 1 load test and compare.
- Producer config sweep: `linger.ms`, `batch.size`, `acks`, compression.
- Partition count vs consumer count experiment.
- Integration test: events survive an API restart.

**Break it:**

- Kill a broker mid-load and watch leader election and producer errors.
- Kill a consumer mid-batch and check duplicates and gaps.
- Send a poison message and watch the consumer loop.
- Slow Postgres and watch consumer lag grow while ingest stays fast.
- Trigger rebalances repeatedly.

**Record:** ingest rps and p99 before/after, lag over time, duplicate count after failures, effect of each producer setting.

**Exit criteria:** at least an order-of-magnitude ingest improvement explained by mechanism, and a written list of every failure that caused duplicates or loss.

**Why Stage 3:** your experiments produced duplicates and possibly loss. Fast is not correct yet.

### Stage 3: Correctness (3-4 weeks)

**Objective:** define and prove delivery guarantees.

**Build:**

- Idempotency keys and a dedup layer (Redis with TTL, or Postgres unique constraint). Compare both.
- Transactional outbox for any write that must also publish.
- Dead-letter queue and retry policy (backoff, max attempts, poison handling).
- Avro or Protobuf with a schema registry, with compatibility rules.

**Tests:**

- Replay the same events 3 times; counts must not change.
- Out-of-order and late events; results must still be right.
- Crash between side effect and offset commit; verify no double effect.
- Schema evolution: add a field, remove a field, check compatibility enforcement.
- Property-based test of the idempotent consumer.

**Break it:**

- Kill consumers at every step of the processing path (before write, after write, before commit).
- Expire dedup keys early and show the failure window.
- Break the Redis dedup store and observe fallback behavior.
- Deploy an incompatible schema.

**Record:** duplicate rate before/after, dedup cost in latency and memory, DLQ volume, test matrix of failure points vs outcomes.

**Exit criteria:** a "delivery guarantees" document stating exactly what is guaranteed and what is not, with a test for each claim.

**Why Stage 4:** correct per-event storage works, but aggregation in Postgres is slow and does not handle time windows.

### Stage 4: Stream Processing and Analytics (4 weeks)

**Objective:** compute real-time aggregates over time windows and serve fast analytical queries.

**Build:**

- Your own windowing consumer first (tumbling, sliding, watermarks, late-event policy, checkpointing).
- Sink to ClickHouse: batch inserts, MergeTree with chosen partition and order keys, materialized views.
- Optional comparison implementation in Flink or Kafka Streams.
- Query API for aggregates.

**Tests:**

- Window correctness against a simple reference computed offline.
- Late events inside and outside the allowed lateness.
- Query benchmarks on 10M, 100M, and hundreds of millions of rows.
- Same query on Postgres vs ClickHouse.

**Break it:**

- Crash the processor mid-window and verify state recovery from checkpoint.
- Cardinality explosion (millions of unique keys) in ClickHouse.
- Insert many tiny batches and watch part merging suffer.
- Skewed data on one key.
- Bad ORDER BY choice, then fix it.

**Record:** processing lag, checkpoint size and time, ClickHouse insert rate, query latency vs row count, storage per million events.

**Exit criteria:** dashboard queries answer in milliseconds over a large dataset, with a written explanation of why ClickHouse and not Postgres, and a decision on custom vs Flink backed by numbers.

**Why Stage 5:** the pipeline works for one well-behaved producer. Real tenants misbehave and need rules and protection.

### Stage 5: Rate Limiting, Caching, and Rule Engine (3 weeks)

**Objective:** protect the platform from noisy tenants and add rule-based actions.

**Build:**

- Distributed rate limiter in Redis with Lua: token bucket per tenant. Also fixed window and sliding window for comparison.
- Rule engine on the stream: conditions, thresholds, notification actions with retries and dedup, idempotent executor.
- Cache-aside for rule and tenant config with TTL strategy and stampede protection.

**Tests:**

- Limiter accuracy under concurrency across multiple API nodes.
- Algorithm comparison: accuracy, burst behavior, Redis cost.
- Rule fires exactly once per condition even with replay.
- Cache hit ratio and expiry behavior.

**Break it:**

- One hot tenant floods; verify others keep their latency.
- Redis failover and total loss: decide fail-open or fail-closed and test it.
- Thundering herd on mass cache expiry.
- Notification provider down; verify retries and DLQ.
- Rule storms producing thousands of alerts.

**Record:** limiter overhead per request, per-tenant latency during the noisy-neighbor test, cache hit ratio, notification success and duplicate rates.

**Exit criteria:** a multi-tenant isolation demo with before/after charts, and documented limiter failure policy.

**Why Stage 6:** shared storage and shared partitions are now the bottleneck, and one hot tenant can still cause hot partitions.

### Stage 6: Scaling the Data Layer (3 weeks)

**Objective:** remove single points of data scaling limits.

**Build:**

- Postgres for metadata: partitioning, a read replica, PgBouncer.
- Your own consistent-hash ring for tenant-to-shard mapping, with virtual nodes.
- Hot-partition mitigation in Kafka (key salting or tenant-aware partitioning).

**Tests:**

- Hash ring distribution evenness and minimal key movement on add/remove.
- Replica lag under write load; read-after-write behavior.
- PgBouncer vs direct connections under load.

**Break it:**

- Add and remove shards under load and measure the rebalance (data moved, latency impact, errors).
- Fail the primary and promote a replica.
- Create a skewed tenant to saturate one partition.
- Run migrations on large tables during traffic.

**Record:** keys moved per topology change, rebalance duration, p99 during rebalance, partition lag skew.

**Exit criteria:** a short write-up on your sharding strategy with a measured rebalance.

**Why Stage 7:** everything runs well on your laptop. You have not yet shown it can run, scale, and be diagnosed on real infrastructure.

### Stage 7: Infrastructure and Operations (4 weeks)

**Objective:** run the system like production and prove you can operate it.

**Build:**

- Kubernetes (k3s or a managed cluster), Helm charts, graceful shutdown, readiness and liveness probes.
- HPA and KEDA scaling on Kafka lag.
- Terraform for reproducible cloud setup and teardown. CI/CD pipeline.
- Nginx or Envoy load balancer tuning.
- Prometheus metrics, Grafana dashboards, OpenTelemetry tracing, structured logs.
- SLOs and alert rules.

**Tests:**

- Rolling deploy under load with zero failed requests.
- Autoscaling reacts to lag within a defined time.
- Alerts fire for injected failures and do not fire for normal load.
- `terraform apply` and `destroy` from scratch works.

**Break it:**

- Kill pods and nodes during load.
- Add latency and packet loss with `tc` or toxiproxy.
- Fill a disk.
- Make a dependency slow rather than dead.
- Deploy a bad version and roll back.

**Record:** time to detect, time to recover, error budget burn, scaling reaction time, cloud cost per test session.

**Exit criteria:** a runbook and a postmortem for an incident you caused, with timeline, root cause, and fixes.

**Why Stage 8:** you can now run it. Time to find out how efficient it really is and where scaling stops.

### Stage 8: Performance Engineering (3-4 weeks)

**Objective:** find real bottlenecks and produce your scaling proof and cost model.

**Build:**

- Profile the ingest path (flame graphs, clinic.js, `perf`).
- Optimize JSON handling, batching, keep-alive, HTTP/2 or gRPC, Kafka producer settings, GC.
- Implement the same ingest endpoint in Go (or Rust) and compare.

**Tests:**

- Same load profile against Node and Go, at equal CPU.
- Scaling runs at 1, 2, 4, and 8 ingest nodes.
- Repeat runs to get variance, not a single number.

**Break it:**

- Push until the next bottleneck appears (broker, network, load generator, ClickHouse) and identify which it is.
- Test with large payloads and many small events.
- Run a multi-hour soak to find leaks and tail latency drift.

**Record:** per-node rps and p99, scaling efficiency per added node, CPU and memory per rps, cost per million events.

**Exit criteria:** a benchmark report with charts, the Node vs Go decision, and a cost model projecting monthly cost at 1M rps.

**Why Stage 9:** the backend story is complete; users still need to see it.

### Stage 9: Frontend and Real-Time (2 weeks)

**Objective:** show the system working without overbuilding UI.

**Build:** React dashboard using WebSocket or SSE with server-side aggregation and throttled pushes. Tenant selection, a time-series chart, and recent alerts.

**Tests:**

- Dashboard stays responsive at the highest event rate.
- Reconnect and resume behavior.
- Many concurrent viewers on one aggregate.

**Break it:**

- Spike the event rate and confirm the browser stays smooth.
- Drop the connection repeatedly.
- Slow client; verify the server applies backpressure rather than buffering unboundedly.

**Record:** messages per second pushed, browser frame rate and memory, server memory per connection.

**Exit criteria:** a live demo that survives a spike and a chaos event.

**Why Stage 10:** you have broad coverage. Depth is what makes the project rare.

### Stage 10: Depth Project (pick one, 4-6 weeks)

**Options:** a Raft-based replicated log (MIT 6.824 style), a log-structured storage engine, or a mini stream-processing windowing engine. Alternatively, a Kafka or ClickHouse internals study backed by experiments.

**Tests:**

- Deterministic simulation or randomized fault testing.
- Linearizability or invariant checks.
- Crash-recovery tests.

**Break it:**

- Network partitions, node crashes, and restarts at random points.
- Message delay, reordering, and loss.
- Disk full and torn writes (storage engine).

**Record:** invariants checked, number of fault schedules passed, bugs found and fixed.

**Exit criteria:** the component, its test suite, and a public write-up.

## 10. Final Deliverables Checklist

- [ ] GitHub repo with architecture README, diagrams, one-command local setup
- [ ] Baseline vs optimized benchmark report with flame graphs
- [ ] Node vs Go comparison
- [ ] Scaling charts for 1, 2, 4, 8 nodes and the 1M rps cost model
- [ ] Delivery guarantees document with tests
- [ ] Chaos and failure report
- [ ] Runbook, SLOs, one postmortem
- [ ] Design docs and ADRs with rejected alternatives
- [ ] Depth project with public write-up
- [ ] 5-8 technical blog posts
- [ ] 5-minute demo video with live dashboards and a chaos event

## 11. Risks and Rules

| Risk | Mitigation |
| --- | --- |
| Scope creep and burnout | Finish stages 1-5 before anything fancy; keep Stage 9 light |
| Cloud bill surprise | Test sessions only, spot instances, billing alerts at $10/$25/$50, hard cap, terraform destroy after each run |
| Unreliable benchmarks | Pin versions and hardware, repeat runs, report variance, keep the load generator off the system under test |
| Resume padding | Never claim a number you did not measure; add a technology only when a measured problem demands it |

## 12. Order of Work

When you are ready, ask for one stage at a time in order, starting with Stage 0. For each stage, the build will include repo structure, code, tests, load scripts, the break-it experiments, and the notebook template.
