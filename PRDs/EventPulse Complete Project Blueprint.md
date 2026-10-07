# EventPulse: Complete Project Blueprint

*The whole system in one document: what it is, how it works end to end, how it scales, how it fails, how it is measured, and how it becomes interview evidence. Stage-by-stage build guides are separate documents.*

## 1. Vision and Success Definition

EventPulse is a multi-tenant, real-time event processing platform. External applications send events; the platform durably ingests them, streams them through a partitioned pipeline, computes windowed analytics, evaluates tenant rules, and serves queries and live dashboards.

**The project succeeds when you can defend all of these with data:**

1. One ingest node sustains a measured rps at a measured p99, and N nodes scale near linearly up to a measured limit.
2. No acknowledged event is lost under a single broker, consumer, or pod failure, and processing is effectively once (at-least-once delivery plus idempotency).
3. Analytics over hundreds of millions of rows answer in milliseconds.
4. One noisy tenant cannot degrade others.
5. You can run, observe, break, and recover the system, and you wrote down what happened.
6. A cost model projects the monthly bill at 1M rps from measured per-node numbers.

**Honesty rule:** 1M rps is a modeled claim. Everything else is measured.

## 2. Scope

**In scope:** ingestion API, durable queue, stream processing, analytics store, rate limiting, rule engine, notifications, tenant metadata, query API, dashboard, observability, infrastructure as code, chaos and load tooling.

**Out of scope:** billing, user management beyond API keys, a full UI product, multi-region active-active (documented as a future extension only), and technologies added without a measured need.

## 3. System Context and Components

```
 Producers (apps/SDKs)
        |
   [ L4/L7 Load Balancer ]  Nginx / Envoy
        |
   [ Ingest API ]  stateless, N replicas
     - authN (API key -> tenant)
     - schema validation
     - rate limit (Redis token bucket)
     - produce to Kafka, return 202
        |
   [ Kafka cluster ]  topics: events.raw, events.dlq, events.retry, aggregates.out
     |                |                       |
 [ Stream processor ] [ Rule engine ]    [ Raw event sink ]
  windows, state,      evaluates rules,   batch insert to
  checkpoints          idempotent actions  ClickHouse
     |                |                       |
 [ ClickHouse ]  <-----+--> [ Notifier ] -> webhooks/email (retry, DLQ)
     |
 [ Query API ] -> [ Dashboard via WebSocket/SSE ]

 Postgres: tenant, API key, rule, limit metadata (small, read-heavy)
 Redis: rate-limit buckets, dedup keys, config cache
 Prometheus + Grafana + OpenTelemetry across everything
```

### Component catalog

| Component | Responsibility | State | Scales by | On failure |
| --- | --- | --- | --- | --- |
| Ingest API | Auth, validate, rate limit, produce | None | Replicas behind LB | LB removes unhealthy; clients retry with idempotency key |
| Kafka | Durable ordered log per partition | Disk, replicated | Partitions, brokers | Leader election; producers retry; acks policy decides loss window |
| Stream processor | Windowed aggregation | Windows + offsets (checkpointed) | Partitions (one consumer per partition) | Restart from checkpoint; reprocess since last offset |
| Raw event sink | Batch write to ClickHouse | Offsets | Partitions | Replay from last committed offset; dedup at storage |
| Rule engine | Evaluate rules, trigger actions | Rule cache, dedup keys | Partitions | Replay is safe because actions are idempotent |
| Notifier | Deliver webhooks/emails | Retry state | Consumers | Backoff, then DLQ |
| ClickHouse | Analytical storage | Disk (replicated) | Shards, replicas | Replica serves reads; inserts retry |
| Postgres | Metadata | Disk | Replicas, PgBouncer | Failover to replica |
| Redis | Buckets, dedup, cache | Memory | Cluster/shards | Defined fail-open or fail-closed per use |
| Query API | Tenant-scoped reads | None | Replicas | Stateless |

## 3.1 Design Principles

- **Stateless at the edge.** Any ingest or query node can die without data loss.
- **The log is the source of truth.** Everything downstream can be rebuilt by replay.
- **Backpressure over buffering.** When downstream is slow, shed or slow producers explicitly instead of growing unbounded memory.
- **Explicit failure policy.** Every dependency has a documented behavior when it is slow, down, or wrong.
- **Measure before optimizing.** No optimization without a profile.

## 4. Event Lifecycle (End to End)

1. **Send.** Producer calls `POST /v1/events` with API key and an `event_id`.
2. **Authenticate.** API key maps to a tenant (cached; Postgres on miss).
3. **Rate limit.** Token bucket per tenant in Redis. Over limit returns 429 with Retry-After.
4. **Validate.** Schema version check and size limit at the edge. Bad events return 400 and never reach Kafka.
5. **Produce.** Event is written to `events.raw`, keyed for ordering and distribution. Producer batches by `linger.ms` and `batch.size`.
6. **Acknowledge.** Return 202 after Kafka acknowledges per the configured `acks` policy (this is the durability point).
7. **Consume.** Three independent consumer groups read the same topic: raw sink, stream processor, rule engine.
8. **Deduplicate.** Consumers apply idempotency by `event_id` (or tenant plus event\_id) so replays do not double count.
9. **Process.** Aggregates computed in event-time windows with a watermark and lateness policy.
10. **Store.** ClickHouse receives batched raw rows and aggregate rows.
11. **Act.** Rule engine fires actions at most once per (rule, condition window) in effect, via idempotency keys.
12. **Serve.** Query API reads ClickHouse; dashboard receives throttled aggregate pushes.

**Where guarantees apply:** durability starts at step 6; effectively-once effect is guaranteed by steps 8 and 11; ordering is guaranteed only within a partition key.

## 5. Data Model

### Event envelope

```
{ event_id: uuid, tenant_id: string, type: string,
  occurred_at: timestamp (event time), received_at: timestamp (ingest time),
  schema_version: int, payload: object/bytes }
```

- `occurred_at` drives windowing; `received_at` measures lag.
- Payload is kept opaque in transit; typed columns are extracted for analytics.

### Kafka topics

| Topic | Key | Purpose | Notes |
| --- | --- | --- | --- |
| events.raw | tenant\_id (or tenant\_id + shard salt for hot tenants) | Main stream | Partition count sized in the capacity model |
| events.retry | original key | Delayed retries | Backoff tiers |
| events.dlq | original key | Poison and exhausted retries | Inspect and replay tooling |
| aggregates.out | tenant\_id + window | Aggregates for dashboards and rules | Compact if useful |

### ClickHouse tables (design intent)

- `events_raw`: MergeTree, partition by day, order by (tenant\_id, type, occurred\_at). Dedup via event\_id strategy decided in Stage 3/4.
- `aggregates_1m` and `aggregates_1h`: fed by materialized views or processor output.
- Insert in large batches only; tiny inserts are a known failure mode you will provoke on purpose.

### Postgres (metadata only)

`tenants`, `api_keys` (hashed), `rate_limits`, `rules`, `rule_actions`, `webhook_endpoints`. Event data never lives here after Stage 2.

### Redis keys

`bucket:{tenant}` (token bucket), `dedup:{tenant}:{event_id}` (TTL), `cfg:{tenant}` (cache-aside).

## 6. Partitioning and Ordering

- Ordering is required only per entity, not globally. Key by tenant (and optionally entity id) to preserve it where needed.
- **Hot-tenant problem:** keying by tenant alone sends one tenant to one partition. Mitigations: key salting for large tenants, tenant-aware partition assignment, and per-tenant rate limits upstream.
- Partition count is a one-way door (hard to reduce, disruptive to increase for keyed data). Choose from the capacity model with headroom, then validate.
- Consumer parallelism is capped by partition count, so partitions also bound processing scale.

## 7. Correctness Model

| Concern | Approach |
| --- | --- |
| Loss | Replication factor 3, `min.insync.replicas=2`, `acks=all` on the durable path; documented trade-off against latency |
| Duplicates | At-least-once delivery plus idempotent consumers keyed by event\_id |
| Ordering | Per-partition only; windows use event time, so out-of-order is handled by the watermark |
| Late events | Allowed-lateness window, then side output or correction policy |
| Side effects | Idempotency keys on every outbound action; transactional outbox where a DB write must also publish |
| Poison messages | Bounded retries with backoff, then DLQ with replay tool |
| Schema change | Registry with backward/forward compatibility rules enforced in CI |

The project will publish a **delivery guarantees document** stating each of these claims, the test that proves it, and what is explicitly not guaranteed.

## 8. Capacity Model for 1M rps

These are planning estimates to be replaced by measured numbers.

**Assumptions:** 500 bytes per event average, replication factor 3, 7-day Kafka retention, ClickHouse compression 5x to 10x.

| Quantity | Estimate |
| --- | --- |
| Ingress | 1M x 500 B = 500 MB/s |
| Kafka write after replication | about 1.5 GB/s cluster-wide |
| Events per day | 86.4 billion |
| Raw data per day | about 43 TB uncompressed; roughly 4-9 TB in ClickHouse |
| Kafka retention (7 days) | about 300 TB x 3 replicas before compression; compression and shorter retention reduce this sharply |
| Ingest nodes | If Node does 20k rps/node: about 50 nodes; if Go does 60k rps/node: about 17 nodes (hypothetical until measured) |
| Kafka partitions | At roughly 10 MB/s per partition target: at least 50; plan 128-256 for headroom |
| Kafka brokers | At roughly 200 MB/s sustained write per broker: at least 8-10 |

**Method:** measure per-node rps, per-partition throughput, broker write rate, and ClickHouse insert rate in your small cluster, then recompute this table with real values and show the linear-scaling evidence that justifies extrapolation. Name what would break the linearity (network, broker disk, partition skew, ClickHouse merges).

## 9. Cost Model

- **Unit costs:** hourly price of the VM types you used, spot discount, storage per GB-month, egress per GB.
- **Cost per million events** = (node-hours consumed x price) / events processed in that window, measured in each test.
- **1M rps projection** = required nodes per component (from section 8) x price x hours, plus storage and egress, stated as a range with assumptions.
- **Test budget:** run cloud only during test sessions, destroy afterward, billing alerts at $10/$25/$50, hard cap, record every session's cost in the notebook.

## 10. Failure Mode Matrix

| Failure | Expected behavior | Mitigation | Proven by |
| --- | --- | --- | --- |
| Kafka broker dies | Brief producer errors, leader election, no acknowledged loss | RF=3, acks=all, producer retries | Broker kill under load |
| Consumer crashes mid-batch | Reprocessing from last commit, no double effect | Idempotency | Crash-at-every-step tests |
| Poison message | Retries then DLQ, partition keeps moving | Bounded retry, DLQ | Poison injection |
| Postgres slow | Ingest unaffected; metadata reads served from cache | Cache-aside, pooling | Latency injection |
| Redis down | Defined policy: limiter fail-open or fail-closed, dedup fallback | Documented per use | Redis kill and failover |
| ClickHouse slow or merging | Lag grows, ingest unaffected, backpressure engages | Batching, lag alerts | Slow-sink test |
| Hot tenant flood | Others unaffected | Per-tenant limits, salted keys | Noisy-neighbor test |
| Cache expiry stampede | No thundering herd | Jittered TTL, request coalescing | Mass-expiry test |
| Pod/node loss | Reschedule, no manual action | Probes, graceful shutdown, replicas | Chaos tests |
| Bad deploy | Detected, rolled back | Canary/rolling, SLO alerts | Deliberate bad release |
| Network latency or loss | Degraded but correct | Timeouts, retries with jitter, circuit breakers | tc/toxiproxy |
| Disk full | Alert before failure; graceful degradation | Retention, disk alerts | Disk fill test |

## 11. Observability Design

**Golden signals per component:** rate, errors, latency percentiles, saturation.

**Key metrics:**

- Ingest: rps, p50/p99/p999, 4xx/5xx, rate-limited count, produce latency.
- Kafka: bytes in/out, under-replicated partitions, consumer lag per group and partition.
- Processor: processing lag, watermark delay, checkpoint duration, late-event count.
- ClickHouse: insert rate, parts count, merge backlog, query latency.
- Redis and Postgres: latency, connections, memory, replication lag.

**Proposed SLIs and SLOs (to be tuned with data):**

- Availability of ingest 99.9% (successful 202 / valid requests).
- Ingest p99 under 50 ms at rated load.
- End-to-end freshness: 99% of events queryable within 5 s.
- Zero acknowledged loss.

**Alerting rules:** page on SLO burn rate and lag growth, not on raw CPU. Every alert must be fired at least once deliberately during chaos tests, and noisy alerts get removed.

**Tracing:** propagate trace context from ingest through Kafka headers to processors so one event can be followed end to end.

## 12. Security and Multi-Tenancy

- API keys stored hashed, scoped to a tenant, rotatable.
- Every read and write path enforces tenant scoping; tests assert cross-tenant access fails.
- Per-tenant rate limits, quotas, and payload size limits.
- TLS in transit; secrets via environment or secret manager, never in the repo.
- Input validation at the edge; reject oversized and malformed payloads before they reach the log.
- Isolation is both security (no leakage) and performance (no noisy neighbors).

## 13. Technology Decisions and Rejected Alternatives

| Decision | Chosen | Rejected | Revisit when |
| --- | --- | --- | --- |
| Ingest framework | NestJS on Fastify, then a Go comparison | Express (slower), Rust (steeper) | Measurements show CPU-bound JSON/GC limits |
| Log | Kafka | Redpanda, Pulsar, SQS/Kinesis | Operational cost or latency data favors another; benchmark against Redpanda as an experiment |
| Analytics store | ClickHouse | Postgres, Druid, Pinot | Query patterns outgrow its model |
| Stream processing | Own windowing first, then Flink or Kafka Streams comparison | Spark Streaming | Operational burden outweighs learning value |
| Cache and limiter store | Redis | Memcached, in-process | Redis becomes the bottleneck |
| Metadata DB | Postgres | MongoDB | Relational needs vanish |
| Serialization | Protobuf or Avro with registry | Raw JSON on the hot path | JSON cost shown by profiles |
| Orchestration | k3s locally, managed Kubernetes for bursts | Nomad, bare VMs | Cost or complexity data |

Every decision becomes an ADR containing context, options, decision, consequences, and the measurement that justified it.

## 14. Repository and Environments

```
eventpulse/
  services/ ingest-api, processor, rule-engine, notifier, query-api
  libs/ schemas, idempotency, ratelimit, telemetry
  infra/ docker-compose, helm, terraform
  loadtests/ k6, wrk2 scripts, scenarios, result archives
  chaos/ experiment scripts and runbooks
  docs/ adr, delivery-guarantees, runbook, benchmarks, postmortems
  notebook/ one markdown file per experiment
```

| Environment | Purpose |
| --- | --- |
| Local (docker-compose, kind/k3d) | Correctness, failure behavior, profiling |
| Cloud test (1-3 spot VMs) | Real per-node numbers, 1-2-4 node scaling |
| Burst cluster (6-10 nodes, hours) | Peak and chaos experiments, destroyed after |
| Permanent mini demo | One small VM or free tier plus recorded large runs |

One command must bring up the local stack, and Terraform must create and destroy the cloud environment from nothing.

## 15. Overall Testing Strategy

| Layer | What it covers | Tools |
| --- | --- | --- |
| Unit | Validation, limiter logic, window math, hash ring | Jest, property-based tests |
| Integration | API to Kafka to sink with real services | docker-compose, testcontainers |
| Contract | Schema compatibility | Registry checks in CI |
| Correctness | Replay, reorder, late data, crash points | Custom harness and reference calculations |
| Load | Throughput and latency curves | k6, wrk2 |
| Soak | Leaks and drift over hours | Same load, long duration |
| Chaos | Pod, broker, network, disk faults | kubectl, tc, toxiproxy, scripts |
| Performance regression | Alert when a change slows the hot path | Benchmarks in CI |

## 16. Benchmark Methodology

- Pin versions, instance types, kernel and JVM/Node settings; record them with every result.
- Keep the load generator on separate machines and confirm it is not the bottleneck.
- Use open-model load (wrk2 style) to avoid coordinated omission.
- Warm up, then measure; repeat at least 3 runs and report variance.
- Increase load stepwise to find the knee, not just peak.
- Change one variable at a time; log each change with its effect.
- Publish raw data and scripts so results can be reproduced.

## 17. Evidence Map for Interviews

| Claim you can make | Evidence |
| --- | --- |
| I found where a naive design collapses | Stage 1 baseline report and flame graph |
| I know why queues change everything | Before/after throughput with explained mechanism |
| I understand delivery semantics | Delivery guarantees doc plus crash-point test matrix |
| I can make analytics fast | ClickHouse vs Postgres benchmark and window correctness tests |
| I protect shared systems | Noisy-neighbor demo and limiter comparison |
| I can shard and rebalance | Hash ring with measured rebalance |
| I can operate it | Runbook, SLOs, fired alerts, postmortem |
| I can optimize | Profiles, Node vs Go, scaling charts, cost per million events |
| I can go deep | Depth project with fault-injection tests and a public write-up |

## 18. Timeline and Dependencies

Roughly 8-11 months part-time. Dependencies are strictly ordered for stages 0-5 (each stage's failures motivate the next). Stages 6 and 7 can overlap partially. Stage 8 needs 7 (you cannot benchmark scaling without reproducible infrastructure). Stage 9 is light and can slip. Stage 10 should start only after stage 5 is solid, but it is the first thing to protect if time runs short.

**Priority if time is short:** stages 1-5, then 7 and 8, then one depth project. The frontend is the first thing to cut.

## 19. Top Risks

| Risk | Mitigation |
| --- | --- |
| Scope creep | Stage exit criteria are strict; no new technology without a measured need |
| Cloud cost overrun | Scripted teardown, spot instances, hard cap, session cost logs |
| Benchmarks that cannot be trusted | Methodology in section 16, repeated runs, raw data kept |
| Burnout on a long project | Each stage ends with a shippable artifact and a blog post; keep a permanent demo running cheaply |
| Claims you cannot defend | Evidence map in section 17; if a claim has no data, remove it |

## 20. How the Stage Documents Work

Each stage will get its own document, written one per response, in this order: Stage 0 through Stage 10. Every stage document will contain: objective and motivating pain, prerequisites, detailed requirements, design and data flow, step-by-step build plan, configuration and code structure, a complete test plan, a catalog of break-it experiments with hypotheses and expected observations, metrics to capture, a lab notebook template, pitfalls, exit criteria, and the handoff into the next stage.
