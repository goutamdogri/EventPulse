# EventPulse Stage 6: Scaling the Data Layer

*Duration: about 3 weeks. Goal: remove single-node assumptions from the data layer, implement consistent hashing yourself, handle hot partitions, and measure what it actually costs to add and remove capacity under load.*

## 1. Objective and Motivating Pain

Stage 5 protected the platform and added actions, but the data layer still assumes one of everything: one Postgres, one Redis, a tenant lookup path that goes through a single database, and Kafka keys that send a large tenant to one partition. Doubling API nodes would expose these immediately.

**An honest framing:** EventPulse's metadata is small, and the heavy event data already lives in Kafka and ClickHouse. You are not sharding Postgres because you must today; you are learning the mechanisms, measuring their costs, and being able to say precisely when each is and is not justified. Over-engineering with no measured need is itself a failure mode your rules warn against, so every mechanism here must be tied to an experiment that shows the problem it solves.

By the end of this stage you must be able to:

1. Explain and demonstrate Postgres partitioning, a read replica with replication lag, and PgBouncer pooling, including what each does not solve.
2. Implement a consistent-hash ring with virtual nodes, prove its properties with tests, and show it moves far fewer keys than modulo hashing.
3. Use the ring for real routing decisions (Redis shard selection and tenant-to-shard mapping) and migrate a tenant between shards under load with zero lost writes.
4. Diagnose and mitigate hot Kafka partitions, including the correctness consequences for the Stage 4 processor.
5. Present a measured rebalance: how much data moved, how long it took, and what users saw.

**Predictions to write down first:** when you add a fourth shard to three, what fraction of tenants move with modulo hashing vs your ring? What happens to rate-limit state and caches when Redis nodes are added? What does salting a hot tenant's key do to your Stage 4 aggregates?

## 2. Prerequisites and Setup

| Item | Requirement |
| --- | --- |
| Stages 0-5 | Complete: outbox, `config.changed` invalidation, processor, limiter, cache, notifier |
| Postgres | Primary plus one streaming replica in docker-compose; additional small instances to act as shards |
| Pooling | PgBouncer container in front of the primary |
| Redis | 2-4 independent Redis instances for client-side sharding (this is not Redis Cluster; you are building the routing yourself) |
| Kafka | Existing cluster; ability to create topics with different partition counts |
| Tools | `pgbench`, `psql`, `pg_stat_replication`, PgBouncer admin console (`SHOW POOLS`, `SHOW STATS`), toxiproxy, your k6 multi-tenant harness |
| Realism note | Several Postgres and Redis instances on one machine share CPU, memory, and disk. Your measurements show mechanisms and relative behavior (keys moved, state loss, migration correctness), not absolute capacity. Record this caveat in every report and revisit with real nodes in Stage 8 |

## 3. Learning Plan

### Postgres scaling mechanics

- **Declarative partitioning:** range, list, and hash. The partition key must be part of the primary key and unique constraints. Partition pruning skips partitions when queries filter on the key. Too many partitions inflate planning time and memory.
- **What to partition here:** growing time-series metadata tables (outbox, alerts, action log, DLQ metadata). Retention becomes `DROP PARTITION` or `DETACH`, which is far cheaper than a large `DELETE` and avoids vacuum pressure.
- **Read replicas:** WAL streaming, replication lag, and the read-your-writes problem. Replicas serve reads that tolerate staleness; flows where a user just changed something must read from the primary or a freshness-aware path.
- **Failover:** promotion of a replica, the old primary returning (split brain risk), and why fencing matters.
- **PgBouncer:** session, transaction, and statement pooling. Transaction pooling gives the best reuse but breaks session state: named prepared statements (depending on version and client), session variables, advisory locks, and `LISTEN/NOTIFY`. Sizing: Postgres performs best with a modest number of active connections, so a pooler multiplexes many clients onto few server connections.

### Sharding concepts

- **Why shard:** write throughput, dataset size, and fault or performance isolation. **Cost:** cross-shard queries, no cross-shard transactions, operational complexity, and rebalancing.
- **Shard key:** `tenant_id` fits EventPulse because nearly every access is tenant-scoped.
- **Mapping strategies:** range, hash modulo N (adding a node remaps almost every key), lookup directory (flexible, needs its own storage), consistent hashing (adding a node moves about 1/(N+1) of keys), and rendezvous hashing as an alternative.
- **Virtual nodes:** each physical node owns many points on the ring, improving balance and allowing weights for unequal capacity.
- **Hot tenants and skew:** real tenant sizes are heavy-tailed. Large tenants can be pinned by a directory override.
- **Resharding and migration:** online migration needs states (copy, dual write, verify, cutover, cleanup), a versioned shard map with an epoch, and fencing so a stale router cannot write to the old location after cutover.
- **Fan-out queries:** a query across all shards has latency equal to the slowest shard, so tail latency amplifies as shard count grows.

### Kafka hot partitions

- **Skew:** key by tenant and one huge tenant owns one partition, capping throughput and building lag there.
- **Mitigations:** key salting (spread a hot tenant over K buckets; ordering is lost across buckets), a tenant-aware partitioner using a directory, dedicated topics for giant tenants, and adding partitions (which remaps keys and breaks per-key ordering during the transition).
- **Consequence for stream processing:** if one (tenant, type, window) is computed on several partitions, each produces a *partial* aggregate that must be merged downstream.

### Reading

DDIA Chapter 6 (partitioning: key range vs hash, skew, rebalancing, request routing) and Chapter 5 (replication lag and read-your-writes). The consistent hashing paper (Karger et al.) or the partitioning section of the Dynamo paper. PostgreSQL documentation on partitioning and on high availability and replication, and the PgBouncer documentation. Kafka documentation on partitioning. In `notebook/sharding-notes.md`, explain with a worked example why modulo hashing moves nearly everything when N changes.

## 4. Requirements

| ID | Requirement |
| --- | --- |
| S6-F1 | Growing metadata tables are range-partitioned by time with automated creation of upcoming partitions and retention by dropping old ones |
| S6-F2 | A read replica serves tolerant reads (config lookups, dashboards of metadata); admin flows that read their own writes use the primary; replication lag is a first-class metric |
| S6-F3 | PgBouncer in transaction mode fronts Postgres; all services connect through it; compatibility issues are found and fixed |
| S6-F4 | A reusable consistent-hash ring library: virtual nodes, weights, add and remove, deterministic, versioned membership, with property tests |
| S6-F5 | The ring selects the Redis instance for limiter buckets and cache keys, and a shard directory maps `tenant_id` to a Postgres shard with optional pinned overrides |
| S6-F6 | A tenant migration tool moves a tenant between shards online using an explicit state machine, with verification and fencing |
| S6-F7 | Hot-partition mitigation by salting for tenants above a configured threshold, with the processor emitting partial aggregates and a merge step that preserves Stage 4 correctness |
| S6-F8 | Skew metrics and alerts per Kafka partition, per shard, and per ring node |
| S6-F9 | A measured rebalance report for adding and removing a shard under load |

### Targets (hypotheses to replace with data)

| Area | Target |
| --- | --- |
| Keys moved when adding a node to N | about 1/(N+1) of keys, within a measured tolerance |
| Ring balance | max node load under 1.2x the average with enough virtual nodes (measure the vnode count needed) |
| Migration | zero lost or duplicated writes, verified by checksums |
| Impact during rebalance | ingest p99 and error rate stay within a stated bound |

## 5. Design

```
API / services
   |-- tenant -> shard: directory override?  else  ring.lookup(tenant)
   |-- metadata reads --> PgBouncer --> primary (writes, read-your-writes)
   |                                  \-> replica (tolerant reads)
   |-- limiter/cache keys --> ring.lookup(key) --> Redis node A | B | C | D
   \-- events --> Kafka key = tenant (or salted tenant:bucket for hot tenants)

Control plane: shard directory (small Postgres) with versioned map and epoch
   map change -> outbox -> config.changed -> routers refresh (Stage 5 mechanism)

Postgres shards: pg-shard-1, pg-shard-2, pg-shard-3 (tenant-scoped metadata)
```

### Hash ring sketch

```ts
class HashRing {
  private points: { h: number; node: string }[] = [];

  add(node: string, weight = 1, vnodes = 128) {
    for (let i = 0; i < vnodes * weight; i++)
      this.points.push({ h: hash32(`${node}#${i}`), node });
    this.points.sort((a, b) => a.h - b.h);
  }

  remove(node: string) {
    this.points = this.points.filter((p) => p.node !== node);
  }

  lookup(key: string): string {
    const h = hash32(key);
    let lo = 0, hi = this.points.length;          // first point with p.h >= h
    while (lo < hi) { const m = (lo + hi) >> 1; this.points[m].h < h ? (lo = m + 1) : (hi = m); }
    return this.points[lo % this.points.length].node;  // wrap around
  }
}
```

Use a good non-cryptographic hash (for example murmur3 or xxHash); a weak hash gives uneven distribution, and you can prove that with your own benchmark. Keep membership versioned (`epoch`) so any node can tell whether its view of the ring is stale.

### Tenant migration state machine

```
PLANNED -> COPYING -> DUAL_WRITE -> VERIFYING -> CUTOVER -> CLEANUP -> DONE
              |           |            |            |
              +-----------+------------+------------+--> ABORT (roll back safely)
```

- **COPYING:** bulk copy a consistent snapshot of the tenant's rows to the target shard while the source remains authoritative.
- **DUAL\_WRITE:** writes go to both; catch up changes made during the copy.
- **VERIFYING:** compare row counts and checksums per table for the tenant.
- **CUTOVER:** flip the directory entry and increment the epoch. The source marks the tenant as moved and **rejects further writes (fencing)**, so a router with a stale map fails loudly instead of writing to the old shard.
- **CLEANUP:** delete the source copy after a safety period.

Every transition is recorded in the directory with a timestamp and operator, and every state has a defined crash recovery behavior.

### Partitioned table example

```sql
CREATE TABLE action_log (
  id         bigserial,
  tenant_id  text        NOT NULL,
  created_at timestamptz NOT NULL,
  alert_id   text, status text, detail jsonb,
  PRIMARY KEY (id, created_at)          -- partition key must be in the PK
) PARTITION BY RANGE (created_at);

CREATE TABLE action_log_2026_10 PARTITION OF action_log
  FOR VALUES FROM ('2026-10-01') TO ('2026-11-01');
```

Build a small maintenance job (or evaluate a partition-management extension) that creates partitions ahead of time and drops or detaches expired ones. Consider what happens if the job is down when a new month starts.

### PgBouncer sketch

```ini
[databases]
eventpulse = host=postgres-primary port=5432 dbname=eventpulse

[pgbouncer]
pool_mode = transaction
max_client_conn = 2000
default_pool_size = 20
```

Check your PgBouncer version's support for protocol-level prepared statements, and adjust your client (for example avoid named prepared statements) where needed. Record every compatibility problem you hit.

### Hot-partition salting and partial aggregates

For tenants on a hot list (flagged by the directory), the producer key becomes `tenant:bucket` where `bucket = hash(event_id) mod K`. The processor then sees one tenant's events on K partitions. It must emit **partial aggregates keyed by (tenant, type, window, partition or bucket)**, and a merge step (a ClickHouse `AggregatingMergeTree` target, or a second-stage sum) combines them. Your Stage 4 golden test (processor output equals SQL over raw data) must still pass with salting on; if it does not, you have found a bug.

### Repo additions

```
libs/hashring/ (ring, hash functions, weights, epoch, benchmarks)
libs/shard-router/ (directory client, cache, fencing checks)
services/shard-migrator/ (state machine, copier, verifier)
services/partition-maint/ (Postgres partition job)
infra/ (replica, pgbouncer, extra postgres, extra redis)
tests/ring/, tests/migration/, tests/skew/
docs/ (sharding-strategy.md, migration-runbook.md, failover-runbook.md, adr/)
```

## 6. Step-by-Step Build Plan

1. **Find what breaks first.** Re-run your Stage 5 load with doubled API nodes and record which dependency saturates, so each mechanism is tied to a measured problem.
2. **PgBouncer.** Introduce it, run the connection-storm experiments, and fix compatibility issues.
3. **Read replica** with replication lag metrics and read routing rules.
4. **Partition the growing tables** and build the maintenance job. Load 100M or more rows into one to see pruning and retention behavior.
5. **Hash ring library** with tests and benchmarks.
6. **Shard Redis** with the ring and handle the state that moves (limiter buckets, cache entries).
7. **Shard directory and tenant-to-Postgres routing** across 2-4 instances.
8. **Migration tool** with the state machine, verification, and fencing.
9. **Kafka hot-partition mitigation** with salting and partial-aggregate merge.
10. **Skew metrics and alerts.**
11. **Run the break-it catalog.**
12. **Write the sharding strategy and rebalance report.**

## 7. Test Plan

| Layer | Tests | Pass condition |
| --- | --- | --- |
| Ring unit | Deterministic lookups, wrap-around, empty ring handling, weights | Spec behavior |
| Ring property | On adding a node, keys move only *to* the new node; on removing, only *from* the removed node; moved fraction near 1/(N+1); balance within the stated tolerance | No counterexample |
| Hash quality | Distribution test across a weak and a strong hash | Weak hash visibly worse |
| Migration | Crash at every state; concurrent writes during COPYING and DUAL\_WRITE; checksum equality at VERIFYING; stale router after CUTOVER | No lost, duplicated, or misrouted writes; stale writes fenced |
| Replica routing | Tolerant read gets stale data under induced lag; admin flow reads its own write | Correct routing per endpoint class |
| Partition | Queries with and without the partition key; retention by drop | Pruning visible in `EXPLAIN`; drop is fast |
| PgBouncer | Transactions, prepared statements, advisory locks, long transactions | Compatible, or incompatibilities documented and avoided |
| Salting | Golden test with a salted hot tenant; ordering test | Aggregates equal unsalted result; ordering loss is documented |
| Fan-out | Scatter-gather query with one slow shard | Behavior under partial failure and timeouts defined |

## 8. Break-It Experiment Catalog

Write the hypothesis first and record results per shard and per node, not only totals.

### S1 Connection storm: direct vs PgBouncer

- **Hypothesis:** with hundreds of direct connections Postgres spends time on context switching and memory; pooling reaches equal or better throughput with far fewer server connections.
- **How:** 50, 200, 1000 client connections with and without PgBouncer; sweep pool size.
- **Observe:** throughput, p99, Postgres CPU and context switches, `SHOW POOLS` waiting clients.
- **Explains:** why pooling and a small active-connection count matter.

### S2 Transaction pooling pitfalls

- **Hypothesis:** session-dependent features misbehave under transaction pooling.
- **How:** use named prepared statements, `SET` session variables, advisory locks, and a long idle-in-transaction connection.
- **Observe:** errors, leaked state, pool starvation.
- **Explains:** which application patterns are incompatible.

### S3 Replica lag and failover

- **Hypothesis:** under heavy writes the replica falls behind; a long query on the replica can conflict with replay; reading your own write from a replica fails; killing the primary requires promotion and a returning old primary can diverge.
- **How:** write burst, long replica query, immediate read after write, then kill and promote (in a controlled drill).
- **Observe:** lag over time, cancelled queries, stale read frequency, time to recover, state of the old primary.
- **Explains:** consistency choices per endpoint and the need for fencing in failover.

### S4 Partition pruning and retention

- **Hypothesis:** queries without the partition key scan all partitions; retention by `DROP PARTITION` takes milliseconds while deleting the same rows takes minutes and creates bloat.
- **How:** 100M-row partitioned vs unpartitioned table; compare queries, `EXPLAIN`, delete vs drop, and the cost of very many tiny partitions.
- **Observe:** latency, planning time, vacuum activity, disk usage.
- **Explains:** when partitioning pays off and what it costs.

### S5 Modulo vs consistent hashing

- **Hypothesis:** adding one node to N moves about N/(N+1) of keys under modulo and about 1/(N+1) under the ring.
- **How:** hash 1M tenant ids to 3 nodes, add a 4th, then remove one, for both schemes.
- **Observe:** fraction moved, which nodes receive them.
- **Explains:** why consistent hashing exists, with your own numbers.

### S6 Virtual node sweep and weights

- **Hypothesis:** balance improves with more virtual nodes but with diminishing returns; lookup time and memory grow; weights give proportionally more keys to bigger nodes.
- **How:** vnodes 1, 16, 64, 256, 1024; record max/avg load and standard deviation; add a double-weight node.
- **Observe:** balance, lookup latency, memory.
- **Explains:** the vnode count you will use and why.

### S7 Redis resharding under load

- **Hypothesis:** keys that move lose their state: limiter buckets reset (allowing a burst) and cache entries go cold (stampede risk on the origin).
- **How:** add and remove a Redis node while running the multi-tenant load; compare strategies: immediate cutover, dual read with fallback to the previous owner, and drain-then-move.
- **Observe:** limiter accepted rate during the change, cache hit ratio, Postgres load, p99, errors.
- **Explains:** that resharding stateful caches has a correctness cost beyond data movement.

### S8 Tenant migration under load

- **Hypothesis:** the state machine moves a tenant with no lost or duplicated writes; without fencing a stale router writes to the old shard after cutover; without dual write, writes made during the copy are lost.
- **How:** migrate a busy tenant with steady writes; crash the migrator at each state; deliberately keep one router on the old epoch; run once with fencing disabled to prove the failure.
- **Observe:** checksum results, rejected stale writes, duration, rows per second, p99 impact on the tenant and its neighbors.
- **Explains:** why each migration step exists.

### S9 Add and remove a shard

- **Hypothesis:** adding shard 4 to 3 moves about a quarter of tenants; removing a shard needs a drain that tolerates failure midway.
- **How:** use the migrator to rebalance all affected tenants in controlled batches, and interrupt it.
- **Observe:** tenants moved, total time, error rate, ability to resume or roll back.
- **Explains:** the measured cost of topology change, which is the core of your report.

### S10 Hot partition in Kafka

- **Hypothesis:** one tenant at 80% of traffic saturates its partition; salting spreads load but loses cross-bucket ordering and requires partial-aggregate merging; increasing partitions mid-stream remaps keys and breaks per-key ordering and processor state locality.
- **How:** test K = 1, 4, 8, 16 buckets; separately add partitions to a live topic.
- **Observe:** per-partition throughput and lag skew, processor correctness (golden test), merge cost, ordering violations.
- **Explains:** the trade-offs, and when a dedicated topic for a giant tenant is better.

### S11 Skewed tenant sizes and pinning

- **Hypothesis:** heavy-tailed tenant sizes make a ring imbalanced in load even when balanced in key count; pinning large tenants through directory overrides fixes it.
- **How:** assign Zipf-distributed load to tenants; compare ring-only and ring plus overrides.
- **Observe:** load per shard, hot shard identification via metrics.
- **Explains:** balancing keys vs balancing load.

### S12 Stale shard map and control-plane failure

- **Hypothesis:** routers with cached maps keep working if the directory is down but cannot learn changes; propagation delay leaves a window where some routers are stale; fencing makes that window safe.
- **How:** stop the directory, change the map, delay invalidations.
- **Observe:** routing errors, time to convergence, behavior of new-tenant creation.
- **Explains:** control plane versus data plane failure modes.

### S13 Fan-out tail latency

- **Hypothesis:** a query across all shards has p99 latency close to the worst shard's p99 and becomes more fragile as shards increase.
- **How:** implement scatter-gather over 1, 2, 3, 4 shards with one slow shard.
- **Observe:** latency percentiles vs shard count, partial-result handling and timeouts.
- **Explains:** why cross-shard queries are avoided and where aggregation moves instead (ClickHouse).

## 9. Metrics and Diagnostics

- **Postgres and pooler:** active connections, context switches, PgBouncer client waiting and server active counts, replication lag (bytes and time), partition counts and sizes, planning time.
- **Ring and routing:** keys per node, moved keys per topology change, lookup latency, routing errors, fenced writes, epoch skew across routers.
- **Migration:** state, rows copied, rows per second, verification mismatches, duration.
- **Redis shards:** ops per second per node, memory, hit ratio after a topology change, limiter reset events.
- **Kafka:** bytes and lag per partition, skew ratio (max over average), partial-aggregate merge lag.
- **Fan-out:** per-shard latency and gather latency.

Useful commands:

```sql
-- replication state (on the primary)
SELECT client_addr, state, sent_lsn, replay_lsn, replay_lag FROM pg_stat_replication;
-- partition pruning check
EXPLAIN (ANALYZE) SELECT * FROM action_log
WHERE created_at >= '2026-10-05' AND created_at < '2026-10-06' AND tenant_id = 't1';
```

## 10. Artifacts

1. **Sharding strategy write-up** (the main artifact): what is sharded and why, shard key, mapping strategy, directory and epoch design, migration state machine, fencing, and a **measured rebalance** (tenants moved, duration, error rate, latency impact).
2. **Hash ring README and benchmark report:** modulo vs ring movement, vnode sweep, hash quality comparison.
3. **Runbooks:** tenant migration and rollback; replica promotion and failover drill; adding a Redis node safely.
4. **Kafka hot-partition ADR:** salting vs dedicated topic vs partition increase, with measurements and the effect on aggregates and ordering.
5. **Postgres notes:** partitioning decisions, retention approach, PgBouncer compatibility list.
6. **Delivery guarantees update:** which guarantees change with routing (for example: ordering holds per bucket for salted tenants; limits may briefly over-allow during Redis resharding by a measured amount).
7. **Optional blog post:** "I implemented consistent hashing and measured the rebalance" or "What happens to rate limits when you add a Redis node."

## 11. Common Pitfalls

- Modulo hashing, or a weak hash function, in the routing path.
- Too few virtual nodes, then wondering about imbalance.
- Moving keys but not the state attached to them (limiter buckets, cache entries, processor state).
- Cutting over without fencing the old location.
- Caching the shard map with no epoch and no invalidation.
- Reading from a replica where read-your-writes is required.
- Partitioning without including the partition key in the primary key, or creating thousands of partitions.
- Forgetting that transaction pooling breaks session state.
- Increasing Kafka partitions on a live keyed topic without considering ordering and processor state.
- Salting hot keys without partial-aggregate merging and then trusting the numbers.
- Sharding data that does not need it and calling it scale; always tie each mechanism to a measured limit.
- Interpreting same-machine results as capacity numbers.

## 12. Exit Criteria

- [ ] PgBouncer, replica, and time-partitioned tables are working with measured effects.
- [ ] Hash ring library passes property tests for balance and minimal movement.
- [ ] Ring used for Redis shard selection and tenant-to-Postgres routing.
- [ ] Tenant migration completes online with verified zero loss, tested at every crash point, with fencing proven necessary.
- [ ] A measured rebalance (add and remove a shard under load) is documented.
- [ ] Hot-partition mitigation implemented, with the Stage 4 golden test still passing.
- [ ] Experiments S1 through S13 have notebook entries.
- [ ] Runbooks and ADRs written; delivery guarantees updated.
- [ ] Repo tagged `stage-6-complete`.

**Self-test questions (answer aloud, without notes):**

1. Why does modulo hashing move almost every key when you add a node, and how does a ring avoid it?
2. What are virtual nodes for, and how did you choose your count?
3. Walk through a tenant migration. What goes wrong if you skip dual write? If you skip fencing?
4. Why can a read replica return stale data, and which of your endpoints tolerate it?
5. What breaks when you put PgBouncer in transaction mode?
6. You salt a hot tenant across 8 partitions. What changes for ordering and for your window aggregates?

## 13. Handoff to Stage 7

Stage 6 leaves components that scale individually and a measured understanding of the cost of change. What remains is operating the whole system as a platform:

- Everything still runs from compose files and scripts; there is no orchestration, autoscaling, or reproducible cloud deployment.
- Scaling decisions are manual; nothing scales consumers on Kafka lag or restarts failed components safely.
- Observability is ad hoc: no standard dashboards, tracing across the whole path, SLOs, or alerts proven to fire.
- Failure testing has been component by component; you have not yet run system-level chaos or written an incident postmortem.

Stage 7 adds Kubernetes, Helm, autoscaling (HPA and KEDA on lag), Terraform, CI/CD, load balancer tuning, full observability with SLOs, and chaos testing. Before starting, write down the three failures you most expect to hurt in production-like conditions, and what you expect your alerts to show for each.

When you are ready, say **Stage 7**.
