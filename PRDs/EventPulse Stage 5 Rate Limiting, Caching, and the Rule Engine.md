# EventPulse Stage 5: Rate Limiting, Caching, and the Rule Engine

*Duration: about 3 weeks. Goal: protect the shared platform from misbehaving tenants, make hot metadata lookups cheap without making them wrong, and turn aggregates into reliable, deduplicated actions.*

## 1. Objective and Motivating Pain

Stage 4 produced a correct, fast pipeline for well-behaved tenants. In a multi-tenant system the real danger is the tenant who is not well-behaved: one that floods ingest, sends huge key cardinality, or saturates a partition. Nothing yet stops that, and nothing yet acts on the aggregates you compute.

By the end of this stage you must be able to:

1. Implement a distributed per-tenant rate limiter (token bucket in Redis via Lua) and compare it with fixed-window and sliding-window approaches using benchmarks.
2. Demonstrate multi-tenant isolation: a flooding tenant cannot degrade other tenants beyond a measured, stated bound.
3. Build a cache-aside layer with TTL jitter, stampede protection, and event-driven invalidation, and break it with a thundering herd.
4. Build a rule engine that evaluates rules on your aggregate stream and triggers notifications that are retried, deduplicated, and idempotent.
5. Document an explicit failure policy for every new dependency (Redis down, cache stale, webhook failing).

**Predictions to write down first:** how much will other tenants' p99 rise when one tenant sends 50x its limit? How much Redis load does the limiter add per request? What will a cache stampede do to Postgres? How many duplicate notifications will a processor restart cause?

## 2. Prerequisites and Setup

| Item | Requirement |
| --- | --- |
| Stages 0-4 | Complete: outbox, retry and DLQ, aggregate stream `aggregates.out`, delivery guarantees document |
| Redis | Primary plus replica plus Sentinel in docker-compose, so you can test failover (cluster mode and sharding are Stage 6) |
| Client | `ioredis` (Lua script commands, Sentinel support) |
| Fault tools | toxiproxy for latency and cuts, `redis-cli`, `redis-benchmark` |
| Webhook simulator | A small controllable HTTP server that can return success, errors, slow responses, hangs, and random failures on demand |
| Load scenarios | k6 scripts with a multi-tenant mix: N well-behaved tenants plus one tenant that can be turned into a flood |
| Postgres | New tables: tenants (limits), rules, webhook endpoints, alerts, action log |

## 3. Learning Plan

### Rate limiting

- **Fixed window counter:** simple, but allows up to twice the limit around a window boundary.
- **Sliding window log:** exact, but stores a timestamp per request (memory heavy).
- **Sliding window counter:** approximates a sliding window using two adjacent counters; cheap and close.
- **Token bucket:** tokens refill at a steady rate up to a burst capacity; a request spends tokens. Allows controlled bursts with a sustained rate. The standard choice for APIs.
- **Leaky bucket and GCRA:** smoothing alternatives. Know how they differ from token bucket.
- **Dimensions and limits:** per tenant, per API key, per endpoint; requests per second, bytes per second, daily quotas, and limits on key cardinality. Different dimensions protect different resources.
- **Distributed correctness:** the check-and-update must be atomic. A Redis Lua script runs atomically. Use Redis `TIME` inside the script instead of application clocks so skewed nodes agree.
- **Cost and hot keys:** every request calls Redis; one tenant is one key, which lives on one shard. Know the two-level design: each API node leases a block of tokens from Redis and spends them locally, trading some accuracy for far fewer Redis calls.
- **Failure policy:** when Redis is slow or down: fail open (accept, risk overload), fail closed (reject, risk outage), or degrade to a local limiter with a fraction of the limit. Choose per use and document it.
- **Response contract:** HTTP 429 with `Retry-After`; clients must back off. Distinguish rate limiting (policy about a tenant) from load shedding (protection of the system, 503).

### Multi-tenant isolation

- **Noisy neighbor** and **fairness:** a rate limit protects your system but a rejected request still costs CPU, so enforce as early and cheaply as possible.
- **Bulkheads:** separate resource pools or queues so one tenant's failure cannot consume shared capacity (worker pools per endpoint, per-tenant caps in notifier concurrency).
- **Quotas vs limits:** burst control vs total consumption over time.

### Caching

- **Cache-aside:** read cache, on miss read the origin and populate. The simplest and most common pattern.
- **TTL strategy:** TTL with random jitter so many keys do not expire together. Negative caching for "does not exist".
- **Stampede (thundering herd):** many requests miss at once and hammer the origin. Defenses: single-flight (coalesce concurrent misses in-process), a short distributed lock or lease, refresh-ahead or probabilistic early expiration, and serving stale data while revalidating.
- **Invalidation:** explicit invalidation on change (your `config.changed` event from the Stage 3 outbox) plus TTL as the safety net. Know the staleness window you are accepting.
- **Two levels:** in-process L1 plus Redis L2, and the consistency cost of the extra layer.
- **Penetration:** floods of lookups for keys that do not exist bypass the cache unless negatives are cached.
- **Eviction:** memory limits and policies (`allkeys-lru`, etc.).

### Rule engine

- **Rule model:** a condition over an aggregate (threshold, rate of change, absence of events), a duration (`for N windows`), a cooldown, and an action.
- **Alert state machine:** OK, PENDING, FIRING, RESOLVED. Hysteresis and cooldown prevent flapping near a threshold.
- **Idempotent actions:** an action is triggered by a *state transition*, and its idempotency key is derived from the rule, alert, and transition so replays and restarts cannot fire it twice.
- **Delivery:** at-least-once webhooks with timeouts, retries with backoff and jitter, circuit breakers per endpoint, and the Stage 3 DLQ. Make receivers able to dedup using the idempotency key you send.
- **Webhook security:** sign payloads (HMAC with a per-endpoint secret), enforce HTTPS, and defend against SSRF: resolve and validate the destination address, block private, loopback, and link-local ranges (including cloud metadata addresses), re-check at connection time to defeat DNS rebinding, and limit redirects, time, and response size.

### Reading

Redis documentation on Lua scripting and atomicity and on eviction. Engineering write-ups on rate limiting at scale from large API companies. The AWS Builders' Library articles on timeouts, retries and backoff with jitter, and on load shedding. Google's SRE material on handling overload and cascading failures. In `notebook/limiter-notes.md`, explain why a fixed window allows a 2x burst and draw it.

## 4. Requirements

| ID | Requirement |
| --- | --- |
| S5-F1 | Per-tenant limits (rate, burst, max payload) stored in Postgres and enforced at ingest by a Redis token bucket implemented as a Lua script |
| S5-F2 | A common limiter interface with three interchangeable implementations (fixed window, sliding window counter, token bucket) and a benchmark harness |
| S5-F3 | Denied requests return 429 with `Retry-After` and are counted per tenant |
| S5-F4 | A documented, configurable failure policy for the limiter (fail open, fail closed, local fallback) |
| S5-F5 | Tenant and API key configuration served through a cache-aside layer with TTL jitter, single-flight, negative caching, and invalidation on `config.changed` |
| S5-F6 | Admin API to create and update rules, endpoints, and limits; writes use a transaction plus outbox |
| S5-F7 | A rule engine consuming `aggregates.out`, evaluating rules with duration, hysteresis, and cooldown |
| S5-F8 | Alert state transitions create actions through the outbox; an idempotent notifier delivers webhooks with retries, circuit breakers, per-endpoint and per-tenant concurrency caps, and DLQ |
| S5-F9 | Webhook URLs validated against SSRF; payloads signed |
| S5-F10 | Isolation requirement: during a flood from one tenant, other tenants' ingest p99 stays within a stated bound of baseline (hypothesis: 20%) |

### Rule example

```json
{
  "rule_id": "r_123", "tenant_id": "t1",
  "metric": "count", "event_type": "order.failed",
  "window": "1m", "condition": { "op": ">", "value": 100 },
  "for_windows": 2, "cooldown_s": 300,
  "action": { "type": "webhook", "endpoint_id": "e_9" }
}
```

## 5. Design

```
Client -> LB -> API node
   auth/tenant config:  L1 (in-process) -> L2 (Redis) -> Postgres (single-flight)
   limiter:              Redis Lua token bucket (or local lease) per tenant
   validate -> serialize -> Kafka events.raw

Admin API -> Postgres tx (config/rule) + outbox -> relay -> Kafka config.changed
                                                    |-> API nodes: invalidate cache
                                                    |-> Rule engine: reload rules

aggregates.out -> Rule engine
    state: per (rule, tenant, key) in Redis (ephemeral) + alerts table in Postgres
    on state transition: Postgres tx { update alert; insert outbox(action) }
    outbox relay -> Kafka actions.pending -> Notifier workers
        idempotency key = rule_id:alert_id:transition
        per-endpoint circuit breaker, per-tenant concurrency cap
        transient fail -> retry tiers; permanent / exhausted -> DLQ
```

### Token bucket script (starting point)

```lua
-- KEYS[1] = bucket key
-- ARGV[1] = rate (tokens/sec), ARGV[2] = burst, ARGV[3] = cost
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost  = tonumber(ARGV[3])
local t = redis.call('TIME')
local now = t[1] * 1000 + math.floor(t[2] / 1000)
local d = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local tokens = tonumber(d[1])
local ts = tonumber(d[2])
if tokens == nil then tokens = burst; ts = now end
local elapsed = math.max(0, now - ts)
tokens = math.min(burst, tokens + elapsed * rate / 1000)
local allowed = 0
local retry_ms = 0
if tokens >= cost then
  tokens = tokens - cost; allowed = 1
else
  retry_ms = math.ceil((cost - tokens) * 1000 / rate)
end
redis.call('HSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], math.ceil(burst / rate * 1000) * 2)
return { allowed, math.floor(tokens), retry_ms }
```

Check how your Redis version treats `TIME` inside scripts, and keep scripts free of per-call allocations you can avoid. Parameterize time through an optional argument only in test mode so unit tests can be deterministic.

### Cache design

- Key: `cfg:{tenant}` with a value containing limits and endpoint references, plus a version.
- TTL: base 5 minutes with plus or minus 20% jitter; negative entries 30 seconds.
- Single-flight: a map of in-flight origin loads keyed by cache key, so 1000 concurrent misses cause one database read per node; optionally a short Redis lock to coalesce across nodes.
- Invalidation: on `config.changed`, delete L1 and L2 entries for the tenant. If the message is lost, the TTL bounds staleness; that bound is a documented number.

### Failure policy table (to fill in with data)

| Dependency | Failure | Policy | Rationale | Measured effect |
| --- | --- | --- | --- | --- |
| Redis (limiter) | slow or down | decide per limiter: fail open / fail closed / local fallback |  |  |
| Redis (cache L2) | down | fall through to Postgres with single-flight and a cap |  |  |
| Postgres | slow | serve stale config within a stated maximum age |  |  |
| Webhook endpoint | down or slow | retry with backoff, circuit open, DLQ |  |  |
| Kafka (`config.changed`) | lagging | TTL bounds staleness |  |  |

### Repo additions

```
libs/ratelimit/ (interface, fixed, sliding, token-bucket, lease, lua/)
libs/cache/ (cache-aside, single-flight, jitter, negative cache)
services/rule-engine/ (rules, state machine, evaluator)
services/notifier/ (workers, breaker, signing, ssrf-guard)
tools/webhook-sim/ (controllable endpoint)
loadtests/k6/multi-tenant.js (normal tenants plus flood tenant)
tests/isolation/ (automated noisy-neighbor test)
docs/ (failure-policy.md, cache-design.md, rule-engine.md, adr/)
```

## 6. Step-by-Step Build Plan

1. **Tenant config, admin API, and outbox.** Limits and rules stored in Postgres; changes publish `config.changed`.
2. **Cache layer** with jitter, negative caching, single-flight, and invalidation. Measure before and after.
3. **Limiter interface** and the three algorithms. Microbenchmark each against a local Redis.
4. **Integrate** the token bucket into the ingest path with `429` and `Retry-After`.
5. **Failure policy** implementation and configuration.
6. **Multi-tenant load harness** and the baseline isolation measurement *before* turning limits on, so you can show the improvement.
7. **Two-level limiter** (token lease) and measure Redis load reduction vs accuracy loss.
8. **Rule engine:** parse rules, evaluate aggregates, implement the state machine with hysteresis and cooldown.
9. **Notifier:** workers, signing, SSRF guard, retries, breakers, concurrency caps.
10. **Test suite** (section 7).
11. **Break-it catalog** (section 8).
12. **Isolation demo and report** (section 10).

## 7. Test Plan

| Layer | Tests | Pass condition |
| --- | --- | --- |
| Unit | Each limiter: allow, deny, refill, burst, `Retry-After` math; cache jitter bounds; rule state machine transitions including hysteresis and cooldown | Spec behavior |
| Property-based | For any request sequence, tokens spent over any interval never exceed `rate * T + burst` | No counterexample |
| Concurrency | 100 parallel clients against one bucket | Allowed total within the bound, no negative tokens |
| Multi-node | 3 API nodes sharing one tenant limit | Aggregate allowed rate close to the configured limit |
| Fixed-window boundary | Burst at a window edge | Reproduces the 2x effect; token bucket does not |
| Cache | 1000 concurrent misses | One origin read per node; hit ratio recorded; invalidation visible within the target time |
| Rule engine | Replay the same aggregate stream twice; restart mid-stream | No duplicate transitions, no duplicate actions |
| Notifier | Success, transient failure then success, permanent failure, timeout, circuit open, duplicate delivery of the same action | Correct retry tiers, DLQ entry, one effective delivery per idempotency key |
| Security | Webhooks to loopback, private ranges, link-local metadata address, redirect to internal host, DNS rebinding simulation | Blocked and logged |
| Isolation | Automated flood from one tenant | Others' p99 within the stated bound |

## 8. Break-It Experiment Catalog

Write the hypothesis first. Record per-tenant metrics, not just totals.

### R1 Limiter algorithm comparison

- **Hypothesis:** fixed window permits about 2x bursts at boundaries; sliding window counter is close to exact at low cost; token bucket allows controlled bursts; sliding log is exact but memory heavy.
- **How:** drive identical bursty traffic through each; measure accepted counts vs ideal, Redis commands per request, memory per tenant, added latency.
- **Observe:** accuracy, cost, tail latency.
- **Explains:** your algorithm choice with numbers.

### R2 Noisy neighbor flood

- **Hypothesis:** without limits one tenant at 50x its normal rate raises other tenants' p99 sharply and builds lag; with limits others stay near baseline, but rejecting still consumes API CPU.
- **How:** well-behaved tenants at steady load plus one flood tenant, with and without the limiter.
- **Observe:** per-tenant p99, 429 rates, API and Redis CPU, Kafka and processor lag per partition.
- **Explains:** isolation limits; also test a tenant that stays *within* its rate but sends huge payloads or high key cardinality, which the request-rate limiter does not catch.

### R3 Redis failure and failover

- **Hypothesis:** killing Redis causes errors or a policy-driven degradation; Sentinel failover takes seconds; token state may reset after promotion, allowing a burst.
- **How:** kill the primary under load for each failure policy; observe promotion time and post-failover bucket state.
- **Observe:** ingest availability, accepted rate during and after, duplicates of burst allowance, time to normal.
- **Explains:** the fail-open vs fail-closed vs fallback trade-off with measured consequences.

### R4 Redis latency

- **Hypothesis:** every ingest request now pays a Redis round trip; added latency propagates one-for-one, and a missing client timeout turns a slow Redis into a full outage.
- **How:** add latency with toxiproxy; compare with and without a tight timeout (for example a few milliseconds) on the limiter call.
- **Observe:** ingest p99, error rate, in-flight requests (Little's Law).
- **Explains:** timeout budgets for hot-path dependencies.

### R5 Hot tenant key and token leasing

- **Hypothesis:** one tenant's bucket is a single hot key capping per-tenant throughput at the Redis op rate; leasing blocks of tokens to nodes cuts Redis load by about the lease size at the cost of accuracy and burst over-allowance.
- **How:** a single tenant at very high rps with direct Redis checks, then with leases of several sizes.
- **Observe:** Redis ops per second, accuracy error, behavior when a node dies holding tokens.
- **Explains:** accuracy vs scalability.

### R6 Cache stampede and mass expiry

- **Hypothesis:** expiring a hot config key under load without protection sends a burst of identical queries to Postgres; identical TTLs make many keys expire together; single-flight and jitter reduce it to about one query per node.
- **How:** force expiry under load for three variants: naive, single-flight, single-flight plus jitter and early refresh.
- **Observe:** Postgres query rate, latency spike, connection pool waits.
- **Explains:** why each defense exists.

### R7 Stale configuration and lost invalidation

- **Hypothesis:** a lost or delayed `config.changed` leaves old limits in force until TTL; lowering a tenant's limit during an incident takes effect only after that delay.
- **How:** change a limit, drop or delay the invalidation message, measure how long old behavior continues.
- **Observe:** staleness duration vs TTL, effect on isolation.
- **Explains:** the staleness bound you must state, and whether the TTL should be shorter for limits than for descriptive config.

### R8 Cache penetration

- **Hypothesis:** a flood of requests with invalid keys or nonexistent tenants misses the cache every time and hits Postgres unless negative results are cached.
- **How:** send random API keys at high rate, with and without negative caching.
- **Observe:** Postgres load, latency, cache memory.
- **Explains:** negative caching and its own staleness cost.

### R9 Rule engine duplicates and flapping

- **Hypothesis:** replaying aggregates or restarting the engine does not create duplicate alerts because transitions are idempotent; a value oscillating around the threshold flaps without hysteresis and `for_windows`.
- **How:** replay the stream, crash and restart mid-evaluation, feed oscillating data.
- **Observe:** transition counts, actions created, actions delivered.
- **Explains:** state machine design and idempotency keys.

### R10 Webhook endpoint failures

- **Hypothesis:** a slow or hung endpoint exhausts notifier workers and delays other tenants' notifications unless timeouts, circuit breakers, and per-endpoint concurrency caps exist.
- **How:** simulator modes: 500s, timeouts, hang, intermittent failures, permanent failure; run alongside healthy tenants.
- **Observe:** delivery latency for healthy tenants, retry volume, circuit state changes, DLQ growth, time to recovery.
- **Explains:** bulkheads and breakers; then replay the DLQ with your tool.

### R11 Alert storm

- **Hypothesis:** one tenant with many rules, or many rules firing at once, can flood the notifier and Postgres unless per-tenant action rate limits apply.
- **How:** create thousands of rules that fire together.
- **Observe:** outbox lag, notifier queue depth, impact on other tenants.
- **Explains:** rate limiting outbound actions, not only inbound events.

### R12 Clock skew

- **Hypothesis:** limiters that read each node's clock disagree under skew; using Redis `TIME` in the script removes the problem.
- **How:** skew one API node's clock in a test environment.
- **Observe:** accepted rates per node.
- **Explains:** why shared state needs one time source.

### R13 SSRF attempts

- **Hypothesis:** an unprotected webhook feature lets a tenant make the platform call internal addresses.
- **How:** register endpoints pointing to loopback, private ranges, a link-local metadata address, a hostname that resolves internally, a redirect to an internal host, and a DNS name that changes after validation.
- **Observe:** each attempt blocked at validation or at connection, with an audit log.
- **Explains:** why validation must also happen at connect time.

### R14 Client behavior after 429

- **Hypothesis:** clients that ignore `Retry-After` and retry immediately amplify load, while honoring it with jitter drains the overload.
- **How:** two k6 client behaviors against a limited tenant.
- **Observe:** offered load, 429 rate, time to settle.
- **Explains:** the importance of the client contract, and a recommendation for your SDK.

## 9. Metrics and Diagnostics

**Limiter:** allowed and denied per tenant, script latency, Redis round-trip time, fallback activations, lease consumption.

**Cache:** hit and miss ratio (L1 and L2), origin calls, coalesced misses, invalidation lag, TTL distribution.

**Rule engine:** evaluations per second, evaluation lag (watermark to evaluation), transitions by type, suppressed duplicates.

**Notifier:** attempts, success rate per endpoint, retry tier volume, circuit state, DLQ size and age, delivery latency (condition met to webhook received).

**Isolation:** per-tenant ingest p99, per-tenant consumer and processor lag, per-tenant notification delay.

## 10. Artifacts

1. **Multi-tenant isolation demo:** charts and a short recording showing a flooding tenant with and without protection, plus the stated bound and whether it held.
2. **Rate limiter benchmark report:** accuracy, cost, and latency across algorithms, and the leasing trade-off.
3. **Failure policy document:** the filled table with measured consequences.
4. **Cache design document:** TTL, jitter, single-flight, invalidation, staleness bound, with the stampede experiment data.
5. **Rule engine design:** rule model, state machine diagram, idempotency key scheme, delivery semantics.
6. **Webhook security checklist** and the SSRF test results.
7. **ADRs:** limiter algorithm and placement, Redis vs in-process, failure policy per limiter, cache invalidation approach.
8. **Delivery guarantees update:** for example, notifications are delivered at least once with a stable idempotency key; an alert transition produces one logical action; limits take effect within the documented staleness bound.
9. **Optional blog post:** "I benchmarked four rate limiting algorithms" or "What happens when you expire a hot cache key."

## 11. Common Pitfalls

- Implementing the limiter with separate read and write calls instead of one atomic script.
- Using application clocks for token refill across multiple nodes.
- Testing only a single tenant, which hides every isolation problem.
- Forgetting that rejected requests still cost CPU, so limiter placement matters.
- Setting the same TTL everywhere so keys expire together.
- Caching without a defined staleness bound, or invalidating only on the happy path.
- Treating rule evaluation as stateless and then firing duplicates after a restart.
- Retrying notification failures without a circuit breaker or per-endpoint cap.
- Allowing webhook URLs without SSRF validation and signing.
- Declaring isolation achieved without measuring per-tenant latency during a flood.
- Letting Redis state loss silently reset limits and calling it fine without measuring the burst.

## 12. Exit Criteria

- [ ] Token bucket via Lua works across multiple API nodes, with concurrency and property tests passing.
- [ ] Three algorithms benchmarked with documented trade-offs.
- [ ] Noisy-neighbor test shows the isolation bound, with before and after charts.
- [ ] Failure policy documented and tested for Redis, cache, Postgres, webhooks, and invalidation lag.
- [ ] Cache stampede and mass expiry demonstrated and fixed with measurements.
- [ ] Rule engine with hysteresis and cooldown passes replay and restart tests with no duplicate actions.
- [ ] Notifier handles failures with retries, breakers, caps, DLQ, signing, and SSRF protection.
- [ ] Experiments R1 through R14 have notebook entries.
- [ ] Delivery guarantees document updated.
- [ ] Repo tagged `stage-5-complete`.

**Self-test questions (answer aloud, without notes):**

1. Why does a fixed window allow twice the limit, and how does a token bucket avoid it while still allowing bursts?
2. Why must the limiter be a single atomic operation, and why use Redis `TIME` instead of application clocks?
3. Redis dies. Describe the three failure policies and when you would choose each.
4. Explain a cache stampede and three different defenses. Which would you apply first and why?
5. How does your system ensure a rule that fires fires once, even after a crash and replay?
6. What stops a tenant from using your webhook feature to reach your internal network?

## 13. Handoff to Stage 6

Stage 5 leaves a protected, multi-tenant pipeline with actions. What it still cannot do:

- Scale the data layer: Redis is one logical hot spot, Postgres metadata and the tenant lookup path assume a single database, and one hot tenant key still limits throughput.
- Distribute tenants deliberately: you have no tenant-to-shard mapping, so adding capacity means rebalancing everything.
- Handle hot Kafka partitions beyond what limiting can prevent.
- Show the cost of rebalancing and topology change under load.

Stage 6 adds Postgres partitioning, a read replica, PgBouncer, a consistent-hash ring you implement yourself, and hot-partition handling in Kafka, then breaks them by adding and removing shards under load. Before starting, write down which of your Stage 5 limits, caches, or keys would break first if you doubled the number of API nodes and Redis instances.

When you are ready, say **Stage 6**.
