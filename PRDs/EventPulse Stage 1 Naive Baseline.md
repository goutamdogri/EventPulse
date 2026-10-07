# EventPulse Stage 1: Naive Baseline

*Duration: about 2 weeks. Goal: build the simplest correct ingest service (NestJS on Express writing straight to Postgres), then break it on purpose and record exactly how and why it fails. This is the permanent "before" for every later stage.*

## 1. Objective and Motivating Pain

Stage 0 taught you to measure. Stage 1 gives you something worth measuring: the design most developers build first, where the HTTP handler waits for a database write before answering.

By the end of this stage you must be able to:

1. Show the latency-vs-load curve of the naive service and identify its knee.
2. Explain each failure with evidence: connection pool exhaustion, lock contention, write amplification from indexes, slow-database propagation, and retry storms.
3. Show that the API's latency is essentially the database's latency, and that one slow dependency can stop all ingestion.
4. State precisely which problems a queue (Stage 2) will fix and which it will not.

**Your Stage 0 prediction to verify:** a handler that awaits a slow downstream behaves like `/io`. Latency tracks database latency, and in-flight requests pile up per Little's Law (concurrency = rate x latency) until a pool or limit is exhausted. You are checking whether that was right and by how much.

## 2. Prerequisites and Setup

| Item | Requirement |
| --- | --- |
| Stage 0 | Complete: harness, notebook habit, benchmark method |
| Runtime | Node LTS, TypeScript, NestJS (default Express adapter) |
| Database | Postgres 16 in Docker, with `pg_stat_statements` enabled |
| DB client | `pg` with `Pool`, deliberately no ORM yet (an ORM is a later experiment) |
| Tools | k6, wrk2, `psql`, `iostat`, `pidstat`, `ss`, a flame graph tool (`0x` or `clinic flame`) |
| Environment | Server, Postgres, and load generator on separate cores at minimum; two machines if you can |

Give Postgres explicit resources so results are comparable between runs.

```yaml
# docker-compose.yml (database portion)
services:
  postgres:
    image: postgres:16
    environment:
      POSTGRES_PASSWORD: eventpulse
      POSTGRES_DB: eventpulse
    command: >
      postgres
      -c shared_preload_libraries=pg_stat_statements
      -c max_connections=100
      -c log_min_duration_statement=200
    cpus: 2
    mem_limit: 2g
    shm_size: 512m
    ports: ["5432:5432"]
    volumes: ["pgdata:/var/lib/postgresql/data"]
volumes:
  pgdata:
```

Record the Postgres version and every non-default setting in the notebook. Do not tune anything yet; defaults are the point of a naive baseline.

## 3. Learning Plan

### Postgres concepts you need for this stage

- **Process-per-connection model.** Each client connection is a backend OS process with real memory and scheduling cost. `max_connections` is a ceiling you can hit. This is why "just add more connections" does not scale.
- **Write path:** a row insert writes to the WAL (write-ahead log) and must be flushed to disk at commit when `synchronous_commit` is on. Commit latency is largely fsync latency.
- **MVCC:** updates and deletes create new row versions; dead tuples need vacuuming. Inserts are cheap, but indexes still must be updated.
- **Index cost:** every index on a table is one more structure to update per insert. Insert throughput drops as indexes are added.
- **Locks:** row-level locks block conflicting updates to the same row. A "hot row" (for example a per-tenant counter) serializes all writers.
- **Checkpoints:** periodic flushes of dirty pages can cause latency spikes under sustained writes.
- **Wait events:** `pg_stat_activity.wait_event_type` and `wait_event` tell you what each backend is waiting for (I/O, locks, WAL).

### Node and Nest concepts

- **`pg.Pool`:** default `max` is 10. Requests beyond that wait in a client-side queue. `connectionTimeoutMillis` controls how long they wait before failing. Know `pool.totalCount`, `idleCount`, and `waitingCount`.
- **NestJS request lifecycle:** middleware, guards, interceptors, pipes, controller, filters. Each layer adds some CPU per request. `class-validator` validation is not free.
- **Express adapter:** NestJS default. It is slower than Fastify in raw benchmarks; you will measure the adapter cost later rather than assume it.
- **HTTP semantics:** 202 Accepted vs 201 Created, 400 vs 422, 429, 503 with `Retry-After`.

### Reading

DDIA chapters 2-3 (data models, storage and retrieval: how B-tree indexes and write-ahead logs behave). Note in `notebook/ddia.md` how Chapter 3 explains why indexes slow writes. The Postgres documentation pages on `pg_stat_activity`, `pg_stat_statements`, and WAL configuration are your reference for this stage.

## 4. Requirements

### Functional

| ID | Requirement |
| --- | --- |
| S1-F1 | `POST /v1/events` accepts one event, validates it, writes it to Postgres, returns 202 with the event id |
| S1-F2 | Reject invalid events with 400 and a clear error body, without touching the database |
| S1-F3 | Authenticate with `Authorization: Bearer <api_key>`; map the key to a tenant (static map from environment config for now) |
| S1-F4 | Duplicate `event_id` for the same event returns 202 and writes nothing (`ON CONFLICT DO NOTHING`); this is naive DB-level dedup that you will test and criticize in Stage 3 |
| S1-F5 | `GET /healthz` (liveness) and `GET /readyz` (checks DB connectivity) |
| S1-F6 | `GET /debug/stats` exposing pool counts, event-loop delay, and request counters |

### Non-functional (for this stage)

- All limits are configurable by environment variable: pool size, connection timeout, statement timeout, payload limit.
- Structured logging off by default during load runs (log volume distorts results); toggle by env.
- No retries inside the server in the baseline; failures surface honestly.

### Error contract

| Condition | Status |
| --- | --- |
| Missing or bad key | 401 |
| Schema or field invalid | 400 |
| Body over limit | 413 |
| Pool wait timeout or DB error | 503 with `Retry-After` |
| Success (committed) | 202 |

Note what 202 means here: **committed to Postgres**. In Stage 2 its meaning changes to "durably accepted by the queue," which is a different promise, and you should write that difference down.

## 5. Design

```
k6 / wrk2  ->  NestJS (Express)  ->  pg.Pool  ->  Postgres
                 |  guard (api key)
                 |  pipe (validation)
                 |  controller -> service -> INSERT ... ON CONFLICT DO NOTHING
                 v
          /debug/stats, logs (off during tests)
```

### Schema

```sql
CREATE TABLE events (
  event_id       uuid PRIMARY KEY,
  tenant_id      text        NOT NULL,
  type           text        NOT NULL,
  occurred_at    timestamptz NOT NULL,
  received_at    timestamptz NOT NULL DEFAULT now(),
  schema_version int         NOT NULL DEFAULT 1,
  payload        jsonb       NOT NULL
);

CREATE INDEX events_tenant_time_idx ON events (tenant_id, occurred_at);
```

The primary key on a random UUID and the secondary index are deliberate: they are realistic choices that carry write costs you will quantify.

### Repo additions

```
services/ingest-api/
  src/
    main.ts
    app.module.ts
    events/ (controller, service, dto)
    auth/ (api-key guard)
    db/ (pool provider, health)
    debug/ (stats controller)
  test/ (unit, integration)
infra/docker-compose.yml
db/migrations/0001_create_events.sql
loadtests/k6/ingest.js
notebook/ stage-1-*.md
```

## 6. Step-by-Step Build Plan

1. **Scaffold.** Create the Nest app, confirm it uses the Express adapter, add config module reading env vars.
2. **Database.** Start Postgres via compose, run the migration, enable `pg_stat_statements`, confirm with `psql`.
3. **Pool provider.** One shared `pg.Pool` configured from env.
4. **DTO and validation.** Define `CreateEventDto` with `class-validator`.
5. **Auth guard.** Static key-to-tenant map.
6. **Service and controller.** Implement the insert and 202 response.
7. **Health and stats endpoints.**
8. **Tests.** Unit and integration (section 7).
9. **Harness.** k6 script generating unique events.
10. **Baseline run, then the break-it catalog** (section 8).
11. **Profile and write the report** (section 10).

### Core code sketch

```ts
// create-event.dto.ts
export class CreateEventDto {
  @IsUUID() event_id: string;
  @IsString() @Length(1, 100) type: string;
  @IsISO8601() occurred_at: string;
  @IsInt() @Min(1) schema_version: number;
  @IsObject() payload: Record<string, unknown>;
}

// events.controller.ts
@Controller('v1/events')
@UseGuards(ApiKeyGuard)
export class EventsController {
  constructor(private readonly svc: EventsService) {}

  @Post()
  @HttpCode(202)
  async ingest(@Body() dto: CreateEventDto, @Req() req: any) {
    await this.svc.insert(req.tenantId, dto);
    return { event_id: dto.event_id };
  }
}

// events.service.ts
@Injectable()
export class EventsService {
  constructor(@Inject(PG_POOL) private readonly pool: Pool) {}

  async insert(tenantId: string, e: CreateEventDto) {
    await this.pool.query(
      `INSERT INTO events (event_id, tenant_id, type, occurred_at, schema_version, payload)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (event_id) DO NOTHING`,
      [e.event_id, tenantId, e.type, e.occurred_at, e.schema_version, e.payload],
    );
  }
}
```

In `main.ts`, use `ValidationPipe` globally and set the body size limit from env. Map pool timeout and database errors to 503 with `Retry-After` using an exception filter.

### Load script sketch

```js
import http from 'k6/http';
import { uuidv4 } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';

export const options = {
  scenarios: {
    ingest: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 500), timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 200, maxVUs: 3000,
    },
  },
};

const pad = 'x'.repeat(Number(__ENV.PAD || 300));
export default function () {
  const body = JSON.stringify({
    event_id: uuidv4(), type: 'order.created',
    occurred_at: new Date().toISOString(), schema_version: 1,
    payload: { user: 'u' + Math.floor(Math.random() * 1e6), data: pad },
  });
  http.post(`${__ENV.BASE || 'http://localhost:3000'}/v1/events`, body,
    { headers: { 'Content-Type': 'application/json', Authorization: 'Bearer ' + __ENV.KEY } });
}
```

Keep the event around 500 bytes to match the planning assumption in the blueprint. Remember Stage 0 rules: open-model load, generator headroom checked, warm-up excluded.

## 7. Test Plan

| Layer | Tests | Pass condition |
| --- | --- | --- |
| Unit | DTO validation (valid, missing field, bad uuid, bad timestamp, oversized), auth guard (missing, wrong key) | Correct accept/reject |
| Integration | Real Postgres via testcontainers or compose: post event, row exists with correct tenant and fields | Row matches |
| Integration | Duplicate `event_id` posted twice | One row, both calls 202 |
| Integration | Invalid event | 400 and zero rows written |
| Integration | Database stopped | 503 with `Retry-After`, no process crash, recovers when DB returns |
| Contract | Error response shapes per the table in section 4 | Shapes match |
| Load sanity | 100 rps for 60 s | Zero errors, row count equals requests sent |
| Row-count audit | After every load run, compare `SELECT count(*)` to successful responses | Must reconcile; discrepancies are findings, not noise |

The row-count audit is the most important habit in this stage. A system that returns 202 but lost the row, or wrote duplicates, has a correctness bug that load tests alone will not reveal.

## 8. Break-It Experiment Catalog

Write the hypothesis in the notebook before every run. Change one variable at a time.

### B1 Ramp to collapse (the baseline)

- **Hypothesis:** latency stays flat, then bends sharply at a knee set by either pool size, database commit capacity, or CPU of the Node process; I will predict which first.
- **How:** step test from low rate upward in even steps, 60 s each.
- **Observe:** p50/p99 vs rate, error rate, Node CPU, Postgres CPU, disk utilization, pool `waitingCount`.
- **Explains:** the real capacity of the naive design and which resource limits it.

### B2 Pool exhaustion

- **Hypothesis:** with `max=10`, requests queue in the pool once concurrent DB work exceeds 10; latency rises by queue wait, then requests fail on `connectionTimeoutMillis`.
- **How:** run at a fixed high rate with pool max of 5, 10, 25, 50, 90, then above Postgres `max_connections`.
- **Observe:** `waitingCount`, request latency vs DB query time, error type, `pg_stat_activity` count, Postgres memory and CPU as connections rise.
- **Explains:** more connections help only up to a point, then hurt; the optimum is small relative to the connection limit.

### B3 Slow database propagates to the API

- **Hypothesis:** adding latency to every query raises API latency one-for-one and cuts max rps by about pool\_size / query\_latency.
- **How:** add artificial delay (for example `SELECT pg_sleep(0.05)` before the insert in a test build, or network latency to the DB container with `tc`/toxiproxy), compare 0, 20, 50, 200 ms.
- **Observe:** capacity vs added latency, in-flight requests vs Little's Law, time to recover after removal.
- **Explains:** the API cannot be faster than its slowest synchronous dependency.

### B4 Lock contention with a hot row

- **Hypothesis:** if each insert also increments a per-tenant counter row, all writers for that tenant serialize on one row lock and throughput collapses even at low CPU.
- **How:** add `UPDATE tenant_counters SET n = n + 1 WHERE tenant_id = $1` in the same transaction; test with 1 tenant vs 1000 tenants.
- **Observe:** `pg_stat_activity` wait events (`Lock`), `pg_locks`, throughput vs tenant count, low CPU with high latency.
- **Explains:** contention is a design problem that more hardware does not fix; the later answer is aggregation downstream, not synchronous counters.

### B5 Index write amplification

- **Hypothesis:** each added index lowers insert throughput.
- **How:** run with 0 secondary indexes, 1, 3, 5 (for example on `type`, `(tenant_id, type, occurred_at)`, a GIN on `payload`), plus the random-UUID primary key vs a time-ordered key.
- **Observe:** max rps, WAL volume, index sizes (`pg_relation_size`), CPU, checkpoint behavior.
- **Explains:** why write-heavy ingestion stores are indexed lightly, and part of why ClickHouse arrives in Stage 4.

### B6 Durability knob

- **Hypothesis:** `synchronous_commit=off` raises throughput significantly but allows loss of the most recent commits on crash.
- **How:** compare `on` vs `off` at the knee; then kill the Postgres container hard and check what was lost.
- **Observe:** rps gain, p99 change, count of rows lost vs 202 responses returned.
- **Explains:** durability has a measurable price; you will meet the same trade-off with Kafka `acks` in Stage 2. This is a diagnostic experiment, not a configuration to keep.

### B7 Payload size

- **Hypothesis:** throughput falls as payload grows because of JSON parsing, network, WAL, and TOAST for large JSONB values.
- **How:** payload sizes of 200 B, 500 B, 2 KB, 10 KB, 100 KB.
- **Observe:** rps at the knee per size, WAL bytes per event, table size growth.
- **Explains:** bytes per event drive cost everywhere in the system.

### B8 Database restart under load

- **Hypothesis:** during a Postgres restart the API returns errors, the pool may hold dead connections, and recovery may lag behind DB availability.
- **How:** restart the container mid-load; also test a network pause.
- **Observe:** error rate over time, time until success rate recovers, whether the pool reconnects cleanly, row-count audit afterwards.
- **Explains:** failure behavior of the dependency chain, and how many events producers lose if they do not retry.

### B9 Retry storm

- **Hypothesis:** if clients retry immediately on failure, an overload becomes self-sustaining: offered load rises above the original rate and the system stays collapsed even after the trigger ends.
- **How:** add client-side retries (up to 3, no backoff) in the k6 script; trigger a short DB slowdown; remove the slowdown.
- **Observe:** offered rate vs original, time to recover with and without backoff and jitter, whether the system recovers on its own.
- **Explains:** metastable failure, and why rate limiting, backoff, and load shedding appear later.

### B10 Missing timeouts

- **Hypothesis:** without a statement timeout and pool wait timeout, a slow query ties up pool connections indefinitely and requests hang rather than fail.
- **How:** run with timeouts unset, then set sensible values and repeat B3.
- **Observe:** request latency distribution, hung requests, memory growth in Node from queued requests.
- **Explains:** why every external call needs a deadline.

### B11 Table growth soak

- **Hypothesis:** over a long run, throughput and p99 degrade as the table and indexes outgrow memory and checkpoints add spikes.
- **How:** run at about 70% of the knee for 30-60 minutes, then longer if feasible, sampling every minute.
- **Observe:** rps and p99 over time, table and index size, checkpoint frequency (`pg_stat_bgwriter`, or `pg_stat_checkpointer` on newer versions), buffer cache hit ratio, disk I/O.
- **Explains:** short tests hide long-run decay, which is why you will run soak tests in later stages.

### B12 Adapter and ORM cost (optional)

- **Hypothesis:** the Express adapter and an ORM each add measurable overhead compared with Fastify and raw `pg`.
- **How:** switch the Nest adapter to Fastify and re-run B1; separately replace raw SQL with your preferred ORM and re-run.
- **Observe:** capacity change and CPU profile shares.
- **Explains:** how much of the ceiling is framework overhead vs database, setting up the Stage 8 language and framework comparison.

## 9. Metrics and Diagnostics

For each run record the standard Stage 0 fields plus:

- **API:** rps, status codes, latency percentiles, event-loop delay, CPU, memory, pool `totalCount` / `idleCount` / `waitingCount`.
- **Postgres:** active connections, wait events, lock waits, commit rate, WAL generated, checkpoint counts, buffer cache hit ratio, table and index sizes.
- **Host:** per-core CPU for API and DB, disk utilization and await (`iostat -x 1`), network.
- **Correctness:** requests sent, 202 received, rows in table.

Useful queries:

```sql
-- What are backends doing right now?
SELECT state, wait_event_type, wait_event, count(*)
FROM pg_stat_activity WHERE datname = 'eventpulse'
GROUP BY 1,2,3 ORDER BY 4 DESC;

-- Slowest and heaviest statements
SELECT calls, mean_exec_time, total_exec_time, query
FROM pg_stat_statements ORDER BY total_exec_time DESC LIMIT 10;

-- Blocked and blocking
SELECT pid, pg_blocking_pids(pid) AS blocked_by, wait_event_type, wait_event, query
FROM pg_stat_activity WHERE cardinality(pg_blocking_pids(pid)) > 0;

-- Sizes
SELECT pg_size_pretty(pg_table_size('events')) AS table,
       pg_size_pretty(pg_indexes_size('events')) AS indexes;
```

Reset `pg_stat_statements` and truncate the table between runs so results are comparable (`SELECT pg_stat_statements_reset(); TRUNCATE events;`).

**Flame graphs:** capture one at about 70% of the knee and one at saturation (`0x` or `clinic flame` on the Node process). Compare where CPU time goes: framework, validation, JSON, the `pg` driver, GC.

## 10. Artifact: Baseline Report

Write `docs/benchmarks/stage-1-baseline.md`:

1. **Setup:** hardware, versions, Postgres settings, topology, pool configuration.
2. **Method:** workload, event size, load model, warm-up, repeat count, correctness audit.
3. **Baseline curve:** latency and error rate vs offered rate, the knee, the dominant resource.
4. **Failure catalog:** one subsection per B-experiment with hypothesis, data, and explanation (chart plus the key diagnostic output).
5. **Flame graphs** at 70% and at saturation, with commentary.
6. **What I predicted wrong.**
7. **What a queue will fix, and what it will not.** Be specific: it removes DB latency from the request path and absorbs bursts, but it does not fix hot-row design, add durability by itself, or remove duplicates (Stage 3).
8. **Questions Stage 2 must answer.**

This report is the baseline cited by every later benchmark. Keep the raw result files and the exact commit so any number can be reproduced.

**Optional blog post:** "Why writing to Postgres in the request path stops scaling" using your own graphs.

## 11. Common Pitfalls

- Letting the load generator or Postgres run out of CPU on the same machine and blaming the wrong component.
- Skipping the row-count audit, so lost or duplicated writes go unnoticed.
- Forgetting to truncate the table between runs, so index and table size contaminate comparisons.
- Tuning Postgres before recording the untuned baseline.
- Raising pool size to "fix" latency without checking database CPU and context switching.
- Measuring with logging, ORM debug output, or the profiler on during the baseline.
- Treating `synchronous_commit=off` as a free win instead of a durability trade.
- Declaring a result from one run; repeat and report variance.
- Mistaking client-side queueing in the pool for database slowness. Compare pool wait time with query time.

## 12. Exit Criteria

- [ ] Service runs with one command and passes unit and integration tests.
- [ ] Row-count audit reconciles on every clean load run.
- [ ] Baseline latency-vs-rate chart with the knee identified and the limiting resource proven with diagnostics.
- [ ] Experiments B1 through B11 each have a notebook entry (B12 optional).
- [ ] You can explain pool exhaustion, lock contention, and index write amplification using your own evidence.
- [ ] You demonstrated a retry storm and showed the effect of backoff with jitter.
- [ ] Flame graphs captured and interpreted.
- [ ] Baseline report committed, repo tagged `stage-1-complete`.

**Self-test questions (answer aloud, without notes):**

1. If Postgres adds 100 ms to every query and the pool has 10 connections, what is the maximum throughput of the service, and why?
2. Why can adding more connections make Postgres slower?
3. Why can a system show low CPU and still be completely stuck?
4. What exactly did `synchronous_commit=off` trade away, and what did you measure that proves it?
5. How does a retry storm keep a system down after its trigger is gone?
6. What does a 202 mean in this stage, and how will that promise change in Stage 2?

## 13. Handoff to Stage 2

Stage 1 should leave you with concrete motivation, not just a vague sense that "queues are better":

- The request path waits on the database, so API latency and capacity are bounded by the database.
- A slow or restarting database stops ingestion immediately and loses events from non-retrying clients.
- Bursts above the knee are lost or rejected rather than absorbed.
- Synchronous contention (hot rows) and heavy indexing limit write throughput regardless of API scaling.

Stage 2 introduces Kafka between the API and the database so ingestion depends only on durably appending to a log. It will also create new problems you have not faced yet: duplicates, consumer lag, rebalances, and a different durability story. Write down, before starting it, your prediction of how much the ingest knee will move and which resource will become the new limit.

When you are ready, say **Stage 2**.
