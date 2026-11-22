# Step 5 (S2-F5): problems found in earlier steps, and deviations

Date: 2026-11-22. S2-F5 was built on top of steps 1-4. These are the places where the earlier code could not support the requirement as written, and what was changed.

## 5.1 Step 1 producer could not start without Kafka, and could not reconnect

**Problem.** `ProducerService.onModuleInit` did `await producer.connect()` once. If Kafka was down at boot, the promise rejected and Nest aborted startup (process exit 1), which contradicts "API starts without Kafka and recovers without a restart". Also, in `@confluentinc/kafka-javascript` a producer whose `connect()` failed ends in state `DISCONNECTED`, and calling `connect()` on it again throws `Connect has already been called elsewhere`. A naive retry loop on the same instance would never succeed.

**Fix.** `services/ingest-api/src/kafka/producer.service.ts`: the producer is built by a `build()` factory; `connectLoop()` creates a **new producer per attempt**, with exponential backoff (`KAFKA_RECONNECT_INITIAL_MS` / `KAFKA_RECONNECT_MAX_MS`). `onModuleInit` waits at most `KAFKA_STARTUP_WAIT_MS` for the first connection, then lets the API start and keeps retrying in the background. A send that fails with `ERR__STATE` (-172, client disconnected) drops the client and starts a new connect loop. Once connected, librdkafka reconnects to restarted brokers by itself (verified by the recovery e2e test: 202 again with no API restart). Shutdown no longer hangs: `disconnect()` is raced against 5 s.

## 5.2 Step 1 batcher: unbounded waits and no deadline on drain

**Problem.** `MicroBatcher` (step 1) had a queue bound by count only, no send timeout, no breaker, and `drain()` waited forever. With Kafka down, a request could wait for librdkafka's whole delivery timeout (30 s default) and SIGTERM could hang while Kafka was down. Validation also refused `maxQueue < 1`, so the K10 "unbounded" mode could not exist.

**Fix.** `services/ingest-api/src/kafka/micro-batcher.ts` (plus new `errors.ts`, `breaker.ts`): queue bound by count **and** bytes (`BATCH_MAX_QUEUE`, `BATCH_MAX_QUEUE_BYTES`); per-send timeout (`KAFKA_SEND_TIMEOUT_MS`) that also caps queue wait; breaker (`KAFKA_BREAKER_FAILURES`, `KAFKA_BREAKER_COOLDOWN_MS`) that rejects new messages and the already-queued ones when it opens; `drain(deadlineMs)` with `SHUTDOWN_DRAIN_TIMEOUT_MS`; `BATCH_MAX_QUEUE=0` = unbounded, breaker off, loud warning. `OverloadedError` moved to `errors.ts` and is re-exported from `micro-batcher.ts`, so old imports still work. Config in `services/ingest-api/src/config.ts`, vars in `services/ingest-api/.env.example`.

## 5.3 Step 1 filter and readiness mapped Kafka failures to 500 / ignored the breaker

**Problem.** `AllExceptionsFilter` returned 500 for `OverloadedError` and Kafka errors (the code said "S2-F5 will map this"). `/readyz` only checked a `connected` flag that never went false after the first connect.

**Fix.** `services/ingest-api/src/common/all-exceptions.filter.ts` returns 503 + `Retry-After` + `reason` (`overloaded` | `kafka_unavailable`) and counts it (`MetricsService.count503`, shown in `/debug/stats` as `http_503_by_reason`; the Prometheus series come with S2-F6). `services/ingest-api/src/health/health.module.ts`: `/readyz` is 200 only when connected **and** the breaker is closed; `/healthz` stays 200. `docs/api-contract.md` documents the 503 body and behavior.

## 5.4 Deviations from the plan text (kept)

- **Endpoint names.** The plan says `/health/ready` and `/health/live`; the code from step 1 uses `/healthz` and `/readyz` (and the shutdown e2e test, k6 and compose healthchecks already depend on them). Kept.
- **Breaker probe.** After the cooldown exactly one message is accepted as the probe and all others are still rejected until its batch settles. A non-availability error (for example a bad message) does not open the breaker.
- **Late sends can duplicate.** The send timeout (5 s) is shorter than librdkafka's `message.timeout.ms` (30 s), so a batch we answered 503 to may still be written later. Same flaw class as step 1's partial-batch note; the client retries with the same `event_id`, the sink absorbs it. Stays on the Stage 3 ledger.

## 5.5 Test hygiene

`test/events-kafka-down.e2e-spec.ts` **stops and starts the shared `ep-kafka1..3` containers** (about 3 minutes in total). `afterEach` always restarts all three and waits for `waitTopicReady`, so a failing test cannot leave the cluster down. Don't run it during a benchmark or while the dev consumer matters (the consumer will pause on broker errors and resume). It uses the throw-away topic from `test/global-setup.ts`, never `events.raw`. Its global-setup admin client prints librdkafka connection warnings while brokers are down; that is expected noise.

**Verified.** `pnpm test`: 72 unit tests pass (errors table, breaker, filter, batcher timeout/bytes/unbounded/breaker/drain, config). E2E: `events-kafka-down` 5/5 (all brokers down: 1,000 requests all 503 with `Retry-After`, fast after the breaker opens, readyz 503, healthz 200; recovery to 202 without restart; start API with Kafka down then bring Kafka up; 2 of 3 brokers down: no 202; 60 s flood at 500 req/s with Kafka down: heap flat, queue depth within `BATCH_MAX_QUEUE`), plus the earlier `events` and `shutdown` e2e still pass.

---

# Review of S2-F5 (2026-11-22): three gaps found and fixed

The S2-F5 build was reviewed against the plan and PRD. Unit and e2e tests were green, but three gaps showed up when they were probed with the real code. Each was confirmed **before** fixing it.

## 5.6 A request during the SIGTERM drain got 500, not 503

**Problem.** On SIGTERM, Nest runs `beforeApplicationShutdown` (the batcher drain, up to `SHUTDOWN_DRAIN_TIMEOUT_MS` = 10 s when Kafka is down) **before** it closes the HTTP server. Requests arriving in that window were rejected by the batcher with `BatcherClosedError`, which the filter didn't know, so the client got **500 without `Retry-After`** and the log an `[unhandled]` line. Confirmed with a probe: `status=500 retry-after=none`.

**Why it mattered.** `docs/api-contract.md` says 500 means a bug, and the retry guidance makes clients treat 500 as maybe-not-retryable. Every rolling deploy would hand out 500s for normal, retryable situations.

**Fix.** `src/common/all-exceptions.filter.ts`:
- `BatcherClosedError` → **503 + `Retry-After`, `reason: "shutting_down"`**, counted in `http_503_by_reason`;
- `MessageTooLargeError` (an event above the batch byte cap, only reachable if `BODY_LIMIT` is raised above ~900 KB) → **413**, like the edge limit, instead of 500.

**Verified.** Unit tests (filter: 503 `shutting_down` with no `[unhandled]` log; 413). New e2e in `test/events-kafka-down.e2e-spec.ts`: with Kafka down, request A hangs in flight, SIGTERM, request B arrives during the drain → **503 `shutting_down`, `Retry-After: 2`**; A is answered 503 by the send timeout; no `[unhandled]` in the log.

## 5.7 Readiness only changed when user traffic arrived

**Problem.** `/readyz` = producer connected AND breaker closed. The producer's `connected` flag never goes false while librdkafka keeps reconnecting, and the breaker only learned about Kafka from real requests. Measured with the real API and real brokers:

| situation | before | after the fix |
| --- | --- | --- |
| Kafka up, no traffic | 200 | 200 |
| Kafka down 20 s, **no traffic** | **200** (wrong) | 503 |
| Kafka down, after 4 requests | 503 | 503 |
| Kafka back 8 s, **no traffic** | **503** (wrong) | 200 |

**Why it mattered.** Behind a load balancer that only routes to ready instances, an instance would never come back after an outage: no traffic, so no breaker probe, so never ready, so no traffic. The plan's own definition of readiness had this flaw.

**Fix.** A background **Kafka health check** (`src/kafka/kafka-health.ts`, pure and unit-tested; `kafka-health.service.ts` runs it):
- Every `KAFKA_HEALTH_INTERVAL_MS` (default 2 s) it fetches the topic's metadata with its own admin client (bounded by `KAFKA_HEALTH_TIMEOUT_MS`). The check passes if every partition has a leader and at least `KAFKA_HEALTH_MIN_ISR` in-sync replicas: default **2 with `acks=all`** (what a write needs with `min.insync.replicas=2`), 1 with acks 0/1. So "2 of 3 brokers down" counts as unhealthy, not just "all down".
- **Check fails** → `breaker.forceOpen()`: requests fail fast immediately, without first waiting for send timeouts.
- **Check passes** → closes the breaker if Kafka just came back, or if its cooldown is over (the check is the half-open probe, so no user traffic is needed). It does *not* close a breaker that real send failures just opened while it is still cooling down; that would make it flap.
- `/readyz` = producer connected AND a passing check no older than 3 intervals + timeout AND breaker closed. `KAFKA_HEALTH_INTERVAL_MS=0` turns the check off (readiness falls back to the old rule). The last result is in `/debug/stats` as `kafka_health`. The admin client is rebuilt if it ends up disconnected (the same Confluent-client limitation as the producer in 5.1).

**Verified.** Unit tests for the decision (leader, ISR, acks 0/1), the check's breaker effects (down → open; back → closed without traffic; no flapping during cooldown), staleness, a hanging request treated as a failure after the timeout, and never throwing. New e2e: brokers stopped with **no requests** → `/readyz` 503 after **0.5 s**; brokers back, still no requests → 200 as soon as they were healthy, then 202. The real-process probe above went from wrong/wrong to right/right.

## 5.8 "Unbounded" mode wasn't unbounded, so K10 couldn't show its failure

**Problem.** `BATCH_MAX_QUEUE=0` removed the count and byte bounds and the breaker, but kept `KAFKA_SEND_TIMEOUT_MS` (default 5 s), which **also expires queued messages**. With Kafka hung, the queue was capped at about rate × timeout. Probe: 500 req/s for 60 s gave **queue depth 1,500 of 30,000**. K10's hypothesis ("an unbounded queue grows until the process dies") could never be observed, and K10 would have wrongly concluded that unbounded is fine. The existing unit test missed it because it built the batcher by hand, without the config's send timeout.

**Fix.** `src/config.ts`: `BATCH_MAX_QUEUE=0` now also forces `sendTimeoutMs = 0` (no send timeout, no queue expiry), even if `KAFKA_SEND_TIMEOUT_MS` is set. The startup warning and `.env.example` say so explicitly. Default (bounded) mode is unchanged.

**Verified.** New unit test builds the batcher **from the real config** with `BATCH_MAX_QUEUE=0` and a hung sender: after 60 s at 500 msg/s the queue holds **more than 29,000** (before: 1,500). Config test: `sendTimeoutMs` is 0 in unbounded mode, even with `KAFKA_SEND_TIMEOUT_MS=5000`.

## Totals after the fixes

`pnpm test`: **88** unit tests (was 72). `pnpm test:e2e`: **14** (was 12; `events`, `shutdown`, and `events-kafka-down` with the 2 new tests). Lint and typecheck clean.

Still accepted, unchanged (5.4): late sends can duplicate because librdkafka's `message.timeout.ms` (30 s default) is longer than the send timeout (5 s). Setting `KAFKA_DELIVERY_TIMEOUT_MS` to the send timeout shrinks that window; the Kafka-down tests do this (4 s).
