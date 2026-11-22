# Fixes for Step 1 (S2-F1: produce to `events.raw`, 202 only after Kafka acknowledges)

*Found in the review of 2026-11-22, fixed the same day. Each entry: the problem, why it mattered, the fix, how it was verified.*

---

## 1.1 The API's e2e tests wrote test events into the real `events.raw`

**Problem.** `services/ingest-api/test/events.e2e-spec.ts` produced to `events.raw`, the topic the real `sink-postgres` group reads. Every API test run therefore put test events (tenant `tenant_a`) into the real pipeline, and the next time the real consumer ran it stored them in the dev database.

**Why it mattered.** Same damage as [step-2.md](step-2.md) 2.1: test data mixed into benchmark data. It also makes the S2-F7 audit's "Kafka offsets" numbers include records no load test sent.

**Fix.**
1. The API's topic is configurable: `KAFKA_TOPIC` (default `events.raw`) in `src/config.ts`, used by `ProducerService`. Production behavior is unchanged.
2. New `test/global-setup.ts` (Vitest `globalSetup`): creates a throw-away 6-partition topic `ingest-e2e-<uuid>` once per run, waits until it is ready (see 1.4), exports it as `KAFKA_TOPIC` to the tests and to spawned API processes, and deletes it at the end.
3. `test/kafka-helpers.ts` reads that topic and **refuses** to run if `KAFKA_TOPIC` is unset or is `events.raw`.

**Verified.** `pnpm test:e2e` passes, and no `ingest-e2e-*` topic is left behind afterwards.

---

## 1.2 No automated test for "SIGTERM with a non-empty queue flushes before exit"

**Problem.** The plan's step 1 "what to cover" includes graceful shutdown with queued requests. It was only checked once by hand (50 queued requests, all 202), and no test protected it.

**Fix.** New `test/shutdown.e2e-spec.ts` runs the **built** API (`dist/main.js`) as a child process with `BATCH_MAX_WAIT_MS=2000`, so requests sit in the batcher. It sends 50 requests, sends **SIGTERM** 400 ms later (before the 2 s flush timer), and requires:
- all 50 answered **202**,
- the process gone within 1.5 s (drained at once, didn't wait for the timer),
- the topic's end offsets grown by exactly **50**.

`pretest:e2e` now runs the full `pnpm build` so `dist/` is current.

**Verified.** Passes.

---

## 1.3 Small differences from the plan

- **`CLUSTER_ID` was a hard-coded default in the compose file.** The plan says to keep it in `infra/compose/.env`. It's now in `infra/compose/.env` (`KAFKA_CLUSTER_ID`, the same value the broker volumes were formatted with). `docker-compose.kafka.yml` requires it (`${KAFKA_CLUSTER_ID:?...}`, the same style as the Postgres variables), and `.env.example` explains how to generate one. Checked: compose resolves it from `.env`, and fails with a clear message when it's missing. (A first attempt put a `:` inside the error text, which broke the YAML; the value is now quoted.)
- **Listener notes location.** They're in `notebook/stage-2/learning/Kafka/listeners.md` (your notebook layout) instead of the plan's `notebook/kafka-notes.md`. The plan was updated to the real path.
- **Leftover `perf` topic.** The `kafka-producer-perf-test.sh` smoke run left a `perf` topic in the cluster; it's deleted. Recording the **raw broker ceiling** is not done on purpose: it's experiment K1's job (it needs the full K1 method: same hardware, recorded CPU, several runs), and a one-off smoke number would be misleading.

---

## 1.4 Acknowledged records missing from the log (producer side)

**Problem.** First seen in one step 3 crash-test run: a topic held 4,999 records although the producer had been told 5,000 were acknowledged (`acks=all`, idempotent producer, `min.insync.replicas=2`, RF 3). If this could happen on the API's path, a client could get a **202 for an event that is not in the log**, which breaks the one promise S2-F1 makes.

**Investigation.** A standalone diagnostic (`produce-diag.mjs`): create a 6-partition RF 3 topic, produce 5,000 sequenced messages in batches of 500 with the same producer settings as the API, compare acknowledged vs log end offsets, and on a mismatch re-check after 3 s and read the whole topic back to list the missing sequence numbers.

| Condition (5,000 msgs per run, batches of 500) | Runs | Runs with records missing | Details |
| --- | --- | --- | --- |
| step 3 crash tests (produce right after `createTopics`) | ~21 | 1 | 4,999 / 5,000 in the log |
| idempotent, produce right after `createTopics` | 60 | 1 | 4,998 / 5,000 |
| idempotent, produce right after `createTopics` (diagnostic with read-back) | 80 | 1 | 4,998 / 5,000, **still 4,998 after 3 s**, read back 4,998, missing seq **2 and 11**: both in the **first batch** |
| idempotent, **3 s wait** after `createTopics` | 80 | 0 | |
| **non-idempotent**, produce right after `createTopics` | 100 | 0 | |
| idempotent, `waitTopicReady` (leaders + full ISR + 2 s) | READY_RUNS | READY_MISMATCHES | READY_DETAILS |

What the data says:
- It is **real loss, not a measurement artifact**: the records were missing after 3 s and when the topic was read back.
- It only happened when producing **into a topic created moments before**, and the lost records were among the **very first** sent to it (sequence numbers 2 and 11 out of 5,000).
- It happened only with the **idempotent** producer (3 cases in about 160 idempotent runs without a wait, 0 in 100 non-idempotent runs).
- Waiting after creation made it disappear in the runs made (0 in 80).

**Fix / mitigation.**
- Production: `events.raw` and `events.dlq` are created by `kafka-init` before any client starts, and nothing in the app creates topics (`auto.create.topics.enable=false`, `allowAutoTopicCreation: false`). The window in which records were lost doesn't exist on the API's path.
- Tests (where topics *are* created on the fly): new `waitTopicReady()` in `packages/kafka-config`. It waits until every partition has a leader and a full ISR, plus a 2 s settle, before anything is produced. Used by consumer-sink's `createTopic`/`createTopicNamed` and by ingest-api's `global-setup.ts`.
- Detection stays: consumer-sink's `produceEvents` test helper still fails with `PRODUCE CHECK` if the log ever holds fewer records than were acknowledged, so a recurrence shows up as a producer problem and not as a fake consumer bug.
- For the Stage 2 failure ledger: the S2-F7 audit's check C (Kafka end offsets ≥ acknowledged 202s) is the production guard for exactly this.

**Root cause: not proven.** The evidence points at the idempotent producer's handling of the first produce requests to a partition whose leader and ISR are still being set up (librdkafka first gets errors such as `UNKNOWN_TOPIC_OR_PART`, retries, and an early message ends up acknowledged without being in the final log). Confirming it needs librdkafka debug logs (`debug=msg,eos,protocol`) and broker logs around topic creation. That's a good investigation for the failure ledger, but outside this step.

**Risk to watch in K4 (kill a broker):** a leader change is also a "first writes to a new leader" moment. If this mechanism is related, K4 could show acknowledged-but-missing records on `events.raw`. K4's audit (check C, acknowledged vs Kafka offsets) must be read with this in mind, and this entry should go into `docs/failure-ledger-stage-2.md`.
