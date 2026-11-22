# K2 Experiment explanations

Status: PRE-RUN. Written 2026-11-22 before any measured run. The sections after the marker are appended after the experiment.

## Purpose

I want to know what each producer setting costs and buys: batch wait and size, acks, idempotence and compression, with
data from my own API and brokers instead of rules of thumb. K1 showed where the ingest knee is; K2 asks which dials move
latency, CPU per request and bytes on the wire, and in which direction. Once I have the numbers I can choose the producer
settings for the rest of Stage 2 on purpose and say what I give up (latency or durability) for each gain.

## Hypothesis

The stage document says bigger batches and compression raise throughput and p50 latency slightly, and `acks=all` costs
latency against `acks=1`. My reasoning: a batch has a fixed cost per send, so more messages per batch means less CPU per
message, but the first message waits longer for the batch to fill. Compression trades client CPU for fewer bytes on the
wire and on the broker disk. `acks=all` waits for the followers, `acks=1` only for the leader, `acks=0` for nothing.

## Predictions (written before the run)

Offered rate is 4,000 req/s, payload about 500 B (JSON value about 496 B), baseline `acks=all`, idempotent, no compression,
wait 5 ms, 500 messages, one batch in flight. K1 at 2,500 req/s had p50 16.7 ms and p99 97 ms; at 1,000 req/s p50 7.5 ms.

1. **Batch size follows the wait, not the size cap.** Messages per batch about rate x (time a batch collects). With one
   batch in flight that time is max(wait, previous send time of about 4 ms): wait 1 ms gives 4,000 x 0.004 = 16 (range 8
   to 25), wait 5 ms gives 4,000 x 0.005 = 20 (range 20 to 40), wait 20 ms gives 4,000 x 0.020 = 80 (range 70 to 100).
   The cap of 50 only binds at wait 20 ms (batches of 50, flush reason `size`). Caps 500 and 5000 give the same results
   (within about 5%), because neither is reached.
2. **p50 latency rises with the wait.** Request p50 = HTTP and validation about 1 ms + average wait in the queue about
   wait/2 (plus time blocked behind the previous send) + send time about 4 ms. I predict p50 of about 10 ms at wait 1 ms,
   about 15 ms at 5 ms (K1 gives the anchor) and about 28 ms at 20 ms. p99 follows, 2x to 4x the p50.
3. **CPU per request falls only a little with bigger batches.** If the total cost is about 125 microseconds per request
   (K1 guess) and a send costs about 150 microseconds, then 16 messages per batch put about 9 microseconds on each request
   and 80 messages about 2: a gain of about 7 microseconds, 6% of 125. Accepted rate is 4,000 in every config, so this
   shows only in CPU per request, not in rps. I do not expect a visible gain in rps at this rate.
4. **acks costs latency in steps.** Send time p50: acks=0 about 0.1 ms (no broker wait), acks=1 about 1.5 ms (one round
   trip plus the leader's write), acks=all about 4 ms (the followers have to fetch the record, which adds a second round
   trip). So request p50 for acks=1 is about 2.5 ms below the baseline and acks=0 about 4 ms below. The batch size under
   acks=0 is rate x wait = 20, not bigger, because the in-flight limit is never felt.
5. **Idempotence on or off with acks=all changes nothing I can see** (within 5% on p50 and CPU): the batcher already
   allows only one batch in flight, and the sequence numbers cost almost nothing.
6. **Compression, wire size.** Uncompressed wire bytes per record about 560 (496 B value + record and batch framing
   about 60 B at 20 messages per batch). With the random base64 filler (300 of the 496 bytes): gzip and zstd can get the
   filler to 6/8 = 225 B and the repeated JSON keys to almost nothing, so about 260 B per record plus framing: about 320 B
   (57% of none). lz4 and snappy cannot entropy-code base64, so the filler stays 300 B: about 450 B (80%). Disk bytes per
   record on the broker follow the wire bytes (same ratios).
7. **Compression, cost.** Compression runs in librdkafka threads, not in the event loop. gzip at about 40 MB/s per core
   is 500 B / 40 MB/s = 12 microseconds per record, so +10% CPU per request over a baseline of 125 microseconds; zstd about
   +4%, lz4 and snappy about +2%. Latency: a 20-message batch is 10 KB, gzip adds about 0.25 ms to send time, so p50
   rises by less than 1 ms for every codec. Compression will not change the accepted rate.
8. **Broker cores stay quiet.** 4,000 x 560 B = 2.2 MB/s from the producer, x3 with replication = 6.7 MB/s; the broker
   CPUs (4 logical CPUs for three brokers) should stay under 15% busy. Compression lowers disk bytes written in line with
   the ratio of item 6.
9. **What I expect NOT to change:** audit equal in every run (202 answers = records in the topic), no 503, dropped
   iterations 0. I am less sure about the dropped iterations: K1 had none at 2,500 req/s and some at 5,000, and 4,000 sits
   between. If the baseline drops iterations, the rate is too high for this sweep and I lower it before the real run.

## Setup (planned)

| Item              | Value |
| ----------------- | ----- |
| Server            | `services/ingest-api` (NestJS, `@confluentinc/kafka-javascript`), `node --env-file=.env dist/main.js` under `taskset -c 0,6`, restarted before every run with the config's knobs written out in full |
| Brokers           | 3 x `apache/kafka:4.3.2`, KRaft, 1 CPU quota and 1 GiB each, pinned to CPUs 1,7,2,8; topic `events.raw`, 6 partitions, RF 3, `min.insync.replicas=2`, recreated before every run |
| Route / generator | `POST /v1/events`, k6 `constant-arrival-rate` at 4,000 req/s, 30 s, pinned to CPUs 3,9,4,10; random base64 filler of 300 chars from a pool of 1024 strings (fixed seed) |
| Matrix            | 16 configs (baseline, 8 batcher grid configs, 3 durability configs, 4 compression configs) x 1 rate x 2 repeats x 30 s; repeat 2 runs the configs in reverse order; no probe runs; about 48 min |
| Dashboard         | `infra/observability/dashboards/stage-2/K2 - Producer settings sweep.json` |
| Versions          | filled in after the run from env.txt |

## Method (planned)

1. `./run.sh` runs the 32 measured runs. For each: stop the server, recreate the topic, start the server with the config,
   100 warm-up requests, 10 s settle, record the server counters, run k6 for 30 s, wait 6 s, record the counters again
   and the topic size, write one row to `summary.tsv`.
2. A run is valid if dropped iterations are 0, the generator CPUs stay under 70%, there are no 503 or other non-202
   answers, and the audit is `ok`. The comparison of configs uses the mean of the 2 repeats; where the two repeats
   differ by more than 25% on a number I treat that number as noise.
3. Discard a run and repeat it if it has dropped iterations or a failed audit, or if something else ran on the machine.
   These criteria are fixed now.
4. A trial run (`-trial` results name) is only a check that the script works and is never quoted.
5. Compression numbers come from the random filler only; the `x` filler run, if I do it, is reported separately as the
   unrealistic best case.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
