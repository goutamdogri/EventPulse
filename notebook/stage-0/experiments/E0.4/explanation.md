# E0.4 Worker threads fix

## Part 1: Isolation test (WORKERS=1)

### Hypothesis

Moving the CPU work to a worker thread keeps `/health` flat, because the main
thread only does `postMessage` and the callback. `/cpu-worker` throughput is
bounded by worker count: capacity = W / job_time.

### Predictions

Recorded baseline job time ≈ 0.35 s.

- `/health` p99 and max stay at baseline (no ~350 ms plateau as in E0.3).
- Event-loop lag stays at baseline.
- Capacity with 1 worker ≈ 2.86 rps.
- Rate 1 (35%): no queueing. Rate 2 (70%): some queueing. Rate 3 (105%): queue grows.

### Setup

    Date 2026-10-29,
    commit 73e068d0f5d4a3f566e92eb9be6e7d39f120e27c,
    Operating System: Ubuntu 24.04.5 LTS
    Kernel: Linux 7.0.0-38-generic
    AC power,
    Node version: v24.21.0
    Server:
        `taskset -c 0,6`,
        WORKERS=1,
        port 8000.
        CPUs 0 and 6: same physical core
    k6: pinned to CPUs 1,2,3,7,8,9.
    Prometheus scrape 1s, k6 RW push 1s.
    Validity:
        dropped_iterations = 0,
        generator CPU = <13.8%,
        repeats = 1.

### Method

1. Calibration (idle, N=38, 5 runs each) with `baseline-observer.sh`.
2. `SCRIPT=E0.4/isolation-test.js ./runner.sh` with CPU_RATE = 1, 2, 3.
   /health constant 200 rps for <120 s>; /cpu-worker starts at 30 s for <60 s>.
   See `loadtests/k6/stage-0/E0.4/README.md`.

### Results

Calibration, N=38 (s):

| route | runs | mean |
| --- | --- | --- |
| /cpu | 0.346, 0.341, 0.345, 0.346, 0.339 | 0.343 |
| /cpu-worker | 0.349, 0.345, 0.349, 0.366, 0.338 | 0.349 |

Thread-hop overhead ≈ 6 ms (≈1.7%), close to the run-to-run noise.

Per-window values over the CPU phase (median / worst window):

| CPU rate | /health p50 | /health p99 | /health p99.9 | loop lag excess max |
| --- | --- | --- | --- | --- |
| 1 rps | 151 µs | 375 µs / 1.94 ms | 2.40 ms / 4.81 ms | 4.44 ms |
| 2 rps | 148 µs | 376 µs / 755 µs | 1.10 ms / 4.81 ms | 4.93 ms |
| 3 rps | 93.5 µs | 316 µs / 1.27 ms | 1.90 ms / 6.24 ms | 5.97 ms |

| CPU rate | /cpu-worker p50 | p99 | p99.9 | worst p99.9 |
| --- | --- | --- | --- | --- |
| 1 rps | 361 ms | 385 ms | 386 ms | 420 ms |
| 2 rps | 339 ms | 353 ms | 354 ms | 385 ms |
| 3 rps | 1.92 s | 2.17 s | 2.18 s | 3.67 s |

Note: medians of per-window percentiles are not true whole-run percentiles.
Screenshots: <latency panel, loop lag panel, per-CPU panel>.

### Observations

- /health stayed in the microsecond range at all three rates, including overload.
  In E0.3 the same load put /health p99/max at ~364 ms.
- Event-loop lag excess stayed under 6 ms (E0.3: ~370 ms).
- Rates 1 and 2: /cpu-worker latency ≈ job time, no queueing.
- Rate 3: latency climbed from 0.4 s to ~3.6 s over the window, queueing happens.

### Explanation

Isolation: the CPU work runs on another thread with its own V8 isolate, so the
main loop only pays for message passing. The queue now lives in the worker
pool, not in the event loop, so queue growth does not hurt /health.

Capacity: one worker serves 1 / 0.349 ≈ 2.86 rps. At 3 rps the backlog grows by
(3 − 2.86) = 0.14 jobs/s, adding 0.14 / 2.86 ≈ 0.049 s of wait per second.
Over 60 s: 0.35 + 2.9 ≈ 3.3 s predicted peak vs 3.5–3.67 s measured. Median
1.92 s fits linear growth from 0.35 s to ~3.6 s.

Little's Law check at rate 3: in-flight ≈ rate × latency ≈ 3 × ~2 s ≈ 6 requests
on average (compare with k6_vus).

### Where I was wrong

- I predicted queueing at 70% utilisation. None appeared (p99 353 ms). k6's
  constant-arrival-rate spaces arrivals evenly (500 ms apart vs 349 ms jobs), so
  a job always finishes before the next arrives. Real bursty traffic would
  queue at this utilisation. This test shows a cliff at ρ = 1 that real
  traffic would not.
- /cpu-worker p50 was 339 ms at rate 2 but 361 ms at rate 1, lower than the idle
  calibration. Unexplained: JIT warm-up, CPU frequency or noise. Needs repeats.

### Cost

- Isolation gained: /health p99 ≈ 375 µs instead of ~345 ms.
- Overhead paid: ≈ 6 ms per job, a hard cap of W / job_time (2.86 rps for 1 worker).
- Overload is invisible to /health but not to /cpu-worker callers: latency grew
  without bound (~0.05 s per second). Memory growth was not measured here
  (see Part 3, unbounded queue).

### Follow-ups

- Repeat 3× per rate, same CPU rate sweep with WORKERS=2 and core-count.
- Capacity step test, `n=10` overhead test, unbounded-queue memory test.
- Add worker_pool_queue_length / wait / run metrics.
- Try jittered (Poisson-like) arrivals to see queueing below ρ = 1.
