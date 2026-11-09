# E0.4 Steps followed

## Isolation test

1. Run `baseline-observer.sh` to record time taken by \cpu and \cpu-worker.
2. `taskset -c 0,6 node labs/stage-0-foundations/src/server.js` - start the server by pinning the cpu.
3. `SCRIPT=E0.4/isolation-test.js ./runner.sh` - test script. Default CPU_RATE is 1
    - repeat with CPU_RATE=2 and CPU_RATE=3

## Prerequisites for parts 2 to 4

1. Add the pool metrics and the `WORKERS` / `MAX_QUEUE` env vars to `labs/stage-0-foundations/src/worker-pool.js`
   (see `reference/worker-pool.instrumented.example.js`), and the optional `pad` query param to `/cpu-worker`.
2. Make sure `/metrics` exposes `worker_pool_*`, `worker_job_*`, plus the default prom-client memory metrics
   (`process_resident_memory_bytes`, `nodejs_heap_size_used_bytes`, `nodejs_external_memory_bytes`).
3. Confirm the Prometheus scrape interval for the server is 1s.
4. Confirm CPUs 0 and 6 are SMT siblings: `lscpu -e` (record it in the notebook).
5. All commands below run from `loadtests/k6/stage-0/` unless noted. `LABEL` is the run-label env var used by `runner.sh`
   to build file names and `TESTID`. If your runner does not read it yet, add it first.

Common record for every run: commit hash, `WORKERS`, `MAX_QUEUE`, server pin (`0,6`), k6 pin (`1,2,3,7,8,9`),
`dropped_iterations`, number of repeats.

## Part 2: Capacity test (Experiment B)

Repeat the whole block for `WORKERS` = 1, 2 and 4 (4 on 2 logical CPUs is the deliberate oversubscription run).

1. Start the server with a fresh process:
   `WORKERS=<W> taskset -c 0,6 node labs/stage-0-foundations/src/server.js`
2. Calibrate: `WORKERS=<W> ./E0.4/capacity-calc.sh` -> note `JOB_TIME` and `C_theoretical`.
3. Smoke test (about 90 s): `STEP=15 GAP=5 FRACTIONS=0.5,1.5 REPS=1 LABEL=e04_B_smoke WORKERS=<W> JOB_TIME=<t> SCRIPT=E0.4/capacity-test.js ./runner.sh`
   Check Grafana: `load_pct` label exists, `route="cpu_worker"` and `route="health"` series appear.
4. Real run (about 7 min per repeat; steps 50/80/95/110/150 % of C, 60 s each, 15 s gap):
   `REPS=3 LABEL=e04_B_w<W> WORKERS=<W> JOB_TIME=<t> SCRIPT=E0.4/capacity-test.js ./runner.sh`
5. During repeat 1, in a second terminal: `./E0.4/sample-threads.sh e04_B_w<W> 420`
   (per-thread CPU: worker thread near 100 %, main thread low).
6. Restart the server before the next `WORKERS` value.

Validity: `dropped_iterations` must be 0 in every step (maxVUs is 1000; the 150 % step holds about 90 in flight for W=1).

## Part 3: Message-overhead sub-test

Server: `WORKERS=1 taskset -c 0,6 node labs/stage-0-foundations/src/server.js` (one start covers all three runs).

1. Generator and HTTP floor: `LABEL=e04_ovh_health TARGET_ROUTE=/health SCRIPT=E0.4/overhead-test.js ./runner.sh`
2. In-thread: `LABEL=e04_ovh_cpu TARGET_ROUTE=/cpu SCRIPT=E0.4/overhead-test.js ./runner.sh`
3. Through the worker: `LABEL=e04_ovh_worker TARGET_ROUTE=/cpu-worker SCRIPT=E0.4/overhead-test.js ./runner.sh`
4. Default steps: 500, 1000, 2000, 3000, 4000, 6000, 8000 rps, 30 s each. If route 1 is still healthy at 8000, raise
   `RATES=...` until it is not. The generator must never be the limit, so check `dropped_iterations` and generator CPU on the `/health` run first.
5. Repeat each 3 times (`REPS=3`). Sample threads once: `./E0.4/sample-threads.sh e04_ovh_worker 300`.

Knee definition (write it in the notebook): the highest offered rate where achieved rps >= 98 % of offered, `dropped_iterations` = 0
and p99 is under 5x its value at the lowest step.

## Part 4: Unbounded queue (Experiment C)

Use `WORKERS=1`. Restart the server before each run so heap, histograms and counters start clean.
First calibrate: `WORKERS=1 ./E0.4/capacity-calc.sh`.

Smoke test (about 70 s): `BASELINE=10 DURATION=20 RECOVER=40 LOAD=1.5 PAD_KB=100 REPS=1 LABEL=e04_C_smoke WORKERS=1 JOB_TIME=<t> SCRIPT=E0.4/overload-test.js ./runner.sh`

### C1: unbounded queue, memory visible

1. `WORKERS=1 taskset -c 0,6 node labs/stage-0-foundations/src/server.js`   (MAX_QUEUE unset = unbounded)
2. `LOAD=1.5 DURATION=300 PAD_KB=100 REPS=1 LABEL=e04_C1_unbounded WORKERS=1 JOB_TIME=<t> SCRIPT=E0.4/overload-test.js ./runner.sh`
   (30 s baseline + 300 s overload + 200 s recovery, about 9 min)
3. Second terminal: `./E0.4/sample-threads.sh e04_C1_unbounded 540`
4. Optional: repeat with `PAD_KB=0` to show how much of the memory growth is the queued payload.

### C2: wasted work (clients give up, server keeps working)

1. Restart the server (unbounded).
2. `TIMEOUT=10s LOAD=1.5 DURATION=300 PAD_KB=100 REPS=1 LABEL=e04_C2_timeout WORKERS=1 JOB_TIME=<t> SCRIPT=E0.4/overload-test.js ./runner.sh`
   (`TIMEOUT` is the client timeout, so k6 abandons requests after 10 s while the server still runs them.)
3. Compare server `worker_pool_jobs_total{outcome="completed"}` with k6 `status="200"` requests.

### C3: bounded queue (the fix)

1. Restart with `MAX_QUEUE=10 WORKERS=1 taskset -c 0,6 node labs/stage-0-foundations/src/server.js`
2. `LOAD=1.5 DURATION=300 PAD_KB=100 REPS=1 LABEL=e04_C3_bounded WORKERS=1 JOB_TIME=<t> SCRIPT=E0.4/overload-test.js ./runner.sh`
3. Optionally repeat with `MAX_QUEUE=3` and `MAX_QUEUE=50` to see the latency vs shed-rate trade-off.

## Panels (add to the dashboard; use `$testid` everywhere)

| Panel | Query |
| --- | --- |
| `/health` p50, p99, max | `histogram_quantile(0.99, sum(rate(k6_http_req_duration_seconds{testid=~"$testid", route="health"}[$__rate_interval])))` and `k6_http_req_duration_seconds_max{...route="health"}` |
| `/cpu-worker` latency by step (B) | `histogram_quantile(0.99, sum by (load_pct) (rate(k6_http_req_duration_seconds{testid=~"$testid", route="cpu_worker"}[$__rate_interval])))` |
| Latency of successful requests (C) | same with `status="200"` added |
| Throughput: completed jobs/s | `rate(worker_pool_jobs_total{outcome="completed"}[$__rate_interval])` (draw a constant line at C) |
| Goodput: successful responses/s | `sum(rate(k6_http_reqs_total{testid=~"$testid", route="cpu_worker", status="200"}[$__rate_interval]))` |
| Shed and timed-out | same with `status="503"` and `status="0"` |
| Queue length, busy workers | `worker_pool_queue_length`, `worker_pool_busy_workers` |
| Wait vs run p99 | `histogram_quantile(0.99, sum by (le) (rate(worker_job_wait_seconds_bucket[$__rate_interval])))` and the same for `worker_job_run_seconds_bucket` |
| In flight and drops | `k6_vus{testid=~"$testid"}`, `sum(rate(k6_dropped_iterations_total{testid=~"$testid"}[$__rate_interval]))` |
| Event-loop lag | your existing panel (max and p99, unsubtracted copy too) |
| Per-CPU busy | `rate(node_cpu_seconds_total{mode!="idle", cpu=~"0\|6"}[$__rate_interval])` |
| Memory | `process_resident_memory_bytes`, `nodejs_heap_size_used_bytes`, `nodejs_external_memory_bytes` |
| Part 3 achieved vs offered | `sum by (offered_rps) (rate(k6_http_reqs_total{testid=~"$testid"}[$__rate_interval]))` |

Note: when a step overloads the pool, completions keep arriving after the step ends and k6 still attributes them to that step's tag.
Use the server-side `worker_pool_jobs_total` rate for true throughput over time.
