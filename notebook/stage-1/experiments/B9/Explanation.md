# B9 Experiment explanations

Status: PRE-RUN. Written 2026-11-21 before any measured run. The sections after the marker are appended after the experiment.

## Purpose

I want to know if clients that retry at once when a request fails can keep an overloaded API down after the original problem is gone. B3 showed that a slow database collapses the API; B9 asks whether the collapse can outlive its cause. Once I know the answer I can explain metastable failure with my own numbers, and I can say which of the later protections (backoff, rate limiting, load shedding, retry budgets) address which part of the problem.

## Hypothesis

If every failed request is retried up to 3 times without waiting, a short database slowdown makes the offered load rise to about 4 times the original rate. After the slowdown ends the load is still above capacity, the server's queue is full of requests whose clients have already given up, and goodput stays near zero. The system stays collapsed until the offered load is reduced from outside. Backoff and jitter should change how fast it recovers, and I do not yet know by how much.

## Predictions (written before the run)

Numbers come from B2 and B3 (hold time per request h0 about 5 ms) and the settings of this experiment (pool 5, base rate 600 new events/s, toxic +50 ms for 20 s, client timeout 1 s, server pool wait 2 s).

1. **Healthy part.** Capacity = pool / h0 = 5 / 0.005 s = 1000 req/s. 600 new events/s is 60 % of it, so no errors and `amplification` is 1.00 for every variant before the trigger. The four variants must look the same here.
2. **Slow part.** Capacity = 5 / (0.005 + 0.050) s = 91 req/s. 600 / 91 is 6.6 times too much, so about 85 % of requests cannot be served. Their wait in the pool queue passes the client timeout of 1 s, so the client sees status 0 and the server still holds the request until its own 2 s limit.
3. **Offered load with retries.** An event that fails all 4 tries sends 4 requests, so at most 4 x 600 = 2400 req/s. With about 85 % failing I expect `peak_offered_x` between 3.0 and 3.9 for `immediate`. For `none` it stays at 1.0.
4. **`none` recovers by itself.** When the toxic is removed, the queue holds at most 600 x 2 s = 1200 requests and they drain at 1000 - 600 = 400 req/s net, about 3 s. I expect `recovery_s` of about 3 to 10 s (the metric requires 10 s of good behaviour in a row and reports where that stretch starts).
5. **`immediate` does not recover by itself.** After removal the capacity is 1000 req/s, but the offered load is still about 2400 req/s (retries of events that keep failing). The queue stays full at the 2 s server limit, so a request that is finally served has waited longer than the 1 s client timeout and nobody reads its answer. Goodput stays near zero and attempts stay near 2400. I expect `recovery_s = never` and `tail_ok_pct` below 20 %. I am not sure of this one: if the queue drains faster than I think, recovery could take 10 to 30 s. `stale_rows` should be large (the database commits events the client gave up on).
6. **Backoff does not fix it with these settings.** The waits (100, 200, 400 ms) are small next to the 1 s timeouts, and the number of attempts per event is still 4. Delaying a retry does not remove it, so the long-run attempts per second stay close to 600 x 4. I expect `backoff` and `backoff_nojitter` to have `peak_offered_x` within 10 % of `immediate` and to stay collapsed too, or recover only after much longer than `none`. Jitter should spread the retries out in time but not lower their number. I put about 40 % on one of them recovering inside the 120 s.
7. **Wasted work.** In the stuck state Postgres keeps committing (capacity about 1000 commits/s, if the server does not cancel the query of a client that left) while the 202 rate the client sees is near zero. I did not verify that the server keeps working on abandoned requests; the commits vs 202 panel will tell.
8. **Relief probe.** When the generator drops to 60 new events/s, the offered load becomes at most 240 req/s, below capacity. I expect the queue to drain and success to return within about 5 to 15 s after the drop.
9. **Generator.** An iteration that retries holds its virtual user for up to about 4.7 s, so about 600 x 4.7 = 2800 VUs, below the maximum of 8000. I expect `dropped_iterations = 0`.

What I expect NOT to change: the healthy part; the `none` variant (it never retries, so it cannot amplify).

## Setup (planned)

| Item              | Value                                                                                                                                       |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Server            | `node --env-file=.env dist/main.js` under `taskset -c 0,6`, started by `run.sh`; `PG_POOL_MAX=5`, `PG_CONN_TIMEOUT_MS=2000`, `PG_STATEMENT_TIMEOUT_MS=5000` |
| Database          | Postgres in Docker through Toxiproxy (latency toxic, downstream, +50 ms, jitter 0)                                                          |
| Sysctls           | none changed                                                                                                                                |
| Route / generator | `POST /v1/events`; k6 `constant-arrival-rate` 600 new events/s, 1 s attempt timeout, up to 3 retries; k6 pinned to `3,9,4,10`              |
| Matrix            | 4 variants x 3 repeats x 170 s (30 s healthy, 20 s slow, 120 s after), plus 1 relief probe; about 50 minutes                                |
| Dashboard         | [`B9 - Retry storm.json`](../../../../infra/observability/dashboards/stage-1/B9%20-%20Retry%20storm.json)                                    |
| Versions          | filled in after the run from env.txt                                                                                                        |

## Method (planned)

1. For every repeat and variant: restart the server, warm up, check once that the toxic slows a request (then remove it), empty the table, start k6, wait until traffic flows, add the toxic after 30 s, delete it 20 s later, let k6 run to 170 s.
2. Measured runs are the 12 variant runs. The relief probe is separate and is not used for headline numbers.
3. Per run `analyze.py` writes a summary row. Recovery time, peak offered load and the share of 202 answers in the last 20 s come from Prometheus time series of that run (definitions in `hook.py`).
4. A run is discarded if `dropped_iterations` is above 0, if the k6 CPUs are above about 70 % busy, if the toxic add or removal time is missing from `events.tsv`, or if the self-check showed the toxic did not slow the server. Decided now so I cannot change it after seeing data.
5. A variant is called recovered only if `recovery_s` is a number in at least 2 of the 3 repeats; otherwise I report the spread and say it was not consistent.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
