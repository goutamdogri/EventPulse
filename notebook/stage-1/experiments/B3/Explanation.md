# B3 Experiment explanations

Status: COMPLETE. Results added 2026-11-20.

## Purpose

I want to know how much of a slow database the API passes on to its users: how much the latency of a request grows, and how much the number of requests per second shrinks, when every query takes longer. It matters now because the ingest API does one synchronous database write per request, so the database is a dependency it cannot get around. Once I know the rule I can predict capacity from the pool size and the query time, and I can explain why an API is never faster than its slowest synchronous dependency.

## Hypothesis

Adding latency L to every query raises the API latency one-for-one (by L), and the capacity falls to about pool_size / (h0 + L), where h0 is the hold time without added latency. The mechanism: the request keeps its pool connection while it waits for the database, so a longer wait means fewer requests per second per connection. Postgres itself is not busier, the connections are just held longer.

## Predictions (written before the run)

Constants: pool = 25. h0 = about 5 ms (B2: 4.5 ms when not saturated, 5.4 ms when saturated), so H = h0 + L.

1. **Capacity (accepted 202/s in an `over` run) = 25 / (h0 + L):**
   - 0 ms: 25 / 0.005 = 5,000 req/s
   - 20 ms: 25 / 0.0250 = 1,000 req/s (range 985 to 1,020 for h0 4.5 to 5.4 ms).
   - 50 ms: 25 / 0.055 = 455 req/s (range 440 to 460).
   - 200 ms: 25 / 0.205 = 122 req/s (range 120 to 123).
2. **Latency in an `under` run is one-for-one.** p50 is about h0 + L + 1 to 3 ms of API work. The slope of p50 against L is 1.0, between 0.9 and 1.1. The server's `ep_pg_query_duration_seconds` p50 moves by about L too.
3. **Little's Law in an `under` run.** In flight = rate x mean latency = about 12 at every level (2,000 x 0.006 = 12 at 0 ms is only approximate, since the API overhead is a large part of the 6 ms there; 500 x 0.025 = 12.5; 225 x 0.055 = 12.4; 60 x 0.205 = 12.3). Pool in use below 25 and waiting about 0, no errors.
4. **Little's Law in an `over` run.** In flight is about 25 in the pool plus the waiting queue. The queue is limited by the 2 s pool timeout to about capacity x 2 s: about 2,000 at 20 ms, 900 at 50 ms, 250 at 200 ms. The excess (offered minus capacity: about 200, 95, 28 req/s) gets 503 `pool_timeout` after about 2 s, so the p50 in `over` runs sits at about 2 s (as pool 5 did in B2: 2,003 ms). The 3,000 k6 virtual users should be enough, but 20 ms is the closest to that limit.
5. **Postgres is not the bottleneck.** At 50 and 200 ms the Postgres container CPU and commits per second fall with the accepted rate, and most connections are `idle` in `pg_stat_activity`.
6. **Recovery.** After the toxic is deleted, the capacity jumps to well above the offered rate (25 / 5 ms = 5,000 against 300 or 600 req/s). The backlog is at most about 250 (A) or 900 (B) requests, and it drains in under a second. p99 should be back near the level-0 value within 0.5 to 3 s of the removal, and never later than the 2 s timeout plus about 1 s of k6 resolution. The 503s stop within about 2 s.
7. **The proxy costs little.** The control probe (no proxy, 2,000 req/s, 45 s: the same rate and duration as the level-0 `under` runs) should have p50 within 1 ms of the level-0 `under` runs through the proxy, and the same accepted rate.
8. **What should not change:** the number of errors of other types (no `too_many_clients`, no statement timeout), because 200 ms is far below the 5 s statement timeout and 25 connections are far below Postgres' 100.

## Setup (planned)

| Item              | Value                                                                                                                                                                                                                                                                                                                                                                                                                             |
| ----------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Server            | ingest-api (NestJS), `node --env-file=.env dist/main.js`, `PG_POOL_MAX=25`, `taskset -c 0,6`, restarted before every run. Connects to `localhost:15434` (Toxiproxy). No server code change for B3.                                                                                                                                                                                                                                |
| Sysctls           | none changed                                                                                                                                                                                                                                                                                                                                                                                                                      |
| Route / generator | `POST /v1/events`, k6 `constant-arrival-rate`, 500 pre-allocated and up to 3,000 VUs, `taskset` on `3,9,4,10`                                                                                                                                                                                                                                                                                                                     |
| Fault injection   | Toxiproxy 2.13.0 in a container (`ep-toxiproxy`, cpuset `5,11`), proxy `pg` from `:15434` to `postgres:5432`, toxic `latency` on the downstream direction, jitter 0, added after the warm-up                                                                                                                                                                                                                                      |
| Matrix            | latency 0, 20, 50, 200 ms x phases `under` and `over` x 3 repeats x 45 s = 24 measured runs; probes `reca` (200 ms, 300 req/s), `recb` (50 ms, 600 req/s), `ctl` (no proxy); about 42 minutes                                                                                                                                                                                                                                     |
| Offered rates     | under: 2000, 500, 225, 60 req/s. over: 5000, 1200, 550, 150 req/s. Level 0 (2000 `under`, 5000 `over`) was chosen by me after the trials: a trial at 4000 req/s accepted 3,995 with no errors, so the level-0 `over` run at 5000 sits at about the pool limit (25 / 5 ms = 5,000) and is a high-load baseline. The trial itself is not a result. The control probe uses the level-0 `under` rate and duration (2000 req/s, 45 s). |
| Dashboard         | `infra/observability/dashboards/stage-1/B3 - Slow database.json`                                                                                                                                                                                                                                                                                                                                                                  |
| Versions          | filled in after the run from env.txt                                                                                                                                                                                                                                                                                                                                                                                              |

## Method (planned)

1. For every run: restart the server, warm up with 400 requests at 40 in parallel (opens most pool connections), add the toxic, check one request is at least 90% of L slower, truncate the table, then run k6 with the shared `runner.sh`. After it: count rows (audit), read error types from the server, write a row with the shared `analyze.py`.
2. `under` runs are for latency and Little's Law, `over` runs for capacity. Probes are never used for headline numbers. The recovery probes use the removal epoch in `removal.tsv`.
3. Odd repeats go from 0 to 200 ms, even repeats from 200 to 0, so drift does not always land on the same level.
4. Discard rules, decided now: a run is discarded if k6 dropped iterations in an `under` run, if k6 CPU (cores 3,9,4,10) is above about 70%, if the self-check fails, or if the audit shows lost rows. Dropped iterations in an `over` run are reported as a finding, not discarded. Cores 5,11 (proxy) above about 80% at level 0 means the proxy is the limit and I will say so.
5. The trial runs (`-trial` name) are for plumbing only and are never quoted. The level-0 rates (2000 and 5000 req/s) were fixed after the trials and before this run; the trials are not quoted.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->

## What actually happened to the plan

- **Nothing was discarded or aborted.** All 24 measured runs and 3 probes finished with status `ok` (`runs.tsv`). The `under` runs all have 0 dropped iterations, k6 CPU stayed under 35% (max 34.8%, level 0 `over`), cores 5,11 (monitoring and proxy) under 22%, and the row audit says `ok` in every run. So none of my discard rules fired.
- **This is the second full run.** I threw away an earlier full run that used other level-0 rates (2,500 and 6,000 req/s) and did everything again with 2,000 and 5,000. Nothing from the first run is used here. The level-0 rates are the ones in the Setup table; they are the `run.sh` defaults, not environment overrides.
- **Dropped iterations in `over` runs** (reported, not discarded, by my own rule): level 0: 27,406 / 28,900 / 28,832; level 20: 2,426 / 2,420 / 2,417; level 50: 631 / 632 / 631; level 200: 0. This changes what "offered" means in those runs (k6 sent less than the rate I set), see observation 8.
- **The level-0 `over` run is a capacity measurement after all.** I had planned it as a high-load baseline (the 4,000 req/s trial was not saturated). At 5,000 req/s a queue formed (p50 674 ms), so the accepted rate is the capacity there.
- **Control probe:** it ran at the same rate and duration as level 0 `under` (2,000 req/s, 45 s), as planned. There is no screenshot saved in the repo, so there is no dashboard image in this document.
- **Follow-up run after the matrix (added 2026-11-20):** one extra run of the `over` phase at 0, 20 and 50 ms with `PRE_VUS=3000` (1 repeat, 45 s, no probes, results in `results/b3-prevus/`) to test why k6 dropped iterations. It is a separate test, not part of the 3-repeat matrix; its numbers are shown in their own table and are not mixed with the matrix.
- **Not looked at:** the heavy-tool captures of the probe runs (`pg_stat_activity` by state, `docker stats`, `iostat`, `pidstat`) are in `captures/`; I did not read them for this write-up.

## Environment (from env.txt)

| Item         | Value                                                                                                                                                   |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Commit       | `79a7e18` (the pre-run commit), 0 dirty files                                                                                                           |
| Kernel / CPU | Linux 7.0.0-43-generic, i5-11400H, 6 cores / 12 threads                                                                                                 |
| Node / k6    | v24.21.0 / k6 v2.4.0                                                                                                                                    |
| Postgres     | 15.17 in Docker, 2 CPUs, 2 GiB, cpuset 1,7,2,8; `max_connections=100`, `shared_buffers=128MB`, `synchronous_commit=on`, `fsync=on`, `wal_level=replica` |
| Toxiproxy    | 2.13.0, container `ep-toxiproxy`, cpuset 5,11 (shared with Prometheus, Grafana, node_exporter, postgres-exporter)                                       |
| Pinning      | server 0,6 (taskset), k6 3,9,4,10, Postgres 1,7,2,8, monitoring + proxy 5,11                                                                            |
| Pool         | `PG_POOL_MAX=25`, connection timeout 2,000 ms, statement timeout 5,000 ms                                                                               |
| Sysctls      | none changed                                                                                                                                            |

## Results and observations

All Prometheus numbers below are for one run window: from start + 8 s to start + 43 s of the run's `start_epoch` in `runs.tsv` (35 s inside the 45 s load, to skip start-up and the settle time), as instant queries at the window end with a 35 s range, for repeat 1 unless said. Queries: pool in use `avg_over_time((max(ep_pg_pool_total)-max(ep_pg_pool_idle))[35s:1s])`; waiting `max_over_time(max(ep_pg_pool_waiting)[35s:1s])`; in flight `avg_over_time(sum(http_requests_in_flight)[35s:1s])`; query p50 `histogram_quantile(0.5, sum by (le)(rate(ep_pg_query_duration_seconds_bucket[35s])))`; Postgres CPU `sum(rate(ep_pg_container_cpu_seconds_total[35s]))`; commits `sum(rate(pg_stat_database_xact_commit{datname="eventpulse"}[35s]))`; connections `avg_over_time(sum(pg_stat_activity_count{state=...})[35s:1s])`; host CPU `100*(1-avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"<group>"}[35s])))`. k6 numbers come from the lap JSONs through `analyze.py` (`summary.tsv`).

### Run outcomes (3 repeats each; accepted = 202 per second)

| Added latency | Phase | Offered | Accepted/s (3 repeats) | p50                      | p99                      | 503 share           | Dropped iterations       |
| ------------- | ----- | ------- | ---------------------- | ------------------------ | ------------------------ | ------------------- | ------------------------ |
| 0 ms          | under | 2,000   | 2,000 / 2,000 / 2,000  | 5.0 / 5.0 / 5.1 ms       | 15.6 / 21.7 / 18.2 ms    | 0                   | 0                        |
| 0 ms          | over  | 5,000   | 4,330 / 4,298 / 4,294  | 674 / 677 / 674 ms       | 732 / 756 / 755 ms       | 0                   | 27,406 / 28,900 / 28,832 |
| 20 ms         | under | 500     | 500 / 500 / 500        | 24.9 / 24.9 / 25.0 ms    | 29.4 / 28.8 / 29.7 ms    | 0                   | 0                        |
| 20 ms         | over  | 1,200   | 998 / 998 / 1,004      | 2.007 / 2.014 / 2.001 s  | 2.03 s                   | 9.0 / 9.0 / 8.5%    | 2,426 / 2,420 / 2,417    |
| 50 ms         | under | 225     | 225 / 225 / 225        | 53.7 / 53.7 / 53.6 ms    | 57.1 / 57.3 / 56.7 ms    | 0                   | 0                        |
| 50 ms         | over  | 550     | 460 / 461 / 461        | 2.047 / 2.047 / 2.048 s  | 2.06 s                   | 10.2 / 10.1 / 10.2% | 631 / 632 / 631          |
| 200 ms        | under | 60      | 60 / 60 / 60           | 204.1 / 204.2 / 204.3 ms | 206.2 / 206.8 / 208.3 ms | 0                   | 0                        |
| 200 ms        | over  | 150     | 122 / 121 / 119        | 2.172 / 2.149 / 2.103 s  | 2.20 / 2.20 / 2.21 s     | 14.9 / 15.2 / 16.8% | 0                        |

Probes (not headline numbers): `reca` 200 ms at 300 req/s: 18.0% 503; `recb` 50 ms at 600 req/s: 4.65% 503; `ctl` (no proxy) 2,000 req/s: p50 5.1 ms, p99 17.0 ms, 0 errors.

### Key numbers, repeat 1 (Prometheus, window above)

| Run                | Pool in use | Waiting max | In flight (gauge) | Little L (k6) | Pg active / idle | Pg CPU (cores) | Commits/s | Server / k6 / mon+proxy CPU % |
| ------------------ | ----------- | ----------- | ----------------- | ------------- | ---------------- | -------------- | --------- | ----------------------------- |
| under 0 ms         | 9.5         | 0           | 10.5              | 10.2          | 8.3 / 17.7       | 0.3            | 2,059     | 20.7 / 16.3 / 13.9            |
| over 0 ms          | 25.0        | 2,972       | 2,836             | 2,560         | 19.7 / 6.3       | 0.6            | 4,381     | 43.5 / 34.8 / 21.9            |
| under 20 ms        | 12.1        | 0           | 13.1              | 12.5          | 2.3 / 21.3       | 0.1            | 533       | 11.3 / 8.9 / 9.7              |
| over 20 ms         | 25.0        | 2,400       | 2,042             | 1,826         | 4.1 / 21.9       | 0.2            | 1,016     | 17.1 / 13.2 / 12.0            |
| under 50 ms        | 12.0        | 0           | 13.0              | 12.1          | 0.6 / 16.9       | 0.1            | 256       | 9.4 / 7.5 / 8.7               |
| over 50 ms         | 25.0        | 1,101       | 1,034             | 913           | 1.3 / 24.7       | 0.1            | 495       | 11.4 / 8.4 / 9.0              |
| under 200 ms       | 12.0        | 0           | 13.0              | 12.2          | 0.1 / 14.8       | 0.0            | 90        | 6.1 / 3.9 / 5.9               |
| over 200 ms        | 25.0        | 300         | 319               | 284           | 0.5 / 25.5       | 0.1            | 154       | 7.1 / 5.8 / 6.7               |
| control (no proxy) | 9.5         | 0           | 10.5              | 10.5          | 8.5 / 17.5       | 0.4            | 2,022     | 27.8 / 25.4 / 22.2            |

Hold time from the data, 25 / accepted rate in the `over` runs (mean of 3): 5.8 ms (0 ms), 25.0 ms (20 ms), 54.2 ms (50 ms), 206.6 ms (200 ms). In the `under` run at 0 ms, pool in use / rate = 9.5 / 2,000 = 4.75 ms.

### Recovery probes (k6 and gauge series from Prometheus, 1 s steps, 3 s rate window for k6)

Time 0 = the epoch in `removal.tsv` (`reca` 1795187393.64, `recb` 1795187495.23).

| Seconds after removal | A (200 ms, 300 req/s): p50 / p99 ms, waiting | B (50 ms, 600 req/s): p50 / p99 ms, waiting |
| --------------------- | -------------------------------------------- | ------------------------------------------- |
| -1                    | 2,095 / 2,366, 600                           | 2,081 / 2,179, 1,197                        |
| +1                    | 2,052 / 2,360, 591                           | 2,047 / 2,178, 674                          |
| +2                    | 1,142 / 2,315, 0                             | 1,097 / 2,172, 0                            |
| +3                    | 3 / 1,373, 0                                 | 7 / 1,393, 0                                |
| +4                    | 3 / 9, 0                                     | 5 / 7, 0                                    |
| +10                   | 3 / 4, 0                                     | 5 / 8, 0                                    |

Accepted per second: A went from 119 (before) to a spike of 512 at +2 s, then 300 (the offered rate); B from 460 to 1,148 at +2 s, then 600.

### Follow-up: `PRE_VUS=3000` in the `over` runs (1 repeat, `results/b3-prevus/`)

Same rates and settings as the matrix, only k6 starts with 3,000 virtual users instead of 500. Same Prometheus window rule as above.

| Added latency | Dropped iterations (matrix, 3 repeats -> follow-up) | Accepted/s (matrix -> follow-up) | 503 per second (count / 45 s; matrix repeat 1 -> follow-up) | Requests k6 sent (follow-up) | In flight gauge / k6 Little L (follow-up) |
| ------------- | --------------------------------------------------- | -------------------------------- | ----------------------------------------------------------- | ---------------------------- | ----------------------------------------- |
| 0 ms          | 27,406 / 28,900 / 28,832 -> 28,061                  | 4,330 / 4,298 / 4,294 -> 4,314   | 0 -> 0                                                      | 196,941                      | 2,986 / 2,905                             |
| 20 ms         | 2,426 / 2,420 / 2,417 -> **0**                      | 998 / 998 / 1,004 -> 992         | 103 -> 164                                                  | 54,001 (= 1,200 x 45)        | 2,353 / 2,103                             |
| 50 ms         | 631 / 632 / 631 -> **0**                            | 460 / 461 / 461 -> 459           | 55 -> 70                                                    | 24,751 (= 550 x 45)          | 1,098 / 975                               |

Also in the follow-up: p50 683 / 2,020 / 2,049 ms, pool in use 25.0 in all three, waiting peak 2,974 / 2,401 / 1,101, k6 CPU 33.5% / 14.7% / 10.2%, errors only `pool_timeout`, rows audit `ok`. Source: `results/b3-prevus/summary.tsv`, Prometheus at start + 43 s of each run in `results/b3-prevus/runs.tsv`.

### What I saw

1. **Latency in `under` runs is the added latency and almost nothing else.** p50 was 5.0, 24.9, 53.7 and 204.2 ms for 0, 20, 50 and 200 ms; the slope is 0.996.
2. **Capacity at 20, 50 and 200 ms is pool / hold time.** Accepted 1,000, 461 and 121 req/s (means); 25 / accepted = 25.0, 54.2 and 206.6 ms. The repeats agree within 1 to 3 req/s.
3. **Level 0 is different:** 4,307 req/s (mean), 14% below the 5,000 I computed. Pool in use was 25 and 2,972 requests waited; no 503, because the wait was bounded by k6's 3,000 virtual users (3,000 / 4,307 = 0.70 s, observed p50 0.674 s).
4. **Little's Law holds in `under` runs** (gauge 10.5 to 13.1 against k6's 10.2 to 12.5). In `over` runs the gauge is 12% to 13% higher than k6's number (2,042 vs 1,826; 1,034 vs 913; 319 vs 284).
5. **Postgres gets quieter as the API slows.** CPU 0.6 -> 0.2 -> 0.1 -> 0.1 cores; active connections 19.7 -> 4.1 -> 1.3 -> 0.5; idle connections rise.
6. **503s are `pool_timeout` only** and the median `over` request lasts the 2 s timeout (2.00 to 2.17 s).
7. **After removal the queue is gone in about 2 s** and latency is normal in about 3 to 4 s (with a 3 s smoothing window).
8. **k6 drops iterations in `over` runs, and the cause differs by level.** In the matrix: 0 at 200 ms (queue 300), 631 at 50 ms (queue about 1,100), about 2,420 at 20 ms (queue about 2,400), about 28,000 at 0 ms (queue about 2,970). The count at 50 ms is the same in all 3 repeats (631 to 632) and in the earlier trial (630). The follow-up with `PRE_VUS=3000` removed the drops at 20 and 50 ms (0 each), so those drops were k6 starting virtual users too slowly. It did not change level 0 (28,061 dropped), where the queue fills all 3,000 users.
9. **The proxy costs nothing measurable at 2,000 req/s:** control p50 5.1 ms against 5.0 to 5.1 ms.

## Explanation

1. **Why latency is one-for-one.** A request holds its pool connection from sending the INSERT to receiving the answer. The toxic delays the answer by L, so the hold time and the request time both grow by L. Nothing else in the request path changed, so nothing else moves (observation 1; evidence: `summary.tsv`, p50 of the `under` runs).
2. **Why capacity is pool / hold.** Each of the 25 connections finishes one request per hold time, so the pool finishes 25 / hold requests per second. At 20 ms and above the pool was always full (pool in use 25.0) and the hold time was dominated by L, so capacity followed it (observation 2; evidence: accepted rate and pool in use in the tables). The hold time at 50 ms added (54.2 ms) is about 4 ms above L, which is the cost of the INSERT and the commit.
3. **Why the API cannot be faster than its slowest synchronous dependency.** At 200 ms the API accepts 121 req/s, 2.8% of the 4,307 req/s it accepted with no added latency (121 / 4,307). Postgres at that moment is nearly idle (0.1 cores, 0.5 active connections), so the limit is only the waiting. More CPU for Postgres would change nothing; only more connections or a shorter wait would (and B2 showed the pool size has limits).
4. **Why `over` runs are stuck at 2 s and fail with 503.** When the offered rate is above pool / hold, requests queue for a connection. The pool gives up after 2 s (`connectionTimeoutMillis`), the API answers 503, so the median request is the timeout. The queue is limited by that timeout: every request that arrives stays at most 2 s, so the queue is about (rate of arrivals) x 2 s, which is Little's Law with W = 2 s. That is why the waiting peak fits the SENT rate x 2 s (2,400 against 1,097 x 2 = 2,194; 1,101 against 513 x 2 = 1,026; 300 against 143 x 2 = 286) and not capacity x 2 s as I predicted. Evidence: `ep_pg_pool_waiting` and the k6 counts in the tables.
5. **Why level 0 does not follow the formula (Not resolved).** At 0 ms the pool was full (25.0 in use) with 2,972 waiting, so it was saturated, yet it accepted 4,307 instead of 5,000. The hold time was 5.8 ms (25 / 4,307) against 4.75 ms at 2,000 req/s (9.5 in use / 2,000). Two explanations fit: the hold time grows when the system is saturated (B2 saw 4.5 ms unsaturated and 5.4 ms saturated), or the Node main thread is the limit (cores 0 and 6 together were 43.5% busy, which is about 0.87 of one CPU for a mostly single-threaded process). I did not run anything that separates them.
6. **Why Postgres idles.** The latency is added after Postgres has answered, so Postgres finishes each INSERT in about the same time and then waits for the next one. Fewer requests arrive per second, so it does less work (CPU and commits fall), and the connections are `idle`, waiting for the API. Evidence: the Postgres columns of the key-numbers table.
7. **Why recovery is fast.** When the toxic is gone the hold time drops back to about 5 ms and capacity jumps far above the offered 300 or 600 req/s. The backlog (600 and 1,195 waiting requests) drains in about 2 s and the 503s end. It is bounded by the 2 s timeout: nothing older than 2 s can be in the queue. The accepted rate spiked (512 and 1,148 req/s at +2 s) while the queue emptied. Evidence: the recovery table.
8. **Why k6 dropped iterations (Observed in a follow-up).** The script pre-allocates 500 virtual users (`PRE_VUS`) and k6 has to start more when the queue needs them, which takes time; requests that were due meanwhile are dropped. I tested it by starting with 3,000 users: the drops at 20 and 50 ms went from about 2,420 and 631 to 0, and k6 then sent the whole offered rate (54,001 requests = 1,200 x 45 at 20 ms; 24,751 = 550 x 45 at 50 ms). The accepted rate did not change (992 against 998 at 20 ms, 459 against 460 at 50 ms), so the drops did not hide capacity; they only made the 503 count too low (164 per second at 20 ms with no drops, against 103 with drops, my prediction was about 200). At 0 ms nothing changed (28,061 dropped, 4,314 accepted): the queue reaches the 3,000-user maximum, so there the drops are the user cap, not the start-up delay. One run each, not repeated.

## Status of each claim

| Claim                                                                                                           | Status                                                                                                                                                                                                                       |
| --------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| p50 in `under` runs = added latency plus about 4 to 5 ms, slope 0.996                                           | Observed (3 of 3 repeats, 4 levels)                                                                                                                                                                                          |
| Capacity at 20, 50 and 200 ms = 25 / hold time: 1,000 / 461 / 121 req/s                                         | Observed                                                                                                                                                                                                                     |
| Hold time is about 5 ms with no added latency and 3.7 to 4.2 ms (p50 minus L) at 50 to 200 ms                   | Observed; why it is lower at higher latency is Not resolved                                                                                                                                                                  |
| Capacity at 0 ms is 4,307 req/s, not 5,000                                                                      | Observed                                                                                                                                                                                                                     |
| The limit at 0 ms is a longer hold time under saturation, or the Node main thread                               | Not resolved                                                                                                                                                                                                                 |
| Little's Law holds between gauge and k6 in `under` runs                                                         | Observed                                                                                                                                                                                                                     |
| In `over` runs the gauge reads 12% to 13% above k6's Little number                                              | Observed (also in the follow-up without drops: 2,353 vs 2,103 and 1,098 vs 975). The cause is Not resolved: the 2 s while the queue fills explains at most about 4% (2 s of 45 s), and dropped iterations are not the reason |
| The queue length is about the sent rate x 2 s                                                                   | Inferred (3 levels and 2 probes fit; not tested with another timeout)                                                                                                                                                        |
| 503s are `pool_timeout` only                                                                                    | Observed                                                                                                                                                                                                                     |
| Postgres is not the bottleneck at 20 ms and above                                                               | Observed (CPU, active connections), Inferred as the reason capacity fell only because of the waiting                                                                                                                         |
| Recovery: queue empty within 2 s, latency normal within 3 to 4 s of removal                                     | Observed (2 probes, 1 s gauge, 3 s smoothed k6); not repeated                                                                                                                                                                |
| The proxy costs nothing at 2,000 req/s                                                                          | Observed (1 control run); not tested at higher rates                                                                                                                                                                         |
| At 20 and 50 ms k6 drops iterations because it starts virtual users too slowly after the 500 pre-allocated ones | Observed (follow-up with `PRE_VUS=3000`: 0 drops); 1 repeat                                                                                                                                                                  |
| At 0 ms the drops are the 3,000-user maximum, and capacity (4,314) is not limited by them                       | Observed (follow-up, 28,061 drops, same accepted rate); 1 repeat                                                                                                                                                             |
| The server's `ep_pg_query_duration_seconds` p50 moves with L                                                    | Not resolved (buckets too coarse at 50 and 200 ms)                                                                                                                                                                           |
| Commits per second exceed the accepted rate by about 30 per second because of the exporter and my polling       | Likely, not verified                                                                                                                                                                                                         |

## Prediction vs observation

Each prediction is copied unchanged from the pre-run half.

> 1. **Capacity (accepted 202/s in an `over` run) = 25 / (h0 + L):**
>    - 0 ms: 25 / 0.005 = 5,000 req/s
>    - 20 ms: 25 / 0.0250 = 1,000 req/s (range 985 to 1,020 for h0 4.5 to 5.4 ms).
>    - 50 ms: 25 / 0.055 = 455 req/s (range 440 to 460).
>    - 200 ms: 25 / 0.205 = 122 req/s (range 120 to 123).

**Verdict:** **Mostly matches, with one wrong number.** 20 ms: 998, 998, 1,004 req/s (predicted 1,000, range 985 to 1,020): matches. 50 ms: 460, 461, 461 (range 440 to 460): matches, 1 req/s above the range. 200 ms: 122, 121, 119 (range 120 to 123): matches, one run 1 req/s below. **0 ms: wrong.** I predicted 5,000 req/s, I saw 4,330, 4,298, 4,294 (about 14% lower). The implied hold time is 25 / 4,307 = 5.8 ms, not 5.0 ms (see Explanation, point 5). The proxy was not the limit (cores 5,11 at 21.9%), so my "if below 2,150 the proxy is the limit" worry did not apply. Source: `summary.tsv`, column `accepted_rps`.

> 2. **Latency in an `under` run is one-for-one.** p50 is about h0 + L + 1 to 3 ms of API work. The slope of p50 against L is 1.0, between 0.9 and 1.1. The server's `ep_pg_query_duration_seconds` p50 moves by about L too.

**Verdict:** **Matches on the slope, wrong on the offset.** p50 in `under` runs: 5.0, 24.9, 53.7, 204.2 ms for 0, 20, 50, 200 ms. Slope 0.996 (0 to 200 ms), 0.995 (0 to 20), 0.958 (20 to 50), 1.003 (50 to 200): inside 0.9 to 1.1. But p50 minus L is 5.0, 4.9, 3.7, 4.2 ms, not h0 + 1 to 3 ms = 6 to 8 ms. At 50 ms I predicted 56 to 58 ms and saw 53.6 to 53.7. So the API adds almost nothing on top of the hold time, and the hold time at low load is at or below 5 ms. The second half (the server's `ep_pg_query_duration_seconds` p50 moves by about L) **could not be checked**: its buckets are 25, 50, 100 and 250 ms wide there, so the quantile returns the bucket middle (75 ms at L=50, 175 ms at L=200); only 21.9 ms at L=20 is meaningful.

> 3. **Little's Law in an `under` run.** In flight = rate x mean latency = about 12 at every level (2,000 x 0.006 = 12 at 0 ms is only approximate, since the API overhead is a large part of the 6 ms there; 500 x 0.025 = 12.5; 225 x 0.055 = 12.4; 60 x 0.205 = 12.3). Pool in use below 25 and waiting about 0, no errors.

**Verdict:** **Matches.** k6 Little's L (rate x mean latency, column `little_L`): 10.2 to 10.6 (0 ms), 12.5 to 12.6 (20 ms), 12.1 (50 ms), 12.2 (200 ms); the in-flight gauge (repeat 1): 10.5, 13.1, 13.0, 13.0. Pool in use (repeat 1): 9.5, 12.1, 12.0, 12.0, never 25. Waiting: 0. Errors: 0 in every `under` run. At 0 ms the prediction of 12 was too high (10.2 to 10.6 seen), because the mean latency there is 5.1 ms, not 6 ms.

> 4. **Little's Law in an `over` run.** In flight is about 25 in the pool plus the waiting queue. The queue is limited by the 2 s pool timeout to about capacity x 2 s: about 2,000 at 20 ms, 900 at 50 ms, 250 at 200 ms. The excess (offered minus capacity: about 200, 95, 28 req/s) gets 503 `pool_timeout` after about 2 s, so the p50 in `over` runs sits at about 2 s (as pool 5 did in B2: 2,003 ms). The 3,000 k6 virtual users should be enough, but 20 ms is the closest to that limit.

**Verdict:** **Partly wrong.** Right: p50 in `over` runs sits at about 2 s (2.00 to 2.01 s at 20 ms, 2.05 s at 50 ms, 2.10 to 2.17 s at 200 ms), and all errors were `pool_timeout`. Wrong: (a) the queue is not capacity x 2 s. Waiting peak (repeat 1, `ep_pg_pool_waiting`): 2,400 (20 ms), 1,101 (50 ms), 300 (200 ms) against my 2,000, 900, 250; it fits the rate k6 actually SENT x 2 s (1,097 x 2, 513 x 2, 143 x 2). (b) The excess I predicted (200, 95, 28 req/s) was not what I saw: 503 per second (count / 45 s, repeat 1) was 103, 55, 22, because k6 dropped iterations at 20 and 50 ms and sent less than the offered rate (1,097 instead of 1,200; 513 instead of 550). (c) I expected 3,000 virtual users to be enough at 20 ms, but k6 dropped 2,417 to 2,426 iterations anyway (see Explanation, point 8).

> 5. **Postgres is not the bottleneck.** At 50 and 200 ms the Postgres container CPU and commits per second fall with the accepted rate, and most connections are `idle` in `pg_stat_activity`.

**Verdict:** **Matches.** Postgres container CPU (repeat 1, `over` runs): 0.6 cores at 0 ms, 0.2 at 20 ms, 0.1 at 50 ms, 0.1 at 200 ms. Active connections: 19.7, 4.1, 1.3, 0.5; idle: 6.3, 21.9, 24.7, 25.5. Commits per second follow the accepted rate (4,381, 1,016, 495, 154 against 4,330, 998, 460, 122), with a roughly constant extra of about 30 per second that is not the API (Likely, not verified: the exporter and my own polling).

> 6. **Recovery.** After the toxic is deleted, the capacity jumps to well above the offered rate (25 / 5 ms = 5,000 against 300 or 600 req/s). The backlog is at most about 250 (A) or 900 (B) requests, and it drains in under a second. p99 should be back near the level-0 value within 0.5 to 3 s of the removal, and never later than the 2 s timeout plus about 1 s of k6 resolution. The 503s stop within about 2 s.

**Verdict:** **Right on the timing, wrong on the backlog size.** Waiting at the moment of removal: 600 (A) and 1,195 (B), my prediction was 250 and 900; it fits offered rate x 2 s (300 x 2 and 600 x 2), the same mistake as in prediction 4. Waiting was 0 two seconds after removal in both probes, so the backlog drained in at most 2 s (the gauge is sampled every second); I said under a second. k6 p99 (3 s rate window) was back to 9 ms (A) and 7 ms (B) four seconds after removal, and p50 three seconds after. That is later than my 0.5 to 3 s, but the 3 s smoothing window in my query explains about 2 s of it, so I count the timing as matching within the resolution. The last second with 503 was +2 s in both probes.

> 7. **The proxy costs little.** The control probe (no proxy, 2,000 req/s, 45 s: the same rate and duration as the level-0 `under` runs) should have p50 within 1 ms of the level-0 `under` runs through the proxy, and the same accepted rate.

**Verdict:** **Matches.** Control (no proxy, 2,000 req/s): p50 5.1 ms, p99 17.0 ms, accepted 2,000, 0 errors. Level 0 `under` through the proxy: p50 5.0, 5.0, 5.1 ms, p99 15.6 to 21.7 ms, accepted 2,000. The p50 difference is 0.0 to 0.1 ms. Pool in use was 9.5 in both. At 2,000 req/s the proxy costs nothing I can measure; its cost at 4,000 and more is not tested (no control at that rate).

> 8. **What should not change:** the number of errors of other types (no `too_many_clients`, no statement timeout), because 200 ms is far below the 5 s statement timeout and 25 connections are far below Postgres' 100.

**Verdict:** **Matches.** The only error type in all 27 runs is `pool_timeout` (summary column `err_types`); no `too_many_clients`, no statement timeout. The rows audit says `ok` in every run.

## Verdict on the hypothesis

**Confirmed with corrections.** Adding latency to every query raised the API latency one-for-one (slope 0.996), and it cut the capacity to pool / hold time at 20, 50 and 200 ms, with the hold time at about 5 ms plus L (1,000, 461 and 121 req/s, within about 1 to 2% of 25 / (5 ms + L)). The corrections: at 0 ms the capacity was 4,307 req/s, 14% below the formula, so the formula needs a hold time that grows when the system is saturated (or another limit); my queue size (capacity x 2 s) was wrong, it is the arrival rate x the 2 s timeout; and k6 dropped iterations in the `over` runs, so I sent less than I planned. The purpose was met: I can now predict the capacity of this API for a slow database in the 20 to 200 ms range from the pool size and the added latency, and I can explain why it is never faster than that dependency; I cannot yet explain the 0 ms ceiling.

## Limitations

- 3 repeats per cell, one machine, so the numbers are relative to this laptop. The repeats agree closely (for example 998, 998, 1,004), but B3 used only one pool size (B2 covers pool sizes).
- The proxy shares cores 5,11 with the monitoring stack. At 5,000 req/s those cores were at 21.9%, so I do not think the proxy limited anything, but the control probe (no proxy) was only run at 2,000 req/s.
- Level 0 `over` is affected by 27,000 to 29,000 dropped iterations and by the 3,000-user cap; the accepted rate is credible as a capacity (a queue formed) but k6 was close to its own limits.
- All `over` runs of the matrix except 200 ms are affected by dropped iterations, so the offered rate was lower than set. I report accepted per second, which the follow-up with `PRE_VUS=3000` showed is not affected at 20 and 50 ms (same capacity, no drops). The follow-up was one repeat per level, so it confirms the cause but does not give a spread.
- The latency toxic is applied only in the database-to-API direction and after the warm-up; real network latency has two directions and also affects new connections. Everything runs on one machine (docker network and loopback), so there is no real network.
- The server-side query histogram has buckets that are too coarse at 50 and 200 ms; I used k6 latency instead.
- Recovery: 2 probes, a 1 s gauge and a 3 s smoothing window in the k6 series, so the 3 to 4 s is an upper bound.
- The self-check output (one request slower by about L) was printed on the screen and not saved to a file, so I cannot show it here. The run would have stopped if it had failed, and the capacity numbers show the latency was in effect.
- No dashboard screenshot is in the repo.

## Cost

- **What a slow database costs:** capacity falls to 23% (20 ms: 1,000 / 4,307), 11% (50 ms: 461 / 4,307) and 2.8% (200 ms: 121 / 4,307) of the 0 ms capacity, with nothing wrong in Postgres.
- **What failure costs:** when the offered rate is above capacity (my `over` runs), 9% to 17% of requests get a 503 after waiting 2 s, and the other requests also wait about 2 s (p50 2.0 to 2.2 s).
- **What the fix costs:** nothing was added in B3. The 2 s connection timeout that turns a queue into 503s was already there. What I did not measure is the cost of not having it (the stage doc describes a follow-up with the timeouts unset).

## Takeaways

1. The capacity of a pooled synchronous API is pool size / hold time, and the hold time includes the whole round trip to the database, not only the query. Here a 20 ms hop cost 77% of the capacity (1 - 1,000 / 4,307).
2. Added latency shows up in my API latency one-for-one, so a latency budget must include every synchronous call.
3. The queue in front of a full pool is (arrival rate x timeout) requests long; that is the number to size memory and virtual users with.
4. The timeout is what turns a slow dependency into fast failures (503 after 2 s); it also decides how fast the system recovers (about 2 s here).
5. Do not trust a capacity formula at the high end: at 0 ms the hold time was not constant, so I measure the ceiling instead of computing it.
6. In k6, pre-allocate enough virtual users for the queue I expect (`PRE_VUS`), otherwise the offered rate is lower than the one I think I am testing. I tested this: with 3,000 pre-allocated users the drops at 20 and 50 ms went to 0.
7. Next: repeat this with the timeouts unset (hung requests, memory growth in Node), find out what limits the 0 ms ceiling (Node main thread or hold time under saturation), and find out why the in-flight gauge reads about 12% above k6's Little number in `over` runs.
