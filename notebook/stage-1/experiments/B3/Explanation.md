# B3 Experiment explanations

Status: PRE-RUN. Written 2026-11-20 before any measured run. The sections after the marker are appended after the experiment.

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

| Item | Value |
|---|---|
| Server | ingest-api (NestJS), `node --env-file=.env dist/main.js`, `PG_POOL_MAX=25`, `taskset -c 0,6`, restarted before every run. Connects to `localhost:15434` (Toxiproxy). No server code change for B3. |
| Sysctls | none changed |
| Route / generator | `POST /v1/events`, k6 `constant-arrival-rate`, 500 pre-allocated and up to 3,000 VUs, `taskset` on `3,9,4,10` |
| Fault injection | Toxiproxy 2.13.0 in a container (`ep-toxiproxy`, cpuset `5,11`), proxy `pg` from `:15434` to `postgres:5432`, toxic `latency` on the downstream direction, jitter 0, added after the warm-up |
| Matrix | latency 0, 20, 50, 200 ms x phases `under` and `over` x 3 repeats x 45 s = 24 measured runs; probes `reca` (200 ms, 300 req/s), `recb` (50 ms, 600 req/s), `ctl` (no proxy); about 42 minutes |
| Offered rates | under: 2000, 500, 225, 60 req/s. over: 5000, 1200, 550, 150 req/s. Level 0 (2000 `under`, 5000 `over`) was chosen by me after the trials: a trial at 4000 req/s accepted 3,995 with no errors, so the level-0 `over` run at 5000 sits at about the pool limit (25 / 5 ms = 5,000) and is a high-load baseline. The trial itself is not a result. The control probe uses the level-0 `under` rate and duration (2000 req/s, 45 s). |
| Dashboard | `infra/observability/dashboards/stage-1/B3 - Slow database.json` |
| Versions | filled in after the run from env.txt |

## Method (planned)

1. For every run: restart the server, warm up with 400 requests at 40 in parallel (opens most pool connections), add the toxic, check one request is at least 90% of L slower, truncate the table, then run k6 with the shared `runner.sh`. After it: count rows (audit), read error types from the server, write a row with the shared `analyze.py`.
2. `under` runs are for latency and Little's Law, `over` runs for capacity. Probes are never used for headline numbers. The recovery probes use the removal epoch in `removal.tsv`.
3. Odd repeats go from 0 to 200 ms, even repeats from 200 to 0, so drift does not always land on the same level.
4. Discard rules, decided now: a run is discarded if k6 dropped iterations in an `under` run, if k6 CPU (cores 3,9,4,10) is above about 70%, if the self-check fails, or if the audit shows lost rows. Dropped iterations in an `over` run are reported as a finding, not discarded. Cores 5,11 (proxy) above about 80% at level 0 means the proxy is the limit and I will say so.
5. The trial runs (`-trial` name) are for plumbing only and are never quoted. The level-0 rates (2000 and 5000 req/s) were fixed after the trials and before this run; the trials are not quoted.

