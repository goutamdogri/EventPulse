# B8 Experiment explanations

Status: PRE-RUN. Written 2026-11-21 before any measured run. The sections after the marker are appended after the experiment.

## Purpose

I want to know what the ingest API does when Postgres goes away for a few seconds: how many requests fail, what the failures look like, and whether the API is healthy again as soon as the database is, or later. This matters now because every event the service answers with 202 is promised to be in Postgres, so the dependency chain API -> pool -> network -> Postgres is the first place where events can be lost. Once I know the numbers I can say how many events a producer loses if it does not retry, and decide if the API needs a client-side query timeout, a readiness check that reflects the pool, or retry guidance for producers.

## Hypothesis

During a Postgres restart the API returns 503s, the pool holds dead connections, and recovery may lag behind the moment the database is available again. My mechanism: a restart closes every connection, the pool finds out one connection at a time, and new connections only work once Postgres accepts them. In a network pause nothing is closed, so I think the pool cannot tell the connections are dead and may stay clogged. In both cases I expect no acknowledged event to be missing from the table.

## Predictions (written before the run)

Setup numbers: 2000 req/s x 120 s = 240000 requests per run; fault at 30 s; pool 25; pool connection timeout 2 s; k6 request timeout 10 s.

1. **Control (`none`)**: 0 non-202 responses, rows = 202s exactly, audit `ok`.
2. **`restart`**: Postgres is down for about 6 s (range 4 to 10 s): fast shutdown 1 to 2 s, container stop and start about 1 s, startup about 2 to 3 s (the shutdown was clean, so no WAL replay). Failed requests: 2000 req/s x 6 s = about 12000 (range 8000 to 20000), which is 5 % of the 240000 (range 3 to 8 %). Almost all failures are 503 (fast), a few status 0.
3. **Error codes in `restart`**: because the server goes through Toxiproxy, a down Postgres shows up as a connection that is accepted and then closed, so I expect `terminated` and `57P01`, maybe `ECONNRESET`, and I do not expect `ECONNREFUSED` (that is what `pause_reset` should give). I expect `57P03` only for a moment or not at all.
4. **Pool in `restart` reconnects cleanly**: removes of about 25 (the pool size) around the fault, then connects of about 25, and the pool is back to total 25 within a few seconds of traffic. A lent-out dead connection is removed when it is released.
5. **Recovery lag in `restart`**: the API answers 202 again within 1 s of the first good database probe (`lag_s` between 0 and 1.5), because a new connection is opened on demand and takes a few ms. So recovery does not lag behind the database, apart from the probe cadence. `api_end` is `ok`.
6. **`pause_reset`** (proxy disabled 10 s): the open connections are closed and new ones are refused, so errors are fast: about 2000 x 10 s = 20000 failed requests (8.3 % of 240000), mostly 503 with `ECONNREFUSED` and `terminated`. The pool reconnects like in `restart`. Recovery lag 0 to 1.5 s after the proxy is enabled again.
7. **`pause_blackhole`** (10 s): no errors for a moment, because the connections are open. Up to 25 in-flight requests hold all 25 pool connections, so after that every new request waits for the pool and fails after 2 s with `pool_timeout`. Latency in the pause is about 2 s, not milliseconds, so about 2000 x 2 s = 4000 requests are in flight at once (under the 8000 virtual users, so no dropped iterations). Failed requests: about 20000 (the 10 s of the pause), plus up to 25 requests that were stuck. Pool: total 25, idle 0, waiting several thousand during the pause. No `removes` during the pause.
8. **After the black hole** I expect the Toxiproxy docs to be right (data is held back, not dropped): when the toxic is removed the 25 stuck queries finish, those rows are committed, and the pool recovers within about 2 s. Then rows are a little larger than 202s (`+N committed-not-acked`, N up to 25) because some of those requests already timed out in k6. If instead the data was dropped, the 25 connections stay stuck for good (no client-side query timeout, `statement_timeout` is server side), the pool stays at 25/0 with waiting above 0, the API stays at 503 `pool_timeout`, `api_end` is `DOWN` and about 2000 x 80 s = 160000 more requests fail. I think this second outcome is less likely (maybe 25 %), but it would be the most important finding.
9. **No acknowledged event is lost in any variant**: audit never `LOST`, because `synchronous_commit` is on and a 202 is only sent after COMMIT. `restart` and `pause_blackhole` may show a few committed-not-acked rows (requests that were in flight when the connection died or hung).
10. **What should not change**: the server's CPU (a failing request is cheaper than a successful one, so it may fall a little), event loop lag stays small (tens of ms at most), k6 dropped iterations 0 in `restart` and `pause_reset`.
11. **Producer loss without retries**: about 5 % (restart), 8 % (pause_reset), 8 % (pause_blackhole) of the events, i.e. roughly 12000, 20000 and 20000 events of 240000. The number scales with the outage length and the offered rate: events lost = rate x seconds the API answers non-202.

## Setup (planned)

| Item              | Value                                                                                                                                        |
| ----------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| Server            | `services/ingest-api` built from `dist/main.js`, `taskset -c 0,6`, pool 25, `.env` values for the timeouts (2000 ms pool, 5000 ms statement), 100 tenants, connects through Toxiproxy `localhost:15434` |
| Postgres          | compose container `ep-postgres`, postgres:15, cpuset 1,7,2,8, `synchronous_commit` on (default)                                              |
| Sysctls           | none changed                                                                                                                                 |
| Route / generator | `POST /v1/events`, k6 `constant-arrival-rate` 2000 req/s, 120 s, pinned to 3,9,4,10, 10 s request timeout, 1000 to 8000 VUs                  |
| Fault             | `restart` = `docker restart ep-postgres`; `pause_reset` = proxy disabled 10 s; `pause_blackhole` = `timeout` toxic (timeout 0, both directions) 10 s; `none` = control; injected 30 s after k6 starts |
| Probes            | database probe (pg_isready in the container) and API probe (GET `/readyz`, 1 s timeout) every 0.25 s, pinned to 5,11                         |
| Matrix            | 4 variants x 3 repeats x 120 s = 12 runs, about 34 min including 50 s of setup per run; odd repeats forward, even repeats reversed          |
| Dashboard         | [B8 - Database restart under load.json](../../../../infra/observability/dashboards/stage-1/B8%20-%20Database%20restart%20under%20load.json) |
| Versions          | filled in after the run from env.txt                                                                                                         |

## Method (planned)

1. Per run: restart the server (new pool), enable the proxy and clear toxics, warm up with 400 requests, truncate `events`, checkpoint, wait 5 s. Start the two probes, start k6, wait 30 s, inject the fault, let k6 finish, stop the probes, count rows.
2. Measured quantities per run: 202, 503 and other counts (k6), error codes and pool counters (server), probe timeline (`db_down_s`, `db_up_s`, `api_down_s`, `api_up_s`, `api_stable_s`, `lag_s`, `api_end`), rows minus 202s.
3. Events lost without retries = requests that were not a 202 (503 plus other). Committed-not-acked rows are counted separately.
4. Discard criteria, decided now: a run with k6 dropped iterations above 0 is not used for the loss percentage (the offered rate was not kept); a run where the k6 cores were above 70 % busy; a run where the warm-up or the server start failed; a run where the fault was injected more than 2 s away from `FAULT_AT` seconds after k6 start. A discarded run is repeated, not hidden. A pause run with dropped iterations is kept as a finding about the generator limit, not about the loss.
5. The `none` runs must give audit `ok`; if they do not, the setup is wrong and nothing else is trusted.

<!-- ===== END OF PRE-RUN CONTENT. Everything below is appended after the experiment. ===== -->
