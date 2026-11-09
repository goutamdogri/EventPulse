# E0.6 Experiment explanations

## Hypothesis

With keep-alive off (a new TCP connection for every request), the maximum rps drops, TIME_WAIT sockets pile up, and more CPU is spent in the kernel.

### Predictions

Let,
ephemeral port range = 32768 to 60999 = 28232 ports
TIME_WAIT lasts 60 s
`tcp_tw_reuse = 2` (loopback only), `tcp_max_tw_buckets = 65536`, `somaxconn = 4096`

If no port is reused,
TIME_WAIT ≈ new connections per second × 60

=> ports run out at λ = 28232 / 60 = 470 new connections/s

So,

1. With keep-alive off, k6 should start failing with `cannot assign requested address` from about 470 new connections/s. At 1000 rps I expect about 60,000 TIME_WAIT sockets.
2. Connections per request: about 0 with keep-alive, about 1 without.
3. The server closes first, so the server holds the TIME_WAIT sockets (client side stays near 0).
4. The extra work lands in the kernel (accept, close, epoll_ctl), so kernel CPU share rises.

`tcp_tw_reuse = 2` allows reuse over loopback, so exhaustion may come later than 470/s. TIME_WAIT per side cannot be more than the number of distinct ports (28232).

---

## Setup

| Item             | Value                                                                                                                                      |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| Server           | plain Node `http` + `prom-client`, started with `server-runner.sh`, pinned with `taskset`; fd limit about 1.05 million (`process_max_fds`) |
| Sysctls          | `ip_local_port_range 32768 60999`, `tcp_tw_reuse 2`, `tcp_max_tw_buckets 65536`, `somaxconn 4096`                                          |
| Route under load | `/health` on `http://localhost:8000` (loopback, k6 and server on the same machine)                                                         |
| Generator        | k6 through `runner.sh`, `constant-arrival-rate`, 60 s per run, `preAllocatedVUs 300`, `maxVUs 5000`, metrics by remote write               |
| k6 switch        | `KEEPALIVE=1` reuses connections, `KEEPALIVE=0` sets `noConnectionReuse` (new connection per request, `Connection: close`)                 |
| Matrix           | rates 500, 1000, 2000, 4000, 8000 rps, each with keep-alive on then off, 1 repeat (10 runs), TIME_WAIT drained before every run            |
| Dashboard        | `infra/observability/dashboards/stage-0/E0.6 - Keep-alive vs new connection per request.json`                                              |

Metrics used: `http_requests_total`, `http_request_duration_seconds`, `http_connections_total`, `http_connections_open`, `http_requests_in_flight`, `http_server_errors_total`, `process_cpu_user/system_seconds_total`, `process_open_fds`, event loop delay, k6 requests / connecting / duration / dropped iterations / failed rate (all split by the `keepalive` tag), and node_exporter TCP socket, netstat, CPU and context switch metrics.

## Method

1. Record the sysctls and wait until TIME_WAIT is near 0.
2. Run one k6 run (`step.sh` through `runner.sh`), alternating keep-alive on and off for each rate.
3. For every run, capture at the start (`pre`), in the middle (`mid`) and at the end (`post`): `nstat` (opens, retransmits, listen overflows), and at `mid`: `ss` TIME_WAIT counts for the server side (`sport = :8000`) and the client side (`dport = :8000`), and a 10 s `strace -c -f -p $PID`.
4. Watch the dashboard live, then compare the runs.
5. Per-run numbers below come from the k6 summary files, `nstat`, `ss` and `strace` of the 4000 rps pair (`e06-r4000-ka-on-n1`, `e06-r4000-ka-off-n1`). Peak values for the other rates are read from the dashboard.

Exact commands are in `loadtests/k6/stage-0/E0.6/README.md`.

---

## Results and observations

### Dashboard over the whole matrix

![E0.6 dashboard](./E0.6%20-%20Keep-alive%20vs%20new%20connection%20per%20request.png)

### Run outcome per rate

| Rate | Keep-alive on                 | Keep-alive off                                                   |
| ---- | ----------------------------- | ---------------------------------------------------------------- |
| 500  | completed, 0 failed           | completed, 0 failed                                              |
| 1000 | completed, 0 failed           | completed, 0 failed                                              |
| 2000 | completed, 0 failed           | completed, 0 failed                                              |
| 4000 | completed, 0 failed           | completed, 0 failed, 13 dropped iterations                       |
| 8000 | completed (8.0K req/s served) | **collapsed**: up to 75% failed, up to 6.0K dropped iterations/s |

### Numbers side by side (4000 rps)

| Measurement                             | Keep-alive on         | Keep-alive off                                      |
| --------------------------------------- | --------------------- | --------------------------------------------------- |
| Requests completed (60 s)               | 240,001               | 239,989                                             |
| Achieved rps                            | 3,999.8               | 3,999.7                                             |
| Failed requests                         | 0                     | 0                                                   |
| Dropped iterations                      | 0                     | 13                                                  |
| VUs used (peak) / allocated             | 7 / 300               | 110 / 302                                           |
| New connections (`PassiveOpens` delta)  | 302                   | 222,811 (window not aligned with the run, see note) |
| Connections per request                 | 0.0013                | ≈ 1 (dashboard max 1.01)                            |
| TIME_WAIT at mid-run, server side       | 0                     | 10,479                                              |
| TIME_WAIT at mid-run, client side       | 1                     | 10,058                                              |
| `http_req_duration` p50 / p90 / p95     | 0.24 / 0.47 / 0.58 ms | 0.18 / 3.66 / 14.3 ms                               |
| `http_req_duration` avg / max           | 0.32 / 23.5 ms        | 2.09 / 66.7 ms                                      |
| `http_req_connecting` p50 / avg / max   | 0 / 0.0004 / 8.9 ms   | 0.094 / 0.141 / 30.7 ms                             |
| `http_req_blocked` p50 / max            | 0.004 / 9.0 ms        | 0.145 / 63.8 ms                                     |
| `iteration_duration` avg                | 0.40 ms               | 2.39 ms                                             |
| Bytes sent per request                  | 76.0                  | 95.0                                                |
| Bytes received per request              | 166.0                 | 138.0                                               |
| `TcpRetransSegs` delta                  | +19                   | +32,558                                             |
| `TcpExtListenOverflows` / `Drops` delta | 0 / 0                 | +36,300 / +36,300                                   |

_The `nstat` window ran from the first to the last snapshot (about 62 s), which is not exactly the 60 s of k6, so the connection counts are close to but not exactly the request counts._

### Syscalls in a 10 s `strace -c -f` sample (4000 rps offered)

| Syscall                              | Keep-alive on | Keep-alive off |
| ------------------------------------ | ------------- | -------------- |
| `accept4`                            | 0             | 17,969         |
| `epoll_ctl`                          | 0             | 17,969         |
| `setsockopt`                         | 0             | 17,969         |
| `shutdown`                           | 0             | 17,969         |
| `close`                              | 30            | 17,998         |
| `io_uring_enter`                     | 0             | 17,968         |
| `read`                               | 40,040        | 18,022         |
| `write`                              | 39,999        | 17,982         |
| `epoll_pwait`                        | 57,977        | 17,975         |
| Requests in sample                   | ≈ 40,000      | ≈ 17,970       |
| Syscalls / request (without `futex`) | **3.45**      | **9.0**        |

### Highest values on the dashboard (whole matrix)

| Measurement                                 | Value                                       |
| ------------------------------------------- | ------------------------------------------- |
| Peak offered / handled rps                  | 8.02K / 8.01K (keep-alive on at 8000)       |
| Peak new connections accepted by the server | 6.32K/s                                     |
| Peak TIME_WAIT (host)                       | 20.9K, which is 74% of the 28232-port range |
| Peak ESTABLISHED                            | 7.22K                                       |
| Peak open connections (server)              | 3.59K                                       |
| Peak open fds                               | 3.53K of 1.05M                              |
| Peak listen overflows = drops               | 5.07K/s                                     |
| Peak TCP retransmits                        | 4.05K/s                                     |
| Peak k6 connecting p99 / p50                | 20.7 s / 19.4 s                             |
| Peak k6 dropped iterations                  | 6.0K/s                                      |
| Peak k6 failed requests                     | 75%                                         |
| Server latency p50 / p99 (handler only)     | max 300 µs / 497 µs                         |
| Peak Node CPU, user / system                | 77.4% / 35.5% of one core                   |
| Peak event loop delay p99                   | 71.8 ms                                     |
| Peak context switches                       | 216K/s                                      |
| Peak host CPU busy                          | 49.5%                                       |
| Peak conntrack usage                        | 6.5%                                        |
| `http_server_errors_total` / 5xx            | no data                                     |

### What I saw

1. **The switch worked.** Connections per request was about 0 with keep-alive and about 1 without (max 1.01). With keep-alive on, 240,001 requests used 302 connections (one per allocated VU plus a few others). With it off, almost every request opened its own connection.
2. **Up to 4000 rps both modes held the target.** The achieved rps was 3,999.8 and 3,999.7, and neither run had failed requests. At 8000 rps only keep-alive on survived.
3. **Keep-alive off collapsed at 8000 rps.** The server accepted at most 6.32K connections/s, listen overflows and drops reached 5.07K/s, retransmits 4.05K/s, k6 connect time reached about 20 s, and dropped iterations reached 6.0K/s. Failed requests went up to 75%.
4. **The k6 errors in that run:**

   ```bash
   Get "http://localhost:8000/health": dial: i/o timeout
   Get "http://localhost:8000/health": read tcp 127.0.0.1:52086->127.0.0.1:8000: read: connection reset by peer
   Get "http://localhost:8000/health": request timeout
   ```

   None of them is `cannot assign requested address`, so the ephemeral ports were not what ran out.

5. **Node did not report any of it.** `http_server_errors_total` and the 5xx panel had no data during the collapse. The connections were dropped in the kernel accept queue, before Node saw them.
6. **The tail got worse, the median did not.** At 4000 rps the median request time was the same (0.24 ms on, 0.18 ms off), but p95 went from 0.58 ms to 14.3 ms and max from 23.5 ms to 66.7 ms. Server-side handler latency stayed under 0.5 ms across the whole matrix, so the extra time is in connection setup and queueing, not in the handler.
7. **The connection itself was cheap on loopback.** Median connect time was 0.094 ms.
8. **Bytes per request changed by exactly the header size.** Sent bytes went from 76 to 95 (+19 = `Connection: close\r\n`). Received bytes went from 166 to 138 (-28). This is what you get when the response carries `Connection: close` instead of `Connection: keep-alive` plus `Keep-Alive: timeout=5` (47 bytes versus 19 bytes).
9. **Keep-alive off needs 2.6x the syscalls.** 9.0 syscalls per request versus 3.45. The extra ones are `accept4`, `epoll_ctl`, `setsockopt`, `shutdown`, `close` and `io_uring_enter`. With keep-alive on there is no `accept4` or `close` at all in the sample.
10. **TIME_WAIT was held by both sides.** At mid-run of the 4000 rps off run, 10,479 sockets were on the server side (`sport = :8000`) and 10,058 on the client side (`dport = :8000`), a split of about 51% / 49%. The total of 20.5K matches the dashboard peak of 20.9K (74% of 28232). With keep-alive on, TIME_WAIT was 0 and 1.
11. **TIME_WAIT never got near `rate x 60`.** At 4000 new connections/s that formula gives 240,000. The observed value was about 20K.
12. **The overflows in the 4000 off run all fall inside the strace window.** In the first 31 s of the 4000 off run (before the strace) listen overflows did not move (delta 0) and retransmits moved by 27. All 36,300 overflows and 32,531 of the retransmits happened between the `mid` and `post` snapshots, which include the 10 s `strace`. During that sample the server handled only about 1,800 requests/s while k6 kept sending 4000.
13. **It was not fds, conntrack, generator CPU or event loop saturation by itself.** Open fds peaked at 3.5K of 1.05M, conntrack at 6.5%, host CPU at 49.5%. Node CPU got close to one core and event loop delay reached 72 ms at the collapse.
14. **Recovery was immediate.** After the failing run, connections, TIME_WAIT and CPU went back to idle values without restarting the server.

> Note on the k6 failed-rate and connecting panels: they show the last value (75%, 7.33 s) for several minutes after the failing run ended, because the series is not marked stale in Prometheus. Treat them as "this happened in the last run", not as live values.

---

## Explanation

**Why keep-alive off costs more.** With a new connection per request, every request pays for the TCP handshake, the accept, registering the socket with epoll, setting socket options, the close sequence, and the TIME_WAIT bookkeeping. The strace shows this directly as 9.0 syscalls per request instead of 3.45, which is 2.6x. These calls run in the kernel and on the single Node event loop thread, so the same CPU serves fewer requests. With keep-alive, the connection is set up once, and after that a request is just `epoll_pwait`, `read` and `write`.

**Why the handler latency did not change.** The server's own latency histogram only measures the time inside the request handler (under 0.5 ms in every run). The extra cost of a new connection is paid before the handler starts and after it ends, in the kernel and in the accept loop. It shows up in the k6 timings (`connecting`, `blocked`, and the tail of the duration), not in the server's histogram.

**Why the 4000 rps off run still passed, and why it looked worse in the tail.** On loopback a handshake costs about 0.1 ms and there is no network round trip, so up to 4000 connections/s the server kept up. The tail (p95 14.3 ms, max 66.7 ms) is partly the connection overhead and partly the 10 s `strace -c -f` sample that I ran in the middle of the run. `strace` slowed the server to about 1,800 requests/s, the accept queue overflowed (36,300 `ListenOverflows`), and the retransmits and most likely the 13 dropped iterations came from that window. So the tail latency of this run is **not a clean number**. A clean measurement needs the same run without `strace`.

**Why keep-alive off collapsed at 8000 rps.** Node accepts connections on one thread. At about 6.3K new connections/s the thread was close to one full core (user 77% plus system 35%, including other threads), so the accept loop could not keep up. New connections waited in the kernel accept queue, which is limited by the listen backlog (`min(backlog, somaxconn)`). When it was full, the kernel dropped new connections (`ListenOverflows` equals `ListenDrops`). The client retransmits its SYN with growing delays, which is consistent with the connect times of about 20 s, and k6 reported `dial: i/o timeout`, `connection reset by peer` and `request timeout`. Dropped iterations in k6 are a consequence of this, not a generator problem: k6 ran out of VUs because each VU was stuck connecting, while host CPU stayed at 49.5%.

**Why the failure was silent in Node.** The drop happens in the kernel before `accept()` returns, so Node never sees the connection and no error is raised. `http_server_errors_total` and the 5xx panel stayed empty. This is the same lesson as E0.5: the fd and queue metrics show the problem, not the application's error log. For this failure the metrics to watch are `ListenOverflows`, `ListenDrops` and retransmits.

**Why TIME_WAIT was on both sides.** With `Connection: close`, either side can send the FIN first. The server calls `shutdown` and `close` right after the response (17,969 `shutdown` calls in 10 s), and k6 closes as soon as it has read the response. On loopback the two race, and the result was a split of about 51% / 49%. The side that closes first keeps the TIME_WAIT socket, so the server did **not** hold all of them, and my prediction was wrong.

**Why TIME_WAIT stayed at about 20K and ports did not run out.** The formula `rate x 60` assumes every TIME_WAIT socket lives 60 s. At 4000 connections/s it predicts 240,000 sockets, but the count settled at about 10K on each side. A TIME_WAIT socket is unique per 4-tuple, and it is recycled before 60 s in two ways: the client can reuse the port for a new connection to the same server (`tcp_tw_reuse = 2` allows this on loopback), and the server accepts a new SYN for a 4-tuple still in TIME_WAIT if the new connection is clearly newer. I did not measure the lifetime of the sockets directly, so this is the explanation that fits the numbers, not a proven one. The result is that on loopback with these sysctls, port exhaustion never happened at 4000 connections/s, and the real limit was the accept loop and the accept queue, not the ports and not `tcp_max_tw_buckets` (peak 20.9K against 65536).

Status of each claim:

| Claim                                                       | Status                                                               |
| ----------------------------------------------------------- | -------------------------------------------------------------------- |
| Keep-alive off opens about one connection per request       | **Observed** (connections per request max 1.01, 302 vs about 222K)   |
| Keep-alive off needs more kernel work per request           | **Observed** (9.0 vs 3.45 syscalls per request)                      |
| The off mode collapses by accept queue overflow at 8000 rps | **Observed** (overflows = drops, retransmits, connect timeouts)      |
| TIME_WAIT is held by both sides                             | **Observed** (`ss`: 10,479 server, 10,058 client)                    |
| TIME_WAIT sockets are recycled before 60 s                  | **Inferred** (20K observed vs 240K predicted, lifetime not measured) |
| The strace sample caused the overflows in the 4000 off run  | **Likely, not verified** (all overflows fall in the strace window)   |
| Kernel share of Node CPU is higher with keep-alive off      | **Not resolved** (the dashboard panel is too noisy at this zoom)     |

### Prediction vs observation

| Prediction                                               | Observed                                                                                                                                                                                                 | Verdict                                            |
| -------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------- |
| Max rps drops with keep-alive off                        | Both modes passed up to 4000 rps. At 8000 only keep-alive on survived (8.0K req/s vs a collapse at about 6.3K conn/s). The knee of off mode is between 4000 and 8000; the knee of on mode was not found. | **matches**, but the knee is not located           |
| About 60,000 TIME_WAIT at 1000 rps (240,000 at 4000 rps) | About 20.5K at 4000 rps, peak 20.9K                                                                                                                                                                      | **wrong**                                          |
| Ports run out from about 470 new connections/s           | No `cannot assign requested address`. TIME_WAIT peaked at 74% of the range                                                                                                                               | **wrong** (because of reuse and the 4-tuple limit) |
| Connections per request about 1 off and about 0 on       | max 1.01; 0.0013 on                                                                                                                                                                                      | **matches**                                        |
| Server closes first and holds the TIME_WAIT              | About half on each side                                                                                                                                                                                  | **wrong**                                          |
| More work in the kernel                                  | 9.0 vs 3.45 syscalls per request; the CPU split per run is not clear on the dashboard                                                                                                                    | **matches** (by syscalls, not by CPU split)        |
| `tcp_max_tw_buckets` could be reached                    | Peak 20.9K against 65536                                                                                                                                                                                 | **wrong**                                          |

**Verdict on the hypothesis:** confirmed with corrections. Keep-alive off costs about 2.6x the syscalls per request and collapses between 4000 and 8000 new connections/s, while keep-alive on handled 8000 rps without failures. TIME_WAIT piled up as predicted in direction, but at about 20K instead of 240K, and it sat on both sides. The part I got wrong is what limits the server: not the ephemeral ports or the TIME_WAIT cap, but the single accept thread and the accept queue, and Node did not report the failure.

### Limitations

- Only one repeat per run, so no variance is available.
- The `strace` sample ran inside every measured run and disturbed the off runs. The 4000 off tail latency and the retransmit and overflow counts of that run are contaminated, and the 8000 off collapse may have been started or made worse by it. The next run needs the `strace` and `ss` sampling moved out of the measured window.
- Loopback hides the network cost. A real handshake needs a round trip (and TLS needs more), so connect time would be much higher than 0.09 ms.
- The knee of keep-alive on was not found (nothing above 8000 rps was run), and the knee of off mode is only bracketed (4000 to 8000).
- Node CPU user and system split per run is not extracted, only the peaks of the whole matrix.

---

## Cost

- **Cost of a connection per request:** 2.6x the syscalls per request, 19 extra bytes sent per request, a worse tail latency (p95 14.3 ms vs 0.58 ms, with the strace caveat), and about 20K TIME_WAIT sockets at 4000 connections/s. On loopback the handshake itself is cheap (0.09 ms), over a real network each request would also pay at least one round trip.
- **Cost of the failure:** at the top rate the server accepts about 6.3K connections/s and then drops the rest in the kernel. Clients see connect timeouts of seconds, and the server shows no error.
- **Cost of the fix:** keep-alive (or a connection pool) is nearly free. With it, 240,001 requests used 302 connections and the server handled 8.0K req/s with no failures.

## Takeaways

1. Reuse connections everywhere: the load balancer to the API, the API to Kafka, Redis and Postgres (pools), and clients to the API. This is the main design rule for the following stages.
2. A new connection per request costs about 2.6x the syscalls, and the failure starts at the accept queue, not at the port range. Watch `ListenOverflows`, `ListenDrops` and retransmits, not only TIME_WAIT.
3. TIME_WAIT is not `rate x 60`. It is limited by the distinct ports and by reuse, and with `Connection: close` it can sit on both sides.
4. The failure is silent in Node: no server error is raised, so the application metrics look clean.
5. `strace -c -f` is not free. Don't run it inside a measured window; capture it in a separate short run.
6. Test on a real network (or at least add latency) before quoting the cost of a connection: loopback understates it.
