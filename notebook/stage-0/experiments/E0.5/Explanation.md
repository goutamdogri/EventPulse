# E0.5 Experiment explanations

## Hypothesis

With a low fd limit the server fails new connections with `EMFILE` once the limit is reached.

### Predictions

Let,
fd limit is 256
latency (request hold time) is 5000 ms

With idle server, recorded fd count is 33.
(fd count - connection count).

Little's law, L = λ × W [ λ = requests per second (rps) and W = latency in seconds]

concurrent requests, L = λ × 5

=> λ = (256 - 33) / 5

=> λ = 44.6 rps

So,

1. at max 223 concurrent connections are possible.
2. at max 44.6 requests per second are possible.

Server can use some fds for other open resources also.
So, actual numbers are less than the max numbers calculated.

---

## Setup

| Item             | Value                                                                                                                                          |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| Server           | plain Node `http` + `prom-client`, pinned to CPUs 0,6 (`taskset`), started through `server-runner.sh` in a subshell with `ulimit -n $FD_LIMIT` |
| Limit check      | `grep 'open files' /proc/<PID>/limits`                                                                                                         |
| Route under load | `/io?ms=5000` (every request holds its socket for 5 s, so open sockets ≈ concurrent requests)                                                  |
| Generator        | k6 on separate cores, metrics by remote write to Prometheus                                                                                    |
| Idle baseline    | 33 fds not caused by client connections, plus 1 connection from the Prometheus scrape = 34 total fds on the idle server                        |
| Dashboard        | `infra/observability/dashboards/stage-0/E0.5 - File Descriptor Exhaustion.json`                                                                |

Three runs, one variable changed at a time:

| Run | k6 executor                                    | Server fd limit | Purpose                                                |
| --- | ---------------------------------------------- | --------------- | ------------------------------------------------------ |
| 1   | `ramping-vus` (up to 400 VUs)                  | 256             | The experiment: closed-loop load against a low limit   |
| 2   | `ramping-vus` (up to 400 VUs)                  | 65535           | Control: same load, same route, only the limit differs |
| 3   | `ramping-arrival-rate` (peak offered ≈ 60 rps) | 256             | Open-model load against the same low limit             |

Metrics used: `process_open_fds`, `process_max_fds`, `nodejs_active_handles_total`, `http_connections_open`, `http_connections_total`, `http_server_errors_total`, event loop lag, process CPU, `up`, k6 VUs / requests / connection success / connection errors, and node_exporter system fd and TCP socket metrics.

## Method

1. Start the server with the chosen limit and confirm it in `/proc/<PID>/limits`.
2. Record the idle fd count (34) and the baseline excluding connections (33).
3. Run the k6 script through `runner.sh` and watch the dashboard live.
4. During the run: `ls -l /proc/$PID/fd | awk '{print $NF}' | cut -d: -f1 | sort | uniq -c` to see what the fds are (sockets vs anything else).
5. At the plateau of run 1, trace the accept syscall to see the kernel-level error: `sudo strace -f -tt -e trace=accept4,close,openat -p $PID -o /tmp/e05.strace`.
6. Repeat for the three runs above, with the same route and the same hold time.

Exact commands are in `loadtests/k6/stage-0/E0.5/README.md`.

---

## Results and observations

### Run 1: ramping VUs, limit 256

![ramping vu, limit 256](./E0.5%20-%20File%20Descriptor%20Exhaustion-vu-256.png)

### Run 2: ramping arrival rate, limit 256

![ramping arrival rate, limit 256](./E0.5%20-%20File%20Descriptor%20Exhaustion-rate.png)

### Run 3 (control): ramping VUs, limit 65535

![ramping vu, limit 65535](./E0.5%20-%20File%20Descriptor%20Exhaustion-vu-65k.png)

### Numbers side by side

| Measurement                                | Run 1: VU, 256  | Run 2: arrival, 256 | Run 3: VU, 65535 |
| ------------------------------------------ | --------------- | ------------------- | ---------------- |
| Peak open fds                              | 254             | 253                 | 434              |
| Peak open connections                      | 223             | 223                 | 401              |
| Total connections accepted (cumulative)    | 224             | 226                 | 402              |
| Peak k6 VUs                                | 400             | 229                 | 400              |
| Peak k6 requests/s                         | 401             | 60.2                | 80               |
| Peak k6 connection errors/s                | ≈ 350           | ≈ 15                | none             |
| Successful connections/s at the plateau    | ≈ 45            | ≈ 44                | up to 80         |
| `http_server_errors_total`                 | no data         | no data             | no data          |
| Scrape `up`                                | stayed 1        | stayed 1            | stayed 1         |
| Peak TIME_WAIT (system)                    | 5,899           | 521                 | 438              |
| Max event loop lag                         | 11.9 ms         | 19.2 ms             | 22.0 ms          |
| Max process CPU                            | 0.050 cores     | 0.064 cores         | 0.053 cores      |
| Peak RSS                                   | 121 MiB         | 117 MiB             | 128 MiB          |
| Latency of successful requests (p50 / p90) | ≈ 5.0 s / 5.1 s | ≈ 4.9 s / 5.1 s     | ≈ 5.0 s / 5.1 s  |

_Peak open fds are as reported by Prometheus. The strace shows the real peak was 256 in the limit-256 runs; see "The 253-254 vs 256 gap" below._

### What I saw

1. **The ceiling is real and flat.** In both limit-256 runs, open fds climbed with the load and then flattened at the limit line (the dashboard shows 253-254, the real value was 256). Open connections flattened at 223 while the k6 VUs kept rising to 400 (run 1) or 229 (run 2). Accepted connections stopped growing at 224-226 and never moved again.
2. **The connection ceiling matched the prediction exactly.** Predicted 223 concurrent connections (256 - 33); observed a peak of 223 in both runs. In run 2 the successful rate was about 44 per second against a predicted 44.6.
3. **Control run proves the limit was the cause.** With 65535 fds the same load produced 401 connections, 434 fds (= 401 + 33), zero connection errors, 80 requests/s (= 400 VUs / 5 s), and 0.66% max fd utilization.
4. **`EMFILE` happened in the kernel, but Node never reported it.** k6 recorded connection errors from the moment connections reached 223. `strace` on the server at the plateau showed `accept4` on the listening socket (fd 26) failing again and again with `EMFILE (Too many open files)`. Yet `http_server_errors_total` stayed empty in every run, so the error was never delivered to JavaScript and the process did not crash.

   ```bash
   $ sudo strace -f -e trace=accept4 -p 408333 2>&1 | grep -m5 EMFILE
   [pid 408333] accept4(26, NULL, NULL, SOCK_CLOEXEC|SOCK_NONBLOCK) = -1 EMFILE (Too many open files)
   [pid 408333] accept4(26, NULL, NULL, SOCK_CLOEXEC|SOCK_NONBLOCK) = -1 EMFILE (Too many open files)
   [pid 408333] accept4(26, NULL, NULL, SOCK_CLOEXEC|SOCK_NONBLOCK) = -1 EMFILE (Too many open files)
   [pid 408333] accept4(26, NULL, NULL, SOCK_CLOEXEC|SOCK_NONBLOCK) = -1 EMFILE (Too many open files)
   [pid 408333] accept4(26, NULL, NULL, SOCK_CLOEXEC|SOCK_NONBLOCK) = -1 EMFILE (Too many open files)
   ```

   ```bash
   sudo strace -f -tt -e trace=accept4,close,openat -p $PID -o /tmp/e05.strace
   # after ~10 s of plateau, Ctrl-C, then:
   grep -n -B2 -A4 EMFILE /tmp/e05.strace | head -60
   ```

   Output -

   ```bash
   541-464845 21:19:22.441708 close(255)       = 0
   542-464845 21:19:22.509456 accept4(26, NULL, NULL, SOCK_CLOEXEC|SOCK_NONBLOCK) = 255
   543:464845 21:19:22.609583 accept4(26, NULL, NULL, SOCK_CLOEXEC|SOCK_NONBLOCK) = -1 EMFILE (Too many open files)
   544-464845 21:19:22.610037 close(20)        = 0
   545-464845 21:19:22.610259 accept4(26, NULL, NULL, SOCK_CLOEXEC|SOCK_NONBLOCK) = 20
   546-464845 21:19:22.610494 close(20)        = 0
   547-464845 21:19:22.610739 accept4(26, NULL, NULL, SOCK_CLOEXEC|SOCK_NONBLOCK) = -1 EAGAIN (Resource temporarily unavailable)
   548-464845 21:19:22.610830 openat(AT_FDCWD, "/", O_RDONLY|O_CLOEXEC) = 20
   549:464845 21:19:22.710148 accept4(26, NULL, NULL, SOCK_CLOEXEC|SOCK_NONBLOCK) = -1 EMFILE (Too many open files)
   550-464845 21:19:22.710459 close(20)        = 0
   551-464845 21:19:22.710529 accept4(26, NULL, NULL, SOCK_CLOEXEC|SOCK_NONBLOCK) = 20
   552-464845 21:19:22.710711 close(20)        = 0
   553-464845 21:19:22.710930 accept4(26, NULL, NULL, SOCK_CLOEXEC|SOCK_NONBLOCK) = -1 EAGAIN (Resource temporarily unavailable)
   554-464845 21:19:22.711012 openat(AT_FDCWD, "/", O_RDONLY|O_CLOEXEC) = 20
   555:464845 21:19:22.809302 accept4(26, NULL, NULL, SOCK_CLOEXEC|SOCK_NONBLOCK) = -1 EMFILE (Too many open files)
   ```

5. **Already-established requests were not hurt.** Successful requests kept completing in about 5 s throughout the saturated period.
6. **Failed connections are fast.** In run 1 the p50 dropped to about 128 µs at the minimum (visible as the dip in the latency panel), because refused connections come back immediately.
7. **Run 1 produced a retry storm, run 2 did not.** With ramping VUs, connection errors reached about 350 per second and TIME_WAIT sockets on the host reached 5.9k. With the arrival-rate executor, errors stayed around 15 per second (about 44 served + 15 refused ≈ 60 offered) and TIME_WAIT peaked at 521.
8. **It was not CPU, event loop, or memory.** Process CPU stayed at 0.05-0.06 cores, event loop lag stayed in the same 5-20 ms band in all three runs (including the control), and the k6 generator CPUs stayed below the 70% line.
9. **The system fd table was nowhere near full.** `node_filefd_allocated / node_filefd_maximum` stayed around 2e-13 (essentially zero). The limit hit was the per-process limit, not the machine's.
10. **The accept queue never built up.** The listen/receive queue panel showed the receive queue at 0-1 throughout, while the listen backlog stayed at 511. Connections were not waiting in the backlog.
11. **Scrape health stayed UP.** The Prometheus scrape never failed during saturation.
12. **Recovery was immediate.** When the load ended, fds fell straight back to 34 without a restart.

> Note on the k6 failed-ratio panel: in runs 1 and 2 it jumps to 100% at the first failures and stays there, even though successful connections continued at about 45 per second. Treat that panel as a marker of "failures started here". The real picture comes from the connection success and connection error panels.

---

## Explanation

**Why 223.** Every accepted TCP connection is one fd. The process has 256 fds in total, and 33 are always taken by other things (stdio, epoll, the listening socket, `/metrics` plumbing, libs). That leaves 223 for connections. With every request held for 5 s, the number of open connections equals the number of in-flight requests, so Little's Law gives 223 / 5 = 44.6 rps as the ceiling. Both numbers were reproduced by the measurements.

**Why the clients get errors but Node reports nothing.** The `strace` output confirms the first half directly: once the fd table is full, the kernel's `accept4()` on the listening socket fails with `EMFILE`, repeatedly. The hypothesis was right about the cause.

`strace -f -tt -e trace=accept4,close,openat` trace shows exactly what libuv does next. One full cycle, copied from the capture (pid column removed):

```bash
21:19:22.609583 accept4(26, ...) = -1 EMFILE (Too many open files)
21:19:22.610037 close(20)        = 0        <- release the spare fd
21:19:22.610259 accept4(26, ...) = 20       <- accept the waiting client into that slot
21:19:22.610494 close(20)        = 0        <- drop it immediately
21:19:22.610739 accept4(26, ...) = -1 EAGAIN (Resource temporarily unavailable)   <- queue drained
21:19:22.610830 openat(AT_FDCWD, "/", O_RDONLY|O_CLOEXEC) = 20   <- re-create the spare fd
```

This is libuv's `EMFILE` handling (the "emfile trick"). It keeps one spare fd open (a handle on `/`), and when `accept4` fails with `EMFILE` it:

1. closes the spare fd, which frees exactly one slot,
2. accepts the waiting client into that slot,
3. closes that client immediately, so the client sees the connection closed right after the handshake,
4. repeats until `accept4` returns `EAGAIN` (nothing left waiting),
5. re-opens the spare fd (`openat("/")`).

Node's `net` layer never receives an error callback from this path, so `server.on('error')` never fires. That matches `http_server_errors_total` being empty. The client simply sees its connection closed straight away, which matches the listen queue panel: the receive queue stayed at 0-1 because waiting connections were drained and dropped, not queued.

Two more details visible in the trace:

- **The fd table really was full.** Just before the first `EMFILE` the server accepted a connection into **fd 255** (`accept4(...) = 255`), the highest slot allowed by a limit of 256 (fds 0-255). The next `accept4` failed. So the process was at 256 fds at that moment.
- **The spare fd is fd 20 here, and it is part of the 33-fd baseline.** That is why `223 connections + 33 baseline = 256`.

Status of each claim:

| Claim                                                        | Status                                                                                                |
| ------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| `accept4` fails with `EMFILE` at the limit                   | **Observed** (strace)                                                                                 |
| Node does not surface it to JS (no server error, no crash)   | **Observed** (`http_server_errors_total` empty, process alive)                                        |
| libuv accepts and immediately closes the waiting connections | **Observed** (strace: `EMFILE`, `close(spare)`, `accept4 = 20`, `close(20)`, `EAGAIN`, `openat("/")`) |
| The accept queue does not build up                           | **Observed** (receive queue 0-1 on the dashboard)                                                     |

Caveats on the trace: `strace -f` slows the traced process, so timing and rates in the capture are perturbed. In this slice each cycle drains a single connection, roughly every 100 ms, which is far below the ~350 refused connections per second that k6 reported at its peak. This trace is to establish the sequence of syscalls, not the rate.

**Why the server stayed healthy.** Already-open sockets keep their fds, so requests in flight finish normally. Only new connections are refused. The process is not slow or broken; it is just full. That is why CPU, event loop lag and memory looked normal and why latency of the successful requests did not change.

**Why scrapes survived (and why that is luck).** Prometheus keeps one keep-alive connection to `/metrics`. It was already established before saturation, so it already owned its fd and kept working. A fresh scraper connection arriving after saturation would have been refused like any client. In production this means monitoring can go blind at exactly the moment you need it.

**Why closed-loop and open-loop differ.** With `ramping-vus`, a VU whose connection fails finishes its iteration instantly and starts the next one. Many VUs therefore hammered the server with new connection attempts (about 350/s) while only about 45/s succeeded. That produced thousands of short-lived sockets (TIME_WAIT 5.9k) and wasted accept work. With `ramping-arrival-rate`, the offered load is fixed by the schedule, so refused requests cost only about 15 failed connections per second. The same server limit looks very different depending on the load model, which I cover in E0.2.

**What the control proves.** Run 3 used the same load, route and hold time and differed only in `ulimit -n`. It had zero errors and peaked at 434 fds, so the failures in runs 1 and 2 are caused by the fd limit, not by the route, the generator, CPU, or the network.

### Prediction vs observation

| Prediction                                                               | Observed                                                                                                                                                                                    | Verdict                                                |
| ------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------ |
| Max 223 concurrent connections                                           | 223                                                                                                                                                                                         | matches                                                |
| Max 44.6 rps                                                             | about 44-45 successful/s                                                                                                                                                                    | matches                                                |
| Real numbers a bit below the ceiling because other fds are used          | The 33-fd baseline was already counted in the prediction, so the connection ceiling was exactly 223, not lower. Dashboard fds showed 253-254, but strace shows the table really reached 256 | partly: I over-hedged, the ceiling was reached exactly |
| The server fails new connections with `EMFILE` once the limit is reached | `strace` shows `accept4` returning `EMFILE` at the limit                                                                                                                                    | **matches**                                            |
| `EMFILE` also shows up as a server error in Node                         | no server errors at all                                                                                                                                                                     | **wrong** at application level                         |
| Scrapes might fail near saturation                                       | scrapes kept working                                                                                                                                                                        | wrong (because the scrape connection already existed)  |

**Verdict on the hypothesis:** confirmed. New connections failed with `EMFILE` once the limit was reached, exactly where predicted, and `strace` shows it at the syscall level. The one part I got wrong is how it is reported: Node never surfaced `EMFILE` to the application, so the failure was visible only on the client side, in `strace`, and in the fd and connection metrics. A server that relies on its own error logging or `server.on('error')` would not notice this failure.

### The 253-254 vs 256 gap (mostly resolved)

The dashboard showed peak fds of 253-254 against a limit of 256, even though connections hit exactly 223 (223 + 33 = 256). The strace answers the main question: the process **did** reach 256. `accept4` handed out fd 255 and the very next `accept4` failed with `EMFILE`, which only happens when no slot below 256 is free. So the 253-254 on the dashboard is a measurement artifact, not a real gap.

Likely reasons the dashboard under-reports (not yet verified):

- Prometheus samples `process_open_fds` only once per scrape interval, so it can miss the instant the table is full.
- `prom-client` computes the value by listing `/proc/self/fd`, which itself needs an fd. When the table is completely full that read can fail, so the gauge may keep its previous value instead of showing 256.

To verify, sample the real count in a tight loop at the plateau (`ls` opens one fd of its own, so subtract 1): `while true; do ls /proc/$PID/fd | wc -l; sleep 0.01; done | sort -n | uniq -c | tail`, or read the `prom-client` source for how it handles a failed read.

---

## Cost

- **Cost of the failure:** at the limit the server serves a fixed 44.6 rps regardless of CPU. Everything beyond that fails instantly, and with a closed-loop client the failures turn into a retry storm that fills the host with TIME_WAIT sockets.
- **Cost of the fix:** raising `ulimit -n` is nearly free here (RSS peaked at about 128 MiB with 400 open connections and was 117-121 MiB with 223). The limit is a safety net, not a capacity plan. Raising it only moves the point where the next resource (memory, CPU, the downstream pool) becomes the limit.
- **Cost of not watching it:** with no alert on `process_open_fds / process_max_fds`, the first sign is failed connections with no server-side error. The dashboard panel for fd utilization is the cheapest early warning.

## Takeaways

1. Size and monitor `ulimit -n` per process: alert at about 70-80% fd utilization, not at 100%.
2. Count fds as `connections + baseline`, and budget the baseline.
3. A full fd table fails quietly: `accept4` returns `EMFILE` in the kernel, but Node reports no error, so new connections are dropped while the process looks healthy. Monitor fd utilization instead of waiting for an error log.
4. Prefer open-model load (arrival rate) for capacity tests, because closed-loop VUs amplify failures into a retry storm.
