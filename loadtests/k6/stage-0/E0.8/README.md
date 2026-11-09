# E0.8 Accept queue and backlog

Hypothesis (from the Stage 0 doc): a sudden burst of connections beyond the backlog causes connection resets or timeouts even though average load is low.

## 1. Run the server

New optional env var `LISTEN_BACKLOG` added in the server, empty = old behaviour. `server-runner.sh` needs no change because it `exec`s node and the environment passes through.

`step.sh` starts and restarts the server itself for every cell (the backlog is only read when the server starts), so I do not start it by hand. If I want to look at it manually:

```bash
LISTEN_BACKLOG=16 ./labs/stage-0-foundations/script/server-runner.sh      # pinned to CPUs 0,6 (default)
ss -tln '( sport = :8000 )'     # Send-Q column = backlog the kernel is really using
```

Pinning (native Linux): server on CPUs `0,6`, k6 on `1,2,3,7,8,9` (runner.sh default), the accept-queue sampler on CPU `4` (preflight checks that it is free). Loopback is used, so there is no real network latency or loss here; only the kernel's own SYN retry timers are realistic.

## 2. What I am testing

**Variable:** the size of the accept queue (listen backlog, and in one cell somaxconn). **Mechanism:** I block the Node event loop with `/stall?ms=1500`, so Node stops calling `accept()`. 0.2 s later I open BURST new connections at once. The kernel finishes the handshake itself and parks connections in the accept queue; when the queue is full it drops new attempts.

**Matrix** (17 measured cells + 1 probe, REPEATS=1, about 9 minutes; `DRY_RUN=1` prints the exact plan):

| Part             | Cells                                   | Settings                                                                                           |
| ---------------- | --------------------------------------- | -------------------------------------------------------------------------------------------------- |
| A: backlog sweep | `A_b{511,128,16,4}_n{50,200,1000}` (12) | somaxconn 4096 (never clamps), tcp_abort_on_overflow 0, stall 1500 ms, client timeout 10 s         |
| B1               | `B1_clamp_s16_b511`                     | somaxconn **16**, Node asks for 511                                                                |
| B2               | `B2_abort1_b16`                         | tcp_abort_on_overflow **1**, backlog 16, N=200                                                     |
| B3a / B3b        | `B3a_nostall_b4`, `B3b_nostall_b511`    | **no stall** (control), N=200, backlog 4 and 511                                                   |
| B4               | `B4_timeout2s_b16`                      | client timeout **2 s**, backlog 16, N=200                                                          |
| Probe            | `P_b16_n200`                            | backlog 16, N=200, with tcpdump and `ss -tan` dumps. Separate run, never used for headline numbers |

### My predictions (written before the run)

Facts I am using: the kernel uses `min(backlog, somaxconn)`; the queue holds `backlog + 1` connections (the check is "length > max"); when the queue is full a new SYN is dropped silently and the client retries its SYN after 1 s, then 2 s later (so at +1 s and +3 s), because the first retransmit timeout is 1 s and it doubles. (This comes from my reading of the kernel behaviour; I have not verified it on this machine yet.)

Timeline of one overflow cell: stall starts t=0, burst lands t=0.2 s, loop is free at t=1.5 s.

- Connections that fit in the queue: connect is instant, but the request waits until the loop is free: waiting about 1.5 - 0.2 = **1.3 s**.
- Connections that did not fit: SYN dropped at 0.2 s, retry at 1.2 s is dropped too (loop still blocked), retry at 3.2 s succeeds: connecting about **3.0 s**.
- The server's own request duration stays tiny because it starts when Node parses the request, after the queue wait.

Counts, with `overflows ~ 2 x (N - backlog - 1)` when that is positive (two waves dropped):

| backlog \ N |       50 |       200 |       1000 |
| ----------: | -------: | --------: | ---------: |
|         511 |        0 |         0 |  about 976 |
|         128 |        0 | about 142 | about 1742 |
|          16 | about 66 | about 366 | about 1966 |
|           4 | about 90 | about 390 | about 1990 |

Also: peak `Recv-Q` = backlog + 1 whenever N > backlog + 1; `Send-Q` = min(backlog, somaxconn); connections needing a SYN retry (`k6_e08_connect_slow_total`) about N - backlog - 1; ListenDrops >= ListenOverflows.

- **B1:** `Send-Q` shows 16, not 511, and the cell behaves like `A_b16_n200`.
- **B2:** with abort_on_overflow=1 I expect some resets, but only for connections that got past the SYN stage (SYN drops stay silent). I do not know how many.
- **B3b:** 0 overflows. **B3a:** I do not know: somewhere between 0 and a few.
- **B4:** client timeout 2 s is shorter than the 3.0 s the overflowed connections need: about 200 - 17 = 183 timeouts and 17 ok.
- **Probe:** ports with 3 SYNs about N - backlog - 1 = 183; gap SYN1 to SYN2 about 1.0 s, SYN2 to SYN3 about 2.0 s; during the stall `ss -tan` shows about 183 in SYN-SENT; after the stall (about 2.0 s) they are still SYN-SENT, waiting for the 3.2 s retry.

**What I expect the verdict to be:** half right. Overflow is real and visible in the counters, but with default settings it shows up as connect delays of about 1 s and 3 s, not as resets. Failures need a short client timeout (B4) or abort_on_overflow=1 (B2).

## 3. Instrumentation needed

- **prom-client defaults used:** `process_cpu_user_seconds_total`, `process_cpu_system_seconds_total`, `process_open_fds`, `nodejs_eventloop_lag_max_seconds`.
- **Custom server metrics used:** `http_connections_total`, `http_connections_open`, `http_request_duration_seconds`.
- **k6 metrics used:** `k6_http_req_connecting_seconds`, `k6_http_req_waiting_seconds`, `k6_http_req_duration_seconds`, `k6_http_reqs_total`, `k6_http_req_failed_rate`, `k6_iterations_total`, `k6_dropped_iterations_total`; **MISSING until the first run, see missing-metrics.txt:** `k6_e08_conn_ok_total`, `k6_e08_conn_timeout_total`, `k6_e08_conn_reset_total`, `k6_e08_conn_other_error_total`, `k6_e08_connect_slow_total` (already added in `script.js`).
- **node_exporter metrics used:** `node_netstat_TcpExt_ListenOverflows`, `node_netstat_TcpExt_ListenDrops`, `node_netstat_TcpExt_TCPSynRetrans`, `node_netstat_TcpExt_TCPTimeouts`, `node_netstat_Tcp_OutRsts`, `node_netstat_Tcp_PassiveOpens`, `node_cpu_seconds_total`, `node_context_switches_total`, `node_sockstat_TCP_tw`, `up`.
- **Textfile exporter:** `ep_listen_recvq`, `ep_listen_sendq` (started and stopped by `step.sh`; check that `infra/compose/textfile/ep_listen.prom` appears while it runs).

## 4. Grafana panels

Dashboard: `infra/observability/dashboards/stage-0/E0.8 - Accept queue and backlog.json`. `[w]` = `$__rate_interval`; job filters omitted.

| #     | Panel                               | Query idea                                                                                                          | What it shows                           |
| ----- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------- | --------------------------------------- |
| 1     | Connect time p99 per cell           | `histogram_quantile(0.99, sum by (cell)(rate(k6_http_req_connecting_seconds{kind="burst"}[w])))`                    | about 0 vs about 3 s                    |
| 2     | Waiting time p99 per cell           | same with `k6_http_req_waiting_seconds`                                                                             | about 1.3 s queue wait                  |
| 3     | Total request time p99 per cell     | same with `k6_http_req_duration_seconds`                                                                            | what the client felt                    |
| 4     | Outcomes per cell                   | `sum by (cell)(k6_e08_conn_*_total)`                                                                                | ok / timeout / reset / other            |
| 5     | Connections needing a SYN retry     | `sum by (cell)(k6_e08_connect_slow_total)`                                                                          | about N - backlog - 1                   |
| 6     | Accept queue vs backlog in use      | `ep_listen_recvq`, `ep_listen_sendq`                                                                                | queue length vs min(backlog, somaxconn) |
| 7     | ListenOverflows / ListenDrops per s | `rate(node_netstat_TcpExt_ListenOverflows[w])`, `...ListenDrops`                                                    | refused attempts                        |
| 8-9   | Overflows / Drops total (stat)      | `increase(...[$__range])`                                                                                           | totals for the selected range           |
| 10    | SYN retransmits, TCP timeouts, RSTs | `rate(TCPSynRetrans)`, `rate(TCPTimeouts)`, `rate(Tcp_OutRsts)`                                                     | retry timers, resets                    |
| 11    | Accepted by Node vs completed by k6 | `rate(http_connections_total[w])`, `rate(k6_http_reqs_total{kind="burst"}[w])`                                      | spike after the stall                   |
| 12    | Server-side vs client-side p99      | `http_request_duration_seconds_bucket{route!~"/metrics\|/stall"}` vs k6                                             | the server cannot see the queue wait    |
| 13    | Proof the loop was blocked          | `up{job="eventpulse-nodejs"}`, `nodejs_eventloop_lag_max_seconds`                                                   | scrape failures during stall            |
| 14-17 | Resource view                       | process CPU user/system, host CPU by mode, context switches, fds                                                    | where the cost lands                    |
| 18-23 | Validity                            | CPU of pinned groups, `k6_iterations_total` per cell, failed rate, TIME_WAIT, scrape health, idle overflow baseline | can I trust the run                     |

Verify the metrics exist (Grafana Explore):

```promql
count by (kind, cell) (k6_http_reqs_total)
count by (cell) (k6_e08_connect_slow_total)
count by (route) (http_request_duration_seconds_count{job="eventpulse-nodejs"})
ep_listen_sendq
```

## 5. Run it

```bash
cd loadtests/k6/stage-0/E0.8
chmod +x step.sh                       # the file may have lost its execute bit when copied
```

**(a) dry run** (prints cells and total minutes, changes nothing):

```bash
DRY_RUN=1 ./step.sh
```

**(b) one-cell trial** (about 1 minute; checks the whole chain, classification of errors, and the dashboard):

```bash
ONLY="A_b16_n200" PROBES=0 ./step.sh
```

**(c) full trial matrix** (17 cells + probe, REPEATS=1):

```bash
./step.sh
```

For real data: `REPEATS=3 ./step.sh` (about 25 minutes).

**(d) a single case by hand through runner.sh** (server and sysctls already set by me):

```bash
cd loadtests/k6/stage-0
CELL=manual BACKLOG=16 SOMAXCONN=4096 ABORT=0 BURST=200 STALL_MS=1500 TIMEOUT=10s \
  RESULTS_NAME=e08-manual LABEL=e08-manual SCRIPT=E0.8/script.js REPS=1 ./runner.sh
```

**(e) only part of the matrix:** `PART=A ./step.sh` or `PART=B ./step.sh`.

**Watch during a run (other terminal; cheap commands only):**

```bash
watch -n 0.2 "ss -tln '( sport = :8000 )'"
watch -n 1 "nstat -asz TcpExtListenOverflows TcpExtListenDrops"
```

**Where results go:** `loadtests/k6/stage-0/results/e08/`

- `summary.tsv` (**start here**): one row per run: outcomes, how many connections connected in <200 ms / about 1 s / about 3 s / 5 s or more, client p50/p99, peak queue, kernel counter deltas. Built by `summarize.sh` at the end of `step.sh` (you can re-run it any time: `./summarize.sh ../results/e08`). It needs no Prometheus.
- `captures/<testid>.req.csv`: **one line per burst connection** (start time, blocked, connecting, sending, waiting, receiving, duration, status, outcome), written by `script.js` and pulled out of k6's output. This is the raw per-connection record.
- `runs.tsv`: one line per run (testid, cell, start/end epoch, status, config check, effective Send-Q, peak Recv-Q). Use the epochs to zoom Grafana.
- `deltas.tsv`: nstat counters after minus before, per run (system-wide).
- `captures/<testid>.queue.csv`: accept queue sampled every 0.1 s (the high-resolution record).
- `captures/<testid>.txt`, `.pre.nstat`, `.post.nstat`; the probe also has `.pcap` and the `ss` censuses.
- `env.txt`: kernel, sysctls, versions, pinning. `<timestamp>/*.json`: k6 summaries from runner.sh.

If `cfg_check` in `runs.tsv` says `CONFIG_MISMATCH`, the kernel did not use the backlog I asked for and that cell is suspect.

## 5b. How everything is measured (Prometheus is only context)

The burst lasts about 1.5 s and Prometheus scrapes every 1 s, so Prometheus gives 1 or 2 points per cell. Node's own `/metrics` cannot even answer during the stall (scrape timeout 1 s), so the server series has a gap exactly when the interesting thing happens. I therefore do **not** use Prometheus as evidence for the mechanism. The evidence comes from four faster sources:

| Question                                                           | Source                                                                               | Resolution                 |
| ------------------------------------------------------------------ | ------------------------------------------------------------------------------------ | -------------------------- |
| What did each connection experience (connect time, wait, outcome)? | `captures/<testid>.req.csv`, one line per connection                                 | per connection, ms         |
| How full did the accept queue get, and when?                       | `captures/<testid>.queue.csv` (Recv-Q and Send-Q sampled by `ss`)                    | 10 Hz (every 0.1 s)        |
| How many attempts did the kernel refuse?                           | `deltas.tsv` (`nstat` before and after the run)                                      | exact totals per run       |
| Why (SYN retries, dropped ACKs, resets)?                           | probe run: tcpdump summary and `ss -tan` state counts at about 0.8 s and about 2.0 s | per packet / two snapshots |

Prometheus and Grafana still show the shape over the whole matrix (and the host and process CPU, the validity checks), but quote numbers from the files above.

### What to look at, in this order

1. `summary.tsv`, column `cfg_check` = `ok` and `eff_sendq` = min(backlog, somaxconn). If not, ignore that cell.
2. `peak_recvq` vs `eff_sendq` + 1: did the queue fill?
3. `c<200ms`, `c~1s`, `c~3s`, `c>=5s`: the groups of connections. Prediction for `A_b16_n200`: about 17 in `c<200ms`, about 183 in `c~3s`, none failed.
4. `overflows` vs my table (about 2 x (N - backlog - 1)); `drops` >= `overflows`.
5. `wait_p50` and `conn_p99`: about 1300 ms (queued group) and about 3000 ms (overflowed group).
6. `timeout` / `reset` columns: only B4 and B2 should be non-zero.
7. The probe's `.txt`: SYNs per client port and the gaps (about 1 s then about 2 s).
8. Only then the Grafana rows, for the overall picture.

**If I want Prometheus to resolve the event: a longer stall.** `STALL_MS` is a knob. With `STALL_MS=5000` the retry timeline changes (stall 0 to 5 s, burst at 0.2 s): SYNs dropped at 0.2 s, 1.2 s and 3.2 s, and the retry at 7.2 s succeeds. Predictions for such a run: connect time of the overflowed group about **7.0 s**, queued group waiting about **4.8 s**, overflows about **3 x (N - backlog - 1)**, `up{job="eventpulse-nodejs"}` at 0 for about 5 scrape points, k6 timeout 10 s still fine. Run it as a separate, separately labelled mini-matrix so it does not mix with the 1.5 s results:

```bash
STALL_MS=5000 RESULTS_NAME=e08-long ONLY="A_b16_n200 A_b4_n1000" PROBES=0 ./step.sh
```

## 6. What this script changes on my machine

| What                                                      | Command                                                                                                                                               | Restored how                                                                                                                                                    |
| --------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `net.core.somaxconn` and `net.ipv4.tcp_abort_on_overflow` | `sudo -n sysctl -qw ...` before every cell                                                                                                            | Original values are saved in `env.txt` and restored in the EXIT trap (also on Ctrl+C). If it ever fails it prints the exact `sudo sysctl -w ...` command to run |
| Server process                                            | started and stopped by `server-runner.sh` per cell (SIGTERM through the pid in `/tmp/eventpulse-e0.5/server.env`)                                     | Stopped in the EXIT trap                                                                                                                                        |
| `listen-queue-exporter.sh`                                | started in the background (not if it is already running)                                                                                              | Killed in the EXIT trap                                                                                                                                         |
| Queue sampler                                             | `taskset -c 4 bash -c ...` in the background                                                                                                          | Killed after every cell                                                                                                                                         |
| sudo ticket                                               | `sudo -v` once, then `sudo -n true` every 50 s in the background                                                                                      | Killed in the EXIT trap                                                                                                                                         |
| Probe only                                                | `sudo -n timeout 30 tcpdump -i lo ...` (captures only SYN and RST packets of port 8000), `sudo -n tcpdump -r` to read it, `sudo -n chown` on the pcap | Nothing to restore; the pcap stays in `captures/`                                                                                                               |

It deletes nothing. The server code change in `server-changes.diff` is additive.

## 7. Analyze in Grafana

Import `infra/observability/dashboards/stage-0/E0.8 - Accept queue and backlog.json` and pick the Prometheus datasource. Read the rows in order: 1 verdict, 2 mechanism, 3 resources, 4 validity. Zoom to one cell at a time with the epochs from `runs.tsv`. k6 series are not marked stale after a run, so the last value of a finished cell can linger for minutes: use the legend `max` per cell.

## 8. When it is done

Bring the results back so the Explanation can be written: `summary.tsv` (the whole matrix in one table), `runs.tsv`, `deltas.tsv`, `env.txt`, the `.req.csv` and `.queue.csv` of the key cells and the probe's `.txt`, dashboard screenshots with the time range visible, and every k6 error line.
