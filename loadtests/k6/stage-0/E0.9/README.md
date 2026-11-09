# E0.9 Payload size scaling

Stage 0 experiment. Question: when the request body gets bigger, how fast does the capacity of `POST /echo` fall, and where does the extra CPU go?

Everything runs from one command: `./step.sh` (in this folder). Results go to `loadtests/k6/stage-0/results/e09/`.

## 1. Run the server

`step.sh` starts and stops the server itself when `MANAGE_SERVER=1` (the default), through the existing launcher:

```bash
./labs/stage-0-foundations/script/server-runner.sh      # what step.sh runs for me, pinned to CPUs 0,6 (default)
```

If I want to start it by hand and let `step.sh` attach: start it, then run `MANAGE_SERVER=0 ./step.sh`.

Pinning: server on CPUs `0,6` (`SERVER_CPUS`, same default as `server-runner.sh`), k6 on `1,2,3,7,8,9` (`K6_CPUS`, the default in `runner.sh`). I should check with `lscpu -e` that 0 and 6 are hyperthread siblings of one core on this machine (I think so, not verified).

Check before the first run:

```bash
PID=$(. /tmp/eventpulse-e0.5/server.env; echo $SERVER_PID)
grep 'open files' /proc/$PID/limits
sysctl net.core.rmem_max net.core.wmem_max net.ipv4.tcp_rmem net.ipv4.tcp_wmem
ip link show lo | head -1          # loopback MTU (expect 65536)
ss -s                              # baseline socket counts
```

`step.sh` also writes these into `results/e09/env.txt`. It POSTs a 100 KB body once at the start (`echo_check`) and stops if the server answers with an error, because a body size limit in the server would make the 100 KB runs measure errors.

## 2. What I am testing

**Hypothesis:** `/echo` throughput falls roughly with payload size once JSON parse and serialize dominate.

**Variable:** request body size: 100 B, 1000 B, 10 000 B, 100 000 B (decimal: 1 KB = 1000 B here).
**Route:** `POST /echo`, `Content-Type: application/json`. The body is built once in k6's init code with exactly the right number of bytes.
**Shape of the JSON (second variable, second pass):**

- `SHAPE=obj` (first pass): many small fields like `"k12":"val12abcdef"` and `"k13":103027`, padded to the size with one last string. This is the expensive kind to parse.
- `SHAPE=str` (second pass): one big string field. Cheap to parse. If `str` is much faster than `obj` at the same size, then the cost is about the structure of the JSON and not only the bytes.

**Matrix (first pass):** 4 sizes x 5 rate steps x 3 repeats = 60 measured runs. Each run = 10 s warm-up at 25% of the rate + 2 s gap + 60 s measured part, plus about 8-15 s settle. Sizes alternate inside each step and repeat. Plus 4 probe runs (one per size). Expected total: **about 92 minutes** (`DRY_RUN=1 ./step.sh` prints it).

| size      | rate ladder (req/s)             |
| --------- | ------------------------------- |
| 100 B     | 4000, 8000, 12000, 16000, 20000 |
| 1000 B    | 4000, 8000, 11000, 14000, 18000 |
| 10 000 B  | 1500, 3000, 4500, 6000, 8000    |
| 100 000 B | 250, 500, 750, 1000, 1300       |

The ladders are built around my predicted knee of each size (below). The 5 steps bracket the knee. If a size shows no bend inside its ladder, I run a second ladder for that size only (`RATES_<size>="..."`, `SIZES="<size>"`).

### My predictions (written before the run)

Model: the server handles one request in `t(S) = a + b * S` microseconds of CPU, where S is the body size in KB. Node runs the JS on one thread, so the maximum rate is about `1 / t(S)`, and the latency bends (the knee) a bit earlier, at about 80% of that.

- `a` = 60 us per request. This is a guess for the fixed cost (HTTP parsing, routing, a few syscalls, response). It means about 16 700 req/s with no body. **I should replace it with 1 / (my E0.1 `/health` knee) before running if that number is very different.**
- `b` = 10 us per KB. My guess for the shape `obj`: JSON.parse about 4, JSON.stringify about 3, joining chunks and decoding to a string about 1, kernel copies and extra read/write syscalls about 2 (not verified, all four are guesses).

| size           | t(S) = 60 + 10 \* KB | max rate 1 / t | knee (about 80%) | bytes/s at max | JSON share (7 us/KB) |
| -------------- | -------------------- | -------------- | ---------------- | -------------- | -------------------- |
| 100 B (0.1 KB) | 61 us                | 16 400 /s      | about 13 000 /s  | 1.6 MB/s       | 0.7 / 61 = about 1%  |
| 1 KB           | 70 us                | 14 300 /s      | about 11 400 /s  | 14 MB/s        | 7 / 70 = 10%         |
| 10 KB          | 160 us               | 6 250 /s       | about 5 000 /s   | 62 MB/s        | 70 / 160 = 44%       |
| 100 KB         | 1 060 us             | 940 /s         | about 750 /s     | 94 MB/s        | 700 / 1060 = 66%     |

What follows from the table:

1. From 100 B to 100 KB the body grows 1000 times but the capacity falls only about 17 times (16 400 / 940). So "falls with payload size" is only true for the big sizes. For 100 B vs 1 KB I expect almost no difference (about 13% less), because `a` dominates.
2. From 10 KB to 100 KB the capacity should fall about 6.6 times (6250 / 940), close to the 10x of the size, because there `b * S` dominates.
3. Bytes per second goes UP with size (1.6 -> 94 MB/s) while requests per second goes DOWN. At 100 KB the server is limited by bytes, at 100 B by requests.
4. JSON share of CPU rises from about 1% to about 66%. GC share: under 5% for the small sizes, maybe 5-15% at 100 KB (a guess).
5. `SHAPE=str` (second pass): parse maybe 0.5 us/KB instead of 4, so `b` about 5 us/KB, t(100 KB) = 60 + 500 = 560 us, max about 1 800 /s. That is about 1.9x better than `obj` at 100 KB and almost the same at 100 B.

## 3. Instrumentation needed

**prom-client defaults used (job `eventpulse-nodejs`):** `process_cpu_seconds_total`, `process_cpu_user_seconds_total`, `process_cpu_system_seconds_total`, `process_resident_memory_bytes`, `process_open_fds`, `process_max_fds`, `process_network_receive_bytes_total`, `process_network_transmit_bytes_total`, `nodejs_gc_duration_seconds_sum`, `nodejs_heap_size_used_bytes`, `nodejs_external_memory_bytes`, `nodejs_eventloop_lag_p99_seconds`.

**Custom server metrics used:** `http_requests_total`, `http_request_duration_seconds_bucket`, `http_requests_in_flight`, `http_connections_total`, `eventpulse_echo_body_bytes_sum` / `_count`, `eventpulse_event_loop_delay_seconds`.

**k6 metrics used (through runner.sh remote write):** `k6_http_reqs_total`, `k6_http_req_duration_seconds` (native histogram), `k6_http_req_failed_rate`, `k6_data_sent_total`, `k6_dropped_iterations_total`, `k6_vus`. Tags that become labels: `size`, `shape`, `phase`, `stage` (`warmup` or `steady`), plus `testid` from runner.sh.

**node_exporter metrics used (job `node`):** `node_cpu_seconds_total`, `node_context_switches_total`, `node_network_receive_bytes_total` and `_transmit_bytes_total` (device `lo`), `node_netstat_Tcp_InSegs`, `node_netstat_Tcp_OutSegs`, `node_netstat_Tcp_RetransSegs`, `node_netstat_TcpExt_ListenOverflows`, `node_netstat_TcpExt_ListenDrops`, `node_sockstat_TCP_tw`, `ep_listen_recvq` (optional, needs the textfile exporter running).

**MISSING - see missing-metrics.txt:** `eventpulse_echo_parse_seconds` and `eventpulse_echo_stringify_seconds` (optional: direct JSON.parse / JSON.stringify time).

Assumptions I could not check because the inventory has no labels: `http_*` metrics have a `route` label, `eventpulse_event_loop_delay_seconds` has a `quantile` label, `k6_http_req_duration_seconds` is a native histogram. Verify queries (paste in Grafana Explore after ONE short k6 run, see 5b):

```promql
count by (size)(k6_http_reqs_total)
count by (route)(http_requests_total{job="eventpulse-nodejs"})
count by (le)(eventpulse_echo_body_bytes_bucket)
count by (quantile)(eventpulse_event_loop_delay_seconds)
count(k6_http_req_duration_seconds_bucket)        # expected: empty (native histogram has no _bucket series)
```

The `count by (le)(eventpulse_echo_body_bytes_bucket)` result also tells me the bucket boundaries of the body-size histogram. If the top finite bucket is below 100 000, the histogram is clamped for big bodies, which is why my panel uses sum / count instead of buckets.

## 4. Grafana panels

Dashboard: `infra/observability/dashboards/stage-0/E0.9 - Payload size scaling.json`. `[w]` = `$__rate_interval`. Job filters omitted. "masked" = shown only while that size is running (the `and on() (sum(rate(k6_http_reqs_total{size="S",stage="steady"}[w])) > 0)` trick), because server metrics have no `size` label.

| #                    | Panel                                 | Query idea                                                                                            | What it shows                   |
| -------------------- | ------------------------------------- | ----------------------------------------------------------------------------------------------------- | ------------------------------- |
| **Row 1: verdict**   |                                       |                                                                                                       |                                 |
| 1                    | Handled rate by size (k6)             | `sum by (size)(rate(k6_http_reqs_total{stage="steady"}[w]))`                                          | where each size flattens = knee |
| 2                    | Server-side handled rate              | `sum(rate(http_requests_total{route="/echo"}[w]))`                                                    | same, counted by the server     |
| 3                    | Latency p99 by size (k6)              | `histogram_quantile(0.99, sum by (size)(rate(k6_http_req_duration_seconds{stage="steady"}[w])))`      | the bend                        |
| 4                    | Latency p50 by size (k6)              | same with 0.5                                                                                         | median vs tail                  |
| 5                    | Payload throughput                    | `sum by (size)(rate(k6_data_sent_total{stage="steady"}[w]))`                                          | bytes/s rises while req/s falls |
| 6                    | k6 failed rate                        | `avg by (size)(k6_http_req_failed_rate{stage="steady"})`                                              | errors                          |
| **Row 2: mechanism** |                                       |                                                                                                       |                                 |
| 7                    | Mean body size seen by server         | `sum(rate(eventpulse_echo_body_bytes_sum[w])) / sum(rate(..._count[w]))`                              | proof the size is right         |
| 8                    | CPU per request, by size (masked)     | `rate(process_cpu_seconds_total[w]) / sum(rate(http_requests_total{route="/echo"}[w])) * 1000000`     | t(S) = a + b\*S                 |
| 9                    | CPU per request user vs system        | same with user / system counters                                                                      | JS vs kernel share              |
| 10                   | JSON parse / stringify time (MISSING) | `rate(..._parse_seconds_sum[w]) / rate(..._count[w]) * 1000000`                                       | direct JSON cost                |
| 11                   | Server network bytes in / out         | `rate(process_network_receive_bytes_total[w])` and transmit                                           | echo symmetry                   |
| 12                   | Server handler p99 / p50              | `histogram_quantile(0.99, sum by (le)(rate(http_request_duration_seconds_bucket{route="/echo"}[w])))` | handler vs queueing             |
| 13                   | GC fraction                           | `sum by (kind)(rate(nodejs_gc_duration_seconds_sum[w]))`                                              | GC cost                         |
| 14                   | Heap, external, RSS                   | `nodejs_heap_size_used_bytes`, `nodejs_external_memory_bytes`, RSS                                    | where bodies live               |
| 15                   | In-flight requests                    | `http_requests_in_flight`                                                                             | Little's Law                    |
| **Row 3: resources** |                                       |                                                                                                       |                                 |
| 16                   | Server CPU (cores) user vs system     | `rate(process_cpu_user_seconds_total[w])`, system                                                     | saturation near 1 core          |
| 17                   | Pinned CPUs busy %                    | `100 * (1 - rate(node_cpu_seconds_total{mode="idle",cpu=~"0\|6"}[w]))`                                | JS thread limit                 |
| 18                   | Host CPU by mode                      | `100 * sum by (mode)(rate(node_cpu_seconds_total{mode!="idle"}[w]))`                                  | system / softirq growth         |
| 19                   | Loopback bytes/s                      | `rate(node_network_receive_bytes_total{device="lo"}[w])`                                              | data through the kernel         |
| 20                   | TCP segments/s                        | `rate(node_netstat_Tcp_InSegs[w])`, OutSegs                                                           | segments per request            |
| 21                   | Context switches/s                    | `rate(node_context_switches_total[w])`                                                                | trend only                      |
| **Row 4: validity**  |                                       |                                                                                                       |                                 |
| 22                   | Dropped iterations                    | `sum by (size)(rate(k6_dropped_iterations_total[w]))`                                                 | must be 0                       |
| 23                   | Generator CPUs busy %                 | cpu `1\|2\|3\|7\|8\|9`, 100 - idle                                                                    | keep under 70%                  |
| 24                   | k6 VUs                                | `max by (size)(k6_vus)`                                                                               | queueing visible                |
| 25                   | Event loop delay                      | `eventpulse_event_loop_delay_seconds`, `nodejs_eventloop_lag_p99_seconds`                             | loop blocking                   |
| 26                   | New connections/s                     | `rate(http_connections_total[w])`                                                                     | keep-alive check                |
| 27                   | fds and TIME_WAIT                     | `process_open_fds`, `process_max_fds`, `node_sockstat_TCP_tw`                                         | not expected to matter          |
| 28                   | Listen overflows, drops, retransmits  | `rate(node_netstat_TcpExt_ListenOverflows[w])` etc.                                                   | must be 0                       |
| 29                   | Accept queue                          | `ep_listen_recvq`                                                                                     | optional exporter               |

The dashboard has one variable, the datasource `ds`. I pick my Prometheus when I import it.

## 5. Run it

All commands from `loadtests/k6/stage-0/E0.9/` unless stated. Start the compose stack first (`docker compose -f infra/compose/docker-compose.yml up -d`).

**(a) Dry run** - prints the plan and the expected minutes, runs nothing:

```bash
DRY_RUN=1 ./step.sh
```

**(b) Small trial** - two sizes, one rate each, one repeat, no probes (about 3 minutes). Use it to see that the metrics exist (then run the verify queries from section 3):

```bash
SIZES="100 100000" RATES_100="4000" RATES_100000="500" REPEATS=1 PROBES=0 RESULTS_NAME=e09-trial ./step.sh
```

A trial is a trial: not used for headline numbers.

**(c) Full matrix (first pass, shape obj)** - one command, about 92 minutes. Run `sudo -v` first so the probes can use perf and strace:

```bash
sudo -v && ./step.sh
```

**(d) One case by hand** through `runner.sh` (server must already be running):

```bash
cd loadtests/k6/stage-0
RESULTS_NAME=e09-manual LABEL=e09-manual SCRIPT=E0.9/script.js ROUTE=/health \
  RATE=500 DURATION=30s WARMUP=10s PAYLOAD_BYTES=100000 SHAPE=obj ./runner.sh
```

`ROUTE=/health` is only for runner.sh's GET preflight. The script posts to `/echo` (`ECHO_ROUTE`).

**(e) Second pass, shape str** (cheap-to-parse JSON). The 100 KB ladder needs higher rates because I predict about 1 800 /s:

```bash
RATES_100000="500 1000 1500 2000 2500" SHAPE=str ./step.sh      # results in results/e09-str/
```

**Optional narrower re-run near a knee** (after pass one, for one size):

```bash
SIZES="10000" RATES_10000="4000 4500 5000 5500 6000" RESULTS_NAME=e09-fine ./step.sh
```

**Watch during a run (another terminal):**

```bash
watch -n1 'ss -s'
curl -s localhost:8000/debug/stats | jq
pidstat -u -p $(. /tmp/eventpulse-e0.5/server.env; echo $SERVER_PID) 1       # %usr vs %system of the server
```

(The `pidstat` above is light, but I should not run `strace` or `perf` by hand during measured runs.)

**Where results go:** `results/e09/runs.tsv` (testid, phase, start/end epoch, status), `results/e09/env.txt`, `results/e09/captures/<testid>.txt` (pre/post nstat and /proc snapshots; probe runs also perf and strace output), `results/e09/<timestamp>/*.json` (k6 summaries) and `manifest.md` per run.

Index of all captures: `grep -h '^=== ' results/e09/captures/*.txt`.

## 6. What this script changes on my machine

- Starts and stops the server with `kill <server pid>` (only when `MANAGE_SERVER=1`). The probe phase restarts the server once with `NODE_OPTIONS=--perf-basic-prof` (Node then writes `/tmp/perf-<pid>.map`, which grows during probes). Measured runs use a server started WITHOUT that flag.
- `sudo -v` (refresh sudo), `sudo -n perf record`, `sudo -n perf report`, `sudo -n strace -c -f -p <pid>`. Only in probe runs. Without sudo or without perf/strace they are skipped with a warning.
- `perf record` writes `captures/<testid>.perf.data`, owned by root. I can delete it with sudo when I no longer need it.
- Writes under `loadtests/k6/stage-0/results/`. Does NOT change sysctls, ulimits, tc rules, or the server code. Nothing needs restoring after Ctrl+C except the server, which the EXIT trap stops.

## 7. Analyze in Grafana

Import `infra/observability/dashboards/stage-0/E0.9 - Payload size scaling.json`. Set the time range to cover the whole matrix (the start and end epochs are in `runs.tsv`). Read the rows in this order: **4 Validity first** (is any run contaminated: dropped iterations, generator CPU, listen overflows), then **1 Verdict** (where is each knee), then **2 Mechanism** (CPU per request vs size, user vs system, JSON share), then **3 Resources**. Remember that k6 series are not marked stale after a run: the last value can linger for minutes.

## 8. When it is done

I bring the results back so `Explanation.md` can be written (list in the hand-off message of the generator, and below in short):

1. `results/e09/runs.tsv`, `env.txt`, and the `manifest.md` of any FAILED run.
2. k6 summary numbers (or the `*-lap.json` files): achieved rps, failed rate, dropped iterations, `http_req_duration{stage:steady}` p50/p90/p95/avg/max.
3. The capture files for the probe runs and for one representative measured run per size.
4. Dashboard screenshots (whole matrix, plus zoomed: Handled rate by size, Latency p99 by size, CPU per request by size, CPU per request user vs system, Mean body size, Generator CPUs, Dropped iterations).
5. Peak and last values from those legend tables, and every k6 error line from failing runs.
6. Anything I changed during the run, runs I discarded, and anything that surprised me.
