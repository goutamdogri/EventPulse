# EventPulse Load Lab v3: Panel Reference

A complete guide to every panel in `eventpulse-load-lab-v3.json`. For each panel you get:

1. every parameter (query, unit, axis, style, thresholds, colors), explained;
2. what the panel means overall;
3. the unique cases you will meet when reading the graph, and what to check next.

The data in the examples comes from your own Stage 0 runs (`/health` route, Node server pinned to CPUs 0 and 6, k6 pinned to 1,2,3,7,8,9, monitoring on 4,5,10,11).

---

## Contents

1. [How the dashboard is organised](#1-how-the-dashboard-is-organised)
2. [Dashboard-level settings and variables](#2-dashboard-level-settings-and-variables)
3. [Parameter glossary (used by every panel)](#3-parameter-glossary-used-by-every-panel)
4. [PromQL building blocks used in the queries](#4-promql-building-blocks-used-in-the-queries)
5. [Where each metric comes from](#5-where-each-metric-comes-from)
6. [Row 0: Verdict tiles](#6-row-0-verdict-tiles)
7. [Row 1: Did the test tell the truth? (k6)](#7-row-1-did-the-test-tell-the-truth-k6)
8. [Row 2: What is the Node process doing?](#8-row-2-what-is-the-node-process-doing-prom-client)
9. [Row 3: Is the machine or kernel the limit?](#9-row-3-is-the-machine-or-kernel-the-limit-node_exporter)
10. [Row 4: Live proof overlays](#10-row-4-live-proof-overlays)
11. [Row 5: Post-run analysis (collapsed)](#11-row-5-post-run-analysis-collapsed)
12. [Cross-panel diagnosis playbook](#12-cross-panel-diagnosis-playbook)
13. [Known limitations and gotchas](#13-known-limitations-and-gotchas)

---

## 1. How the dashboard is organised

The rows follow the order of the questions you ask during a load test.

| Row | Question it answers | Panels |
|---|---|---|
| 0. Verdict | Did anything go wrong? (peak values, at a glance) | 8 tiles |
| 1. k6 | Was the test valid, and what did clients experience? | 5 |
| 2. Node process | What is the server process doing? | 9 |
| 3. Machine and kernel | Is the OS, CPU or network the limit? | 10 |
| 4. Live proof overlays | What moved at the moment throughput stopped growing? | 2 |
| 5. Post-run analysis | One row of numbers per test run, and the knee curve | 2 (collapsed) |

Suggested reading order during a run:

1. Row 1 first. If the test was not valid (dropped iterations, VUs exhausted), nothing else can be trusted.
2. Row 2 and row 3 to find which resource moved.
3. Row 4 to see timing and cause together.
4. Row 5 after the whole series finishes.

---

## 2. Dashboard-level settings and variables

### Settings

| Setting | Value | Meaning |
|---|---|---|
| Time range | Last 15 minutes | Default window. Change it to cover your runs. For Row 5, it must cover the entire series. |
| Refresh | 5s | The dashboard re-queries every 5 seconds. Heavy queries (Row 5) are in a collapsed row so they do not run on every refresh. |
| Refresh choices | 1s, 5s, 10s, 30s, 1m | Offered in the picker. A 1s choice only works if Grafana is started with `GF_DASHBOARDS_MIN_REFRESH_INTERVAL=1s`, which your compose file does. |
| Shared tooltip (crosshair) | Enabled | Hover one panel and a cursor line appears on every panel at the same moment. This is the main analysis tool: you read all panels at one instant. |
| Time zone | Browser | Times follow your computer's zone (IST), not UTC. |

### Variables

| Variable | Type | Value | What it filters | How to use it |
|---|---|---|---|---|
| `ds` | Datasource | prometheus | Which Prometheus all panels query | Leave as is. |
| `testid` | Query, multi-select, includes All | `label_values(k6_http_reqs_total, testid)` | All k6 panels (`testid` label) | Select one run to analyse it, several to compare, All to see everything. |
| `instance` | Query, multi-select | `label_values(process_cpu_user_seconds_total, instance)` | Node app (prom-client) metrics | Your server, for example `host.docker.internal:8000`. |
| `node_instance` | Query, multi-select | `label_values(node_uname_info, instance)` | node_exporter metrics | The host, for example `host.docker.internal:9100`. |
| `server_cpus` | Text box | `0\|6` | CPU groups panel | Regex of logical CPUs where the Node server is pinned. |
| `k6_cpus` | Text box | `1\|2\|3\|7\|8\|9` | CPU groups panel | Regex of logical CPUs where k6 is pinned. |
| `spare_cpus` | Text box | `4\|5\|10\|11` | CPU groups panel | Regex of logical CPUs left for monitoring. |

Notes:

- The CPU variables are regular expressions, and Prometheus anchors them fully. `0|6` matches CPU `0` and CPU `6`, but not CPU `10`.
- When you select **All**, Grafana expands the variable to `.*`. With several selected values it becomes `a|b|c`.
- If you change your `taskset` pinning, change these three boxes. The panels do not guess.

---

## 3. Parameter glossary (used by every panel)

### Time-series panels

| Parameter | What it controls |
|---|---|
| Type: `timeseries` | A line or bar chart with time on the X axis. |
| Size (w x h) | Position on the 24-column grid. Width 24 is full width, 12 is half. |
| Series / query | One PromQL expression per line on the chart. The legend text comes from the "legend" field. |
| Unit | How values are formatted. `reqps` = requests per second, `ops` = operations per second, `percentunit` = a fraction 0-1 shown as 0-100%, `s` = seconds (auto-scaled to µs, ms, s), `bytes`, `Bps` = bytes per second, `µs` = microseconds, `short` = plain number with K and M suffixes. |
| Min / Max | Fixed axis bounds. `min 0` forces the axis to start at zero, so a line cannot visually exaggerate small changes. Not used on log axes (a log axis cannot show 0). |
| Scale: `linear` or `log` (base 10) | Linear shows equal differences equally. Log shows equal ratios equally, which is needed when values span several orders of magnitude (100 µs to 60 s). On a log axis each gridline is 10 times the previous one. |
| Draw style: `line` or `bars` | Lines suit continuous values. Bars suit events and rates where each interval is a separate count. |
| Line width | 2 for lines, 1 for bars. |
| Fill opacity | Transparency of the shaded area under a line. 10 = light, 60 = solid (bars), 0 = none. |
| Stacking: `none` or `normal` | `normal` adds series on top of each other, so the top edge is the total and each band is a part. |
| Show points: `never` | No dots on the line. |
| Span nulls: `false` | A gap in the data stays a gap. The line is not drawn across it, so missing data is visible. |
| Thresholds | Colour steps at given values, for example "transparent below, red from 0.05". |
| Thresholds style: `off`, `line`, `dashed` | `line` or `dashed` draws a horizontal reference line at each threshold value. A `transparent` base step means nothing is drawn below the first real threshold. |
| Colour: `palette-classic` or fixed | Automatic series colours, or one colour for the whole panel. |
| Overrides | Settings that apply to one named series: a fixed colour, a dashed line, a right-hand axis, a hidden axis, or a different unit. |
| Legend: table, bottom | A table under the chart with one row per series. |
| Legend calcs: `lastNotNull`, `max` | Two columns. "Last \*" is the latest non-empty value in the time range. "Max" is the highest value in the time range. Panels with only `max` omit the Last column. |
| Tooltip: `multi`, sorted descending | Hovering shows every series' value at that time, highest first. |

### Stat tiles (Row 0)

| Parameter | What it controls |
|---|---|
| Type: `stat` | One big number. |
| Reduce calc: `max` | The tile shows the **highest value in the selected time range**, not the latest value. So after a run ends the tile still shows the run's peak. |
| Colour mode: `background` | The whole tile turns the threshold colour (green, orange, red). |
| Graph mode: `area` | A small sparkline in the tile shows the trend. |
| Text mode: `value`, justify `center` | Only the number is shown, in the centre. |
| Unit and decimals | Formatting of the number. |
| Thresholds | Colour steps. A tile with a single `blue` step is purely informational. |

### Table and XY chart (Row 5)

| Parameter | What it controls |
|---|---|
| Instant query | A query evaluated once, at the end of the time range, instead of as a graph over time. Used to get one value per run. |
| Format: `table` | Result comes back as rows and columns, with labels (such as `testid`) as columns. |
| Transformations | Steps Grafana applies after the query: join queries by a column, drop unwanted columns, rename, convert types, sort. |
| Cell options: `color-background` | A table cell is coloured by its thresholds. |
| Series mapping: `auto` (XY chart) | The first numeric column becomes X and the following numeric columns become Y. |

---

## 4. PromQL building blocks used in the queries

| Piece | What it means |
|---|---|
| `{testid=~"$testid"}` | Keep only series whose label `testid` matches the selected variable (the `=~` operator means regex match). |
| `rate(counter[window])` | Average per-second increase of a counter over the window. Used for requests per second, CPU seconds per second, drops per second, and so on. |
| `$__rate_interval` | The window Grafana chooses for `rate()`. It is the larger of "chart step + scrape interval" and "4 x scrape interval". **The scrape interval comes from the Prometheus data source setting.** If that is left at the 15 s default, the minimum window is 60 s, which smears a 60 s test into a triangle. Set it to 1s in the data source provisioning file. |
| `sum(...)`, `max(...)`, `avg(...)` | Combine many series into one. `sum` adds, `max` takes the highest, `avg` takes the mean. |
| `by (label)` | Combine but keep one result per label value, for example `sum by (testid)` gives one line per run. |
| `or vector(0)` | If the left side is empty (no series exists), use 0 instead. Prevents "No data" when something has not happened yet, such as no dropped iterations. |
| `histogram_quantile(q, ...)` | Estimates a percentile (p50 = 0.5, p99 = 0.99) from a histogram. |
| `increase(x[range])` | Total increase of a counter (or a native histogram's counts) over the range. |
| `max_over_time(expr[range:step])` | A subquery: evaluate `expr` every `step` across the range and take the maximum. Used to find a run's peak throughput. |
| `clamp_min(x, 0)` | Never go below 0. Used for the event loop lag excess so a value slightly under the floor shows as 0. |
| `count by (cpu)(...)` and `scalar(...)` | Count logical CPUs, then use that number as a plain divisor. |
| `> 1000` (filter) | Drops values at or below 1000. Used so cost per request is only computed when meaningful traffic exists. |
| `$__range` | The full length of the dashboard's time range. |

---

## 5. Where each metric comes from

| Source | Metric prefix | Collected by | Notes |
|---|---|---|---|
| k6 load generator | `k6_...` | Pushed to Prometheus by remote write every 1 s | Labels include `testid`, `rate`, `series`. Latency is a native histogram (`k6_http_req_duration_seconds`) when native mode is on. |
| Node app | `nodejs_...`, `process_...`, `http_request_duration_seconds` | Scraped from `/metrics` on port 8000 every 2 s | The `nodejs_*` and `process_*` metrics come from prom-client defaults. The request histogram is the one you added. |
| Host | `node_...` | node_exporter on port 9100 (host network mode), scraped every 1 s | CPU, context switches, TCP counters, sockets, loopback bytes. |
| Textfile | `ep_listen_...` | `scripts/listen-queue-exporter.sh` writes a file, node_exporter reads it | Only exists while the runner (or the script) is active. |

---

## 6. Row 0: Verdict tiles

Eight tiles across the top. They show the **peak in the selected time range**. Use them as a smoke detector: any red tile tells you which row to open.

Two reminders for every tile:

- With the `testid` variable on All, the tile mixes every run in the time range. Select the run you care about.
- Tiles that use `rate()` inherit the data source scrape-interval problem. With a 60 s window, a 60 s test shows a peak about 8% below the true value.

### 6.1 Peak rps

**Query:** `sum(rate(k6_http_reqs_total{testid=~"$testid"}[$__rate_interval]))`

| Parameter | Value | Meaning |
|---|---|---|
| Unit / decimals | `reqps` / 0 | Requests per second, whole numbers. |
| Calc | max | Highest throughput in the range. |
| Thresholds | blue only | No pass/fail; informational. |

**Overall meaning:** the highest throughput k6 actually completed. This is "achieved" load. Compare it to the rate you asked for.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| About equal to the offered rate | k6 delivered the load and the server kept up | Row 1 latency to see how well |
| More than 1% below the offered rate | Load was not delivered | "Peak dropped /s" tile and the Offered vs achieved panel |
| About 8% below offered on a clean run | Rate window is longer than the run (smoothing artefact) | Set the data source scrape interval to 1s |
| "No data" | k6 did not push, or the `testid` selection matches nothing | Prometheus targets, `--web.enable-remote-write-receiver`, the runner's output |

### 6.2 Peak dropped /s

**Query:** `(sum(rate(k6_dropped_iterations_total{testid=~"$testid"}[$__rate_interval])) or vector(0))`

| Parameter | Value | Meaning |
|---|---|---|
| Unit / decimals | `ops` / 0 | Iterations per second. |
| Thresholds | green, red from 1 | Red when at least 1 iteration per second is dropped. |
| `or vector(0)` | | When k6 never drops anything the series does not exist; this makes it show 0. |

**Overall meaning:** iterations k6 wanted to start but could not, because no VU was free. This is how an open-model test shows overload. A run with sustained drops has not delivered its target rate.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| 0 | k6 started every iteration on time | None |
| A few ops/s (0.01-0.1% of the rate) | Usually k6 creating new VUs mid-run when the pre-allocated pool ran out | VUs active vs max. Set `preAllocatedVUs` equal to `maxVUs` |
| Large and growing with the offered rate | Server cannot keep up; VUs pile up | Latency panel, Row 2 CPU, event loop |
| Drops with low server CPU | k6 or the machine is the limit | CPU groups: k6 busiest CPU |

### 6.3 Peak error %

**Query:** `avg(k6_http_req_failed_rate{testid=~"$testid"})`

| Parameter | Value | Meaning |
|---|---|---|
| Unit / decimals | `percentunit` / 3 | Fraction shown as a percent, three decimals. |
| Thresholds | green, red from 0.001 | Red when 0.1% or more of requests fail. |

**Overall meaning:** the fraction of requests that failed: connection timeouts, resets, or non-2xx/3xx responses. Verify the metric name in Prometheus Explore if the tile looks wrong.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| 0.000% | No failures | None |
| Small but nonzero | A few timeouts or resets | k6 console for `i/o timeout` or `connection reset`, then Accept queue drops and TCP retransmits |
| High with fast responses | Server returned errors | Server logs |

### 6.4 Peak p99 (k6)

**Query:** `histogram_quantile(0.99, sum(rate(k6_http_req_duration_seconds{testid=~"$testid"}[$__rate_interval])))`

| Parameter | Value | Meaning |
|---|---|---|
| Unit / decimals | `s` / 1 | Auto-scaled to µs or ms. |
| Thresholds | green, orange from 50 ms, red from 200 ms | Example latency budgets. Tune to your target. |

**Overall meaning:** the 99th-percentile response time as seen by k6, using the native histogram. 99% of requests were at least this fast. It is the highest value across the time range.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Green but near the threshold | Approaching your latency budget | The knee panels |
| Red during a run with drops | Overload; latency here is for the served requests only and understates the real problem | Dropped iterations |
| "No data" | Native histogram not enabled in k6 or Prometheus | `K6_PROMETHEUS_RW_TREND_AS_NATIVE_HISTOGRAM=true` and `--enable-feature=native-histograms` |

### 6.5 Peak loop lag p99

**Query:** `max(nodejs_eventloop_lag_p99_seconds{instance=~"$instance"})`

| Parameter | Value | Meaning |
|---|---|---|
| Unit / decimals | `s` / 1 | |
| Thresholds | green, orange from 20 ms, red from 60 ms | The limits sit above prom-client's built-in 10 ms floor. |

**Overall meaning:** the longest typical delay before the event loop could run a timer. prom-client's sampler has a 10 ms resolution, so an idle server reads about 10 ms. Anything well above that means the main thread was busy.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| About 10-13 ms | Idle or light load (floor) | None |
| 20-60 ms | The loop is queueing work | Process CPU, Per-CPU utilisation |
| Over 60 ms | Heavy blocking or saturation | Event loop lag panels, flame graph |

### 6.6 Busiest core

**Query:** `max((1 - avg by (cpu) (rate(node_cpu_seconds_total{mode="idle",instance=~"$node_instance"}[$__rate_interval]))))`

| Parameter | Value | Meaning |
|---|---|---|
| Unit / decimals | `percentunit` / 0 | |
| Thresholds | green, orange from 70%, red from 90% | |

**Overall meaning:** the busiest single logical CPU anywhere on the machine. The query takes "1 minus the idle fraction" for each CPU, then the maximum. It does not know which CPU is which; use the CPU groups panel for that.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| 80-90% while all other tiles are green | A single thread is nearly saturated | Per-CPU utilisation |
| Near 100% | A thread is saturated | Whether it is the server's CPU or k6's |

### 6.7 Peak ListenOverflows/s

**Query:** `sum(rate(node_netstat_TcpExt_ListenOverflows{instance=~"$node_instance"}[$__rate_interval]))`

| Parameter | Value | Meaning |
|---|---|---|
| Unit / decimals | `ops` / 1 | |
| Thresholds | green, red from 0.001 | Any drop turns it red. |

**Overall meaning:** new connections dropped per second because the server's accept queue was full. Needs node_exporter in host network mode, otherwise it reads the container's counters and will always be 0.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| 0 | No accept-queue drops | None |
| Nonzero | Connection attempts are being refused | Accept queue drops, SYN retransmits, VUs and connection count (usually a connection burst) |

### 6.8 Peak FD usage

**Query:** `max(process_open_fds{instance=~"$instance"} / process_max_fds{instance=~"$instance"})`

| Parameter | Value | Meaning |
|---|---|---|
| Unit / decimals | `percentunit` / 1 | |
| Thresholds | green, orange from 60%, red from 80% | |

**Overall meaning:** how close the Node process is to its file descriptor limit. Each open connection uses one descriptor.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Under 1% | No risk (your limit is about 1 million) | None |
| Rising toward 80% | Connection storm or a leak | File descriptors panel, TCP sockets |

---

## 7. Row 1: Did the test tell the truth? (k6)

### 7.1 Offered vs achieved

**Position:** left half, 12 x 8.

| Series | Query | Style |
|---|---|---|
| Offered | `sum(rate(k6_http_reqs_total{...}[$__rate_interval])) + (sum(rate(k6_dropped_iterations_total{...}[$__rate_interval])) or vector(0))` | Grey dashed line |
| Achieved | `sum(rate(k6_http_reqs_total{...}[$__rate_interval]))` | Green line |

| Parameter | Value | Meaning |
|---|---|---|
| Unit | `reqps` | Requests per second. |
| Min | 0 | Axis starts at zero. |
| Scale / style | linear, line, fill 10 | |
| Legend | Last and Max | |

**Overall meaning:** "Offered" is the rate k6 intended to send: what it completed plus what it dropped. "Achieved" is what it actually completed. On a valid run the two lines lie on top of each other. The gap between them is the part of the load that never reached the server on time.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Lines overlap, flat plateau | Valid run | Latency |
| Lines overlap but shaped like a triangle | `$__rate_interval` is about as long as the run; the values are smeared and the peak is understated | Data source scrape interval |
| Achieved plateaus below Offered | The server (or k6) has hit its ceiling | Dropped iterations, CPU panels |
| Achieved drops to 0 mid-run | Server crashed or k6 stopped | Server and k6 logs |
| Offered itself lower than the target | Window smoothing again; compare with the table's "Offered rps" | Row 5 table |

### 7.2 Dropped iterations

**Position:** right half, 12 x 8.

**Query:** `(sum(rate(k6_dropped_iterations_total{...}[$__rate_interval])) or vector(0))` as bars.

| Parameter | Value | Meaning |
|---|---|---|
| Unit | `ops` | Dropped iterations per second. |
| Min | 0 | |
| Style | bars, fill 60, red | Each bar is a time slice. |
| Legend | Last and Max | |

**Overall meaning:** when k6 could not start an iteration because every VU was busy. It is the main validity check: a run with substantial drops did not deliver the rate you asked for, and its latency figures describe only the requests that were sent.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| No bars | Clean run | None |
| Tiny bars during ramp-up or bursts | VU creation overhead in k6 | VUs active vs max |
| Tall bars that grow with rate | Server saturated | CPU, event loop, latency |
| Bars only at the start of a run | k6 was still warming up VUs | Increase `preAllocatedVUs` |

### 7.3 VUs active vs max

**Position:** 8 x 8, third row.

| Series | Query | Style |
|---|---|---|
| VUs active | `max(k6_vus{testid=~"$testid"})` | Line |
| VUs max | `max(k6_vus_max{testid=~"$testid"})` | Grey dashed |

| Parameter | Value |
|---|---|
| Unit | short (a count) |
| Min | 0 |

**Overall meaning:** by Little's Law, in-flight requests equal rate x latency. At 23,000 rps and 0.2 ms response time you need only about 5 VUs; at 23,000 rps and 20 ms you need about 460. So "VUs active" is a sensitive measure of how slow responses are. "VUs max" is the number of VUs k6 has allocated so far. It starts at your `preAllocatedVUs` and grows only when needed.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Active far below max | Server is fast; k6 has spare VUs | None |
| Active climbs with rate | Latency is rising | Latency panel |
| Max grows during a run | k6 had to create VUs because of a latency spike. A stall of 20 ms at 23,000 rps needs about 460 simultaneous requests | Dropped iterations; set `preAllocatedVUs` to 1000 |
| Active reaches the configured `maxVUs` | k6 is out of VUs, so drops follow. This is a symptom of slow responses, not a harness limit to raise blindly | Server saturation |

### 7.4 Latency percentiles (k6 native histogram, log axis)

**Position:** right of the VU panel, 16 x 8.

| Series | Query | Colour |
|---|---|---|
| p50 | `histogram_quantile(0.5, sum(rate(k6_http_req_duration_seconds{...}[$__rate_interval])))` | Green |
| p90 | same with 0.9 | Yellow |
| p99 | same with 0.99 | Orange |
| p99.9 | same with 0.999 | Red |

| Parameter | Value | Meaning |
|---|---|---|
| Unit | `s` | Auto-scaled: 100 µs, 1 ms, 10 ms. |
| Scale | log base 10 | Needed because the lines span 100 µs to seconds. |
| Thresholds style | dashed, red at 0.05 s | A dashed 50 ms reference line (your example latency budget). |
| Min / Max | auto | Not set on a log axis. |

**Overall meaning:** client-side response time as k6 measures it, including time queued before the server picks up the request. The percentiles are computed from the native histogram over the rate window, so each point is "latency during this window". p50 is the typical request. p99 is the slowest 1%. p99.9 is the slowest 1 in 1000.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| All four lines flat and low | Server comfortably below capacity | None |
| p50 flat, p99 and p99.9 rising | Occasional stalls (GC, bursts, k6 hiccups) before the server is saturated | GC time, event loop lag max |
| p50 itself rising | Typical request is now queueing: you are at or past the knee | CPU panels, Saturation overlay |
| Lines far apart (a decade or more) | Heavy-tailed latency; averages would hide it | Which percentile matters for your SLO |
| Gaps in the lines | No traffic in that window (between runs) | Normal |
| Lines suddenly step and then stay flat across a wide area | Long rate window smoothing | Data source scrape interval |
| "No data" | Native mode not enabled | See tile 6.4 |

### 7.5 Error rate

**Position:** full width, 24 x 5.

**Query:** `avg(k6_http_req_failed_rate{testid=~"$testid"})`

| Parameter | Value | Meaning |
|---|---|---|
| Unit | `percentunit` | |
| Min / Max | 0 / 1 | Axis fixed to 0-100% so a tiny rate cannot look huge. |
| Style | line, fill 30, red | |

**Overall meaning:** fraction of failed requests over time. Failures with status 0 mean no HTTP response arrived (timeout or reset).

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Flat at 0 | Healthy | None |
| Bursts of failures when load rises | Connection timeouts or resets under overload | Accept queue drops, retransmits, VUs |
| Failures in only the first seconds | Cold start or connection ramp | Warm-up |

---

## 8. Row 2: What is the Node process doing? (prom-client)

### 8.1 Event loop lag (log axis)

**Position:** 8 x 8.

| Series | Query |
|---|---|
| p50 | `max(nodejs_eventloop_lag_p50_seconds{instance=~"$instance"})` |
| p99 | `max(nodejs_eventloop_lag_p99_seconds{...})` |
| max | `max(nodejs_eventloop_lag_max_seconds{...})` |

| Parameter | Value | Meaning |
|---|---|---|
| Unit | `s` | |
| Scale | log | |
| Thresholds | dashed lines: orange at 20 ms, red at 60 ms | Sit above the 10 ms sampler floor. |

**Overall meaning:** how late the event loop runs a timer that should fire on schedule. Node runs all your JavaScript on one thread, so a late timer means the thread was busy with something else. Because prom-client's sampler resolution is 10 ms, an idle server reports about 10 ms; read values as "10 ms + real delay".

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| All lines about 10 ms | Loop is idle | None |
| p99 and max rising with load, p50 flat | Bursts of delay while the loop is mostly free (early queueing) | Process CPU |
| p50 rising too | Loop is persistently late: saturation | Per-CPU utilisation, flame graph |
| Max spikes alone | A single long task or GC pause | GC time |

### 8.2 Process CPU (1.0 = one full core)

**Position:** 8 x 8, stacked.

| Series | Query |
|---|---|
| user | `sum(rate(process_cpu_user_seconds_total{instance=~"$instance"}[$__rate_interval]))` |
| system | `sum(rate(process_cpu_system_seconds_total{instance=~"$instance"}[$__rate_interval]))` |

| Parameter | Value | Meaning |
|---|---|---|
| Unit | `percentunit` | 100% = 1.0 = one logical CPU's worth of CPU time per second. |
| Min | 0 | |
| Stacking | normal | User plus system equals the total. |
| Thresholds | solid red line at 1.0 | One full core. |

**Overall meaning:** total CPU time the Node process consumed, split into user time (your JavaScript, V8, JSON, the HTTP parser) and system time (the kernel working for the process: sockets, TCP, `epoll`). The sum counts all of the process's threads, so it can exceed 1.0 when helper threads are busy.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Rises with load, below 1.0 | Spare capacity in the process | |
| Total near 0.9-1.0 | The process is using about a core: the single main thread is near its limit | Event loop lag, Busiest CPU |
| Above 1.0 | Helper threads (GC, libuv, V8 compile) are significant | GC time |
| System share large (over 30%) | Heavy kernel work: many syscalls or small packets | Context switches, CPU cost per request |
| Flat high line after a run ends | The next run has started (no cool-down), or something else is using CPU | Run timing in the manifest |

### 8.3 Active handles / requests

**Position:** 8 x 8.

| Series | Query | Meaning |
|---|---|---|
| active handles | `max(nodejs_active_handles{...})` | Open handles: mostly sockets, plus timers. |
| active requests | `max(nodejs_active_requests{...})` | Pending libuv requests (file, DNS). Normally 0 for `/health`. |

**Overall meaning:** how many connections Node is holding open. Compare it with k6's VUs: roughly one connection per active VU.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Tracks VUs max | Connections are reused (keep-alive working) | |
| Much lower than VUs | Many connections are still waiting in the kernel, not accepted yet | Listen queue, accept queue drops |
| Grows and never falls | Connection leak | File descriptors |
| Active requests above 0 | Work in the thread pool (file or DNS) | Handler code |

### 8.4 Client p99 vs server p99 / p50

**Position:** 9 x 8, log axis.

| Series | Query | Meaning |
|---|---|---|
| client p99 (k6) | native histogram p99 | What the client experienced. |
| server p99 (app) | `histogram_quantile(0.99, sum by (le) (rate(http_request_duration_seconds_bucket{...}[$__rate_interval])))` | Time measured inside the server, from the request hook to the response hook. |
| server p50 (app) | same with 0.5 | Typical server-side time. |

**Overall meaning:** the gap between the client line and the server lines is time spent **waiting before Node's handler ran**: in the kernel receive buffer, the event loop backlog, or k6 itself. If the server lines stay flat while the client line rises, the server is not getting slower per request; requests are queueing. This is the strongest evidence of a saturated single thread.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Server flat, client rising | Queueing before the handler | CPU panels |
| Both rising together | The handler itself got slower (for example a slow dependency or GC) | Heap and GC |
| Server p99 pinned exactly at a bucket edge (for example 99 µs) | Histogram resolution: all values fall in one bucket, so the reading is a bucket boundary, not a measurement | Add finer buckets between 50 µs and 5 ms |
| Server lines missing | The custom histogram is not registered or not scraped | Scrape target, histogram code |
| Client far above server even at low load | k6 or loopback overhead | CPU groups |

### 8.5 GC time

**Position:** 5 x 8, stacked.

**Query:** `sum by (kind) (rate(nodejs_gc_duration_seconds_sum{instance=~"$instance"}[$__rate_interval]))`

| Parameter | Value | Meaning |
|---|---|---|
| Unit | `percentunit` | Seconds of GC per second: the fraction of time spent in GC pauses. |
| Min | 0 | |
| Stacking | normal | Kinds add to the total. |
| Series | one per `kind` | minor (young generation), major (old generation), incremental, and weak callbacks. |

**Overall meaning:** how much of the server's time goes to garbage collection. Your values (about 0.1-0.2%) are negligible.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Under 1% | GC is not a factor | |
| Minor GC spikes | Allocation-heavy handling | Heap/RSS, allocation profile |
| Major GC rising over time | Heap growing: possible leak | Heap/RSS |
| GC spikes aligned with p99.9 spikes | GC pauses cause tail latency | Latency percentiles |

### 8.6 Heap / RSS

**Position:** 5 x 8.

| Series | Query | Meaning |
|---|---|---|
| heap used | `max(nodejs_heap_size_used_bytes{...})` | Live JavaScript objects. |
| RSS | `max(process_resident_memory_bytes{...})` | Total memory the process holds in RAM (heap, buffers, code, native). |

Unit `bytes`, min 0.

**Overall meaning:** memory health. Heap used shows a sawtooth (garbage collection) in a healthy server. RSS rises under load and stays flat afterwards.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Sawtooth that returns to baseline | Healthy | |
| Steady upward trend across runs | Memory leak | Heap snapshot |
| RSS much larger than heap | Native buffers or fragmentation | Buffer usage |
| Sudden drop to near zero | Process restarted | Server logs |

### 8.7 File descriptors

**Position:** 5 x 8, log axis.

| Series | Query | Style |
|---|---|---|
| open fds | `max(process_open_fds{...})` | Line |
| limit | `max(process_max_fds{...})` | Red dashed |

**Overall meaning:** descriptors in use against the limit. A log axis is used because the numbers differ by orders of magnitude (hundreds against about one million), so both lines stay visible.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Open far below the limit | No risk | |
| Open climbing toward the limit | Connection leak or storm; `EMFILE` errors follow | TCP sockets, `ulimit -n` |
| Limit very low (1024) | Default shell limit; raise it before big tests | |

### 8.8 CPU cost per request (µs)

**Position:** 12 x 8.

| Series | Query meaning |
|---|---|
| µs per request total | (process user + system CPU seconds per second) divided by (requests per second, only when above 1000), times 1,000,000 |
| of which kernel (system) | Same, using only system CPU |

| Parameter | Value |
|---|---|
| Unit | `µs` |
| Min | 0 |

**Overall meaning:** how much CPU time the process spends per request. It is the core number for the Stage 8 overhead ladder. In your data: about 40 µs per request, of which about 25% is kernel time. Remember that the measurement includes helper threads and Prometheus scraping, so it is slightly higher than the main thread's own cost.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Flat in the middle of a run | Stable per-request cost; this is the number to record | |
| Very high at the start and end of a run | Artefact: at low request rates fixed overheads (timers, scrapes) divide over few requests | Ignore the edges |
| Cost rising at the same rate | Code got slower, or CPU frequency or SMT contention changed | CPU governor, flame graph |
| Cost falling as rate rises | Batching effects: fewer wake-ups per request | |
| Kernel share rising | More syscalls per request | Context switches |

### 8.9 Event loop lag excess (minus 10 ms floor)

**Position:** 12 x 8, linear.

**Query:** `clamp_min(max(nodejs_eventloop_lag_p99_seconds{...}) - 0.01, 0)`

| Parameter | Value | Meaning |
|---|---|---|
| Unit | `s` | |
| Min | 0 | |
| Thresholds | line: orange at 10 ms, red at 50 ms | Excess delay budget. |
| Colour | orange | |

**Overall meaning:** the real queueing delay in the event loop, after removing prom-client's 10 ms sampler floor. On a linear axis small differences are visible; on the log panel they are compressed.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Under 1 ms | Idle loop | |
| 2-8 ms during runs at the knee | Loop is queueing under load (your data) | Process CPU, latency |
| Above 10 ms steady | Loop saturated | |
| Sharp isolated spikes | One-off long tasks | GC, startup |

---

## 9. Row 3: Is the machine or kernel the limit? (node_exporter)

### 9.1 Per-CPU utilisation

**Position:** 8 x 8.

**Query:** `(1 - avg by (cpu) (rate(node_cpu_seconds_total{mode="idle",...}[$__rate_interval])))`, legend `cpu {{cpu}}`.

| Parameter | Value | Meaning |
|---|---|---|
| Unit | `percentunit` | |
| Min / Max | 0 / 1 | Fixed 0-100%. |
| Fill | 0 | Lines only, so overlapping CPUs stay readable. |
| Legend | Max only | |

**Overall meaning:** one line for each logical CPU. On your machine CPUs 0 and 6 are two hyperthreads of the same physical core, as are 1/7, 2/8, 3/9, 4/10 and 5/11.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| One line high (80-100%), the rest low | A single thread is the limit | Is it a server CPU or a k6 CPU? CPU groups |
| Many lines high | Generator and server are competing, or the machine is oversubscribed | Run queue |
| A CPU and its sibling both busy | They share one physical core; real capacity is less than the sum | |
| CPUs outside your pinning are busy | Something else is running (browser, IDE) | Close other work |

### 9.2 CPU groups: server vs k6 vs spare

**Position:** 8 x 8.

| Series | Query meaning |
|---|---|
| server busiest CPU | Highest utilisation among CPUs matching `$server_cpus` |
| server avg | Average utilisation of the same CPUs |
| k6 busiest CPU | Highest among `$k6_cpus` |
| k6 avg | Average among `$k6_cpus` |
| spare avg | Average among `$spare_cpus` |

Unit `percentunit`, 0-1, fill 0, legend Max.

**Overall meaning:** the proof that the load generator is not your bottleneck. Rule of thumb: if "k6 busiest CPU" stays under about 70%, k6 had headroom, and a plateau or latency rise belongs to the server.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Server busiest 80-100%, k6 busiest below 50% | Server-limited | |
| k6 busiest above 70% | Generator might be limiting; results suspect | Use more k6 CPUs or a second machine |
| Server avg about half of server busiest | One hyperthread busy, its sibling less so | |
| Spare group busy | Monitoring or another process is using the spare CPUs | |
| Group lines all near 0 during a run | The regex variables do not match your CPUs | Edit the text boxes |

### 9.3 CPU by mode (whole machine, stacked)

**Position:** 8 x 8.

**Query:** sum of CPU seconds per mode (all modes except idle), divided by the number of logical CPUs.

| Parameter | Value | Meaning |
|---|---|---|
| Unit | `percentunit` | |
| Min / Max | 0 / 1 | 100% means every CPU fully busy. |
| Stacking | normal | The top edge is total machine utilisation. |
| Legend | Max | |

Modes: `user` (application code), `system` (kernel work for processes), `nice`, `iowait` (waiting for disk), `irq` and `softirq` (interrupt and network-stack processing), `steal` (CPU taken by a hypervisor).

**Overall meaning:** where the machine's CPU time goes. In your tests it is mostly `user` and `system`; the whole machine runs at about a third of its capacity.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Mostly user | Application code | |
| High system and softirq | Network stack is expensive (many small packets) | Loopback traffic, context switches |
| iowait above a few percent | Disk is slow | |
| steal above 0 | Running in a VM with a noisy host | Not applicable to a laptop |
| Total low while one CPU is pinned | Normal for a single-threaded server; the machine total hides it | Per-CPU panel |

### 9.4 Run queue

**Position:** 8 x 8, second line of Row 3.

| Series | Query | Style |
|---|---|---|
| running | `node_procs_running` | Line |
| blocked (I/O) | `node_procs_blocked` | Line |
| logical CPUs | count of CPUs | Grey dashed |

**Overall meaning:** how many tasks want a CPU right now, compared with how many CPUs exist. If running stays below the dashed line there is no systemic CPU contention. With CPU pinning, a high total can still hide contention on one particular CPU.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Running below the CPU line | Not oversubscribed | |
| Running above the line | More runnable tasks than CPUs | Per-CPU panel |
| Blocked above 0 for long | Tasks waiting on disk I/O | iowait |

### 9.5 Context switches

**Position:** 8 x 8.

**Query:** `rate(node_context_switches_total{...}[$__rate_interval])`

Unit `ops` (per second).

**Overall meaning:** how often the kernel switches between tasks, machine-wide. It rises with request rate because each wake-up of Node or k6 is a switch (your data: about 240,000 per second, roughly 10 per request across the whole system).

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Proportional to request rate | Normal | |
| Sharp rise with no rate increase | Thread thrashing or a noisy process | Run queue |
| Falls while rate stays flat | Batching improved (more events per wake-up) | |

### 9.6 TCP retransmits

**Position:** 8 x 8.

| Series | Query |
|---|---|
| retrans segments /s | `rate(node_netstat_Tcp_RetransSegs{...}[...])` |
| SYN retrans /s | `rate(node_netstat_TcpExt_TCPSynRetrans{...}[...])` |

**Overall meaning:** TCP resending data it believes was lost. On loopback these should be near zero. SYN retransmits specifically mean connection attempts were not answered, which is the signature behind `i/o timeout` errors and 1 s, 3 s and 7 s connect stalls.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Zero | Network is clean | |
| Brief blips at the start of a run | Connection ramp | Accept queue drops |
| SYN retransmits with ListenOverflows | Accept queue overflow is the cause | Backlog and `somaxconn` |
| Sustained retransmits | Packet loss (rare on loopback) | |

### 9.7 Accept queue drops

**Position:** 8 x 8, bars.

| Series | Query |
|---|---|
| ListenOverflows /s | `rate(node_netstat_TcpExt_ListenOverflows{...}[...])` |
| ListenDrops /s | `rate(node_netstat_TcpExt_ListenDrops{...}[...])` |

**Overall meaning:** connections the kernel refused because the listening socket's accept queue was full. `ListenDrops` counts every drop at a listener and is always at least `ListenOverflows`. They are counters from boot, and only their rate matters.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| No bars | Queue never overflowed | |
| Bars aligned with latency or error spikes | Accept queue overflow is a cause | Listen queue length, VU count, connection storm |
| Bars only in the first seconds | Many connections opened at once (ramp-up) | Warm-up and VU preallocation |
| Drops equal to overflows | All were queue-full cases | |
| Drops above overflows | Another reason (for example memory) | |

### 9.8 TCP sockets

**Position:** 8 x 8.

| Series | Query | Meaning |
|---|---|---|
| TIME_WAIT | `node_sockstat_TCP_tw` | Closed connections lingering for 60 s. |
| in use | `node_sockstat_TCP_inuse` | TCP sockets in use. |
| established | `node_netstat_Tcp_CurrEstab` | Established connections. On loopback both ends are counted, so it is about twice your connection count. |
| ephemeral port range | `vector(28232)` | Red dashed constant: the default number of client ports. |

**Overall meaning:** socket pressure. TIME_WAIT growing toward the red line means the client side (k6) is close to running out of source ports.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| TIME_WAIT in the hundreds | Healthy keep-alive | |
| TIME_WAIT climbing toward 28k | Connection churn: new connections per second times 60 s approaches the port range | VUs, keep-alive, k6 errors |
| Established tracks VUs x 2 | Keep-alive reuse | |
| Established much larger than VUs | Connection leak | File descriptors |

### 9.9 Listen queue length (textfile)

**Position:** 8 x 8.

| Series | Query | Style |
|---|---|---|
| accept queue (Recv-Q) | `max(ep_listen_recvq)` | Line |
| max queue (Send-Q) | `max(ep_listen_sendq)` | Grey dashed |

**Overall meaning:** a live view of the listening socket from `ss`. For a **listening** socket, Recv-Q is the number of connections waiting for `accept()` and Send-Q is the maximum queue size (your backlog, 511). Data exists only while `listen-queue-exporter.sh` runs (the runner starts it). The script deletes its file on exit, so a stopped exporter shows a gap, not a frozen value.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Recv-Q 0 | Server accepts connections as fast as they arrive | |
| Recv-Q approaching Send-Q | Queue about to overflow | Accept queue drops |
| Send-Q 511 | Backlog is the Node default | |
| "No data" | Exporter not running or textfile directory not mounted | Runner start-up message |
| A flat line that never changes | Exporter died but its file stayed (older script versions) | Restart the exporter |

### 9.10 Loopback traffic

**Position:** full width, 24 x 6.

| Series | Query |
|---|---|
| lo rx | `rate(node_network_receive_bytes_total{device="lo",...}[...])` |
| lo tx | `rate(node_network_transmit_bytes_total{device="lo",...}[...])` |

Unit `Bps` (bytes per second).

**Overall meaning:** bytes crossing the loopback interface. Because every packet on loopback is both sent and received, the two lines are identical and overlap. Bytes per request is about throughput divided by requests per second (about 390 bytes in your tests).

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Proportional to request rate | Normal | |
| Much higher per request than expected | Large headers or bodies | |
| Zero while k6 runs | k6 is not hitting `localhost` (different address) | |

---

## 10. Row 4: Live proof overlays

### 10.1 Saturation overlay

**Position:** left half, 12 x 10.

| Series | Query | Axis | Style |
|---|---|---|---|
| Achieved rps | `sum(rate(k6_http_reqs_total{...}[...]))` | Left, linear, `reqps` | Green line |
| Event loop lag p99 | `max(nodejs_eventloop_lag_p99_seconds{...})` | **Right**, log, `s` | Orange line |
| ListenOverflows /s | `sum(rate(node_netstat_TcpExt_ListenOverflows{...}[...]))` | **Hidden** axis | Red bars |
| Busiest CPU | `max((1 - avg by (cpu)(rate(...idle...))))` | **Hidden** axis, `percentunit` | Blue dashed line |

**Overall meaning:** four signals on one graph, so you can see which one moves at the moment throughput stops growing. The first three tell you cause: loop lag (the Node thread), overflows (the kernel queue), busiest CPU (the machine).

Because two series use hidden axes, their vertical positions are **not comparable** with the left axis. Read their values from the legend or the tooltip, not from the grid.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Achieved climbs, lag flat, CPU flat | Healthy region | |
| Lag rises, achieved still climbing | Loop starts queueing before the ceiling (the knee) | |
| Achieved flat while lag keeps rising | Throughput ceiling: saturation | CPU groups |
| Overflow bars appear first | Kernel accept queue is the first limit | Listen queue, VUs |
| CPU line at 100% when achieved flattens | CPU-bound | |
| CPU stays at 80-85% and latency still climbs | The thread is near its limit; queueing grows sharply even before 100% | Event loop utilisation |
| Everything looks like a triangle | Rate window equals run length (see data source setting) | |

### 10.2 Run comparison (rps per testid)

**Position:** right half, 12 x 10.

**Query:** `sum by (testid) (rate(k6_http_reqs_total{testid=~"$testid"}[$__rate_interval]))`, legend `rps {{testid}}`.

| Parameter | Value |
|---|---|
| Unit | `reqps` |
| Min | 0 |
| Legend | Max only |

**Overall meaning:** one line per test run showing its throughput. Each run only has data while it ran, so runs appear one after another in **time**, not on top of each other. Use it to confirm which runs happened and that each reached its intended rate. For an actual before/after comparison, use the table and knee curve in Row 5.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Lines in sequence, each flat at its rate | Normal series | |
| A run lower than its offered rate | That run was saturated | Dropped iterations for that run |
| Runs touching each other | No cool-down between runs | Manifest start and end times |
| Too many lines | `testid` is on All | Select runs |

---

## 11. Row 5: Post-run analysis (collapsed)

The row is collapsed on purpose, because its queries use the whole time range and are heavy. Expand it after the series finishes, and set the time range to cover every run you want to include.

### 11.1 Per-run summary (offered, achieved, dropped %, latency)

**Position:** full width, 24 x 10. Table.

**Queries** (all instant, table format):

| Ref | Meaning |
|---|---|
| A | For each `testid` (and its `rate` label), the highest per-second throughput inside the range. It uses a 15 s `rate()` and a subquery with a 10 s step. |
| B | Dropped iterations divided by (dropped plus completed), over the range. Runs with no drops show 0. |
| C | p50 latency over the whole run, from `increase()` of the native histogram. |
| D | p99 over the whole run. |
| E | p99.9 over the whole run. |

**Transformations:** join A to E on `testid`; keep only `testid`, `rate` and the value columns; convert `rate` to a number; rename columns; sort by `testid`.

| Column | Meaning |
|---|---|
| testid | The run. It contains the date and time. |
| Offered rps | From the `rate` tag you gave k6: the intended rate. |
| Achieved peak rps | Highest throughput k6 achieved. |
| Dropped % | Share of iterations dropped. The cell turns red from 0.1%. |
| p50, p99, p99.9 | Percentiles for the whole run, in seconds with four decimals. |

**Overall meaning:** the clean summary of your experiment. Each row is one run. It uses fixed windows and whole-run histograms, so it is not affected by the data source smoothing.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Achieved peak equals Offered | Server kept up | |
| Achieved well below Offered | Saturation | |
| Dropped % red | Run not valid for latency conclusions | |
| Three repeats at the same rate with wide p99 spread | Run-to-run noise: use medians; p50 is steadier than p99 | Add more repeats |
| p50 doubling between steps | You are at the knee | |
| Empty table | Time range does not cover the runs, or the `testid` selection is empty | |
| Rows missing | The run started before the range began | Widen the time range |

Notes: values are approximate (PromQL `increase()` extrapolates slightly). A run cut off by the edge of the time range will show a partial result.

### 11.2 Knee curve: peak achieved rps (X) vs p99 (Y), one dot per run

**Position:** full width, 24 x 10. XY chart.

**Queries:** A (peak throughput per run) and D (whole-run p99 per run), joined on `testid`; only the two value columns are kept and renamed to "Achieved peak rps" and "p99".

| Parameter | Value | Meaning |
|---|---|---|
| Series mapping | auto | First numeric column is X, the next is Y. |
| Show | points, size 9 | One dot per run. |
| Colour | orange | |
| Y unit | seconds | A value of 0.01 means 10 ms. |

**Overall meaning:** latency against actual throughput, with time removed. Dots stay low and flat while the server is comfortable and shoot upward at the knee. Repeats at the same rate stack vertically, so the vertical spread at one X shows run-to-run noise.

**Cases:**

| What you see | Meaning | Check next |
|---|---|---|
| Dots in a low band | Below the knee | |
| Dots rising with X, with an acceleration | The knee is where the slope increases | |
| Large vertical spread at one X | Noisy p99; compare with p50 | |
| Dots stop moving right but keep rising | Throughput plateau: saturation | |
| All dots at one X | Only one rate was run | Run a ladder |
| Blank or wrong axes | Series mapping failed | Edit panel: X = Achieved peak rps, Y = p99 |

---

## 12. Cross-panel diagnosis playbook

Use these combinations to name the cause of a problem. Look at the panels together, using the shared crosshair.

| Situation | Panels that show it | What they say |
|---|---|---|
| **Single-thread CPU saturation** | Server busiest CPU near 100% (or 80-90% at the knee), Process CPU about 0.9-1.0, event loop lag excess rising, client p99 rising while server p99 stays flat, k6 busiest CPU low, accept queue empty | Node's main thread is the bottleneck |
| **Accept queue overflow** | Accept queue drops above 0, SYN retransmits, Listen queue length near its maximum, errors with `i/o timeout` | The kernel connection queue overflowed; usually a burst of new connections |
| **Port exhaustion (client side)** | TIME_WAIT approaching 28k, k6 errors `cannot assign requested address` | Too many new connections per second |
| **File descriptor exhaustion** | FD usage tile red, open fds near the limit, errors `EMFILE` | Raise `ulimit -n` or fix a leak |
| **Load generator is the limit** | k6 busiest CPU above 70%, dropped iterations with low server CPU | Add CPUs or machines for k6 |
| **GC problem** | GC time rising, p99.9 spikes that line up with GC spikes, heap sawtooth growing | Reduce allocations |
| **Memory leak** | Heap and RSS rising across runs and not returning | Heap snapshot |
| **Slow dependency (later stages)** | Server p99 rising along with client p99, CPU low, active requests or handles rising | The server is waiting on something else |
| **Invalid run** | Dropped % above 0.1%, VUs active at the maximum | Use throughput only; ignore latency |
| **Monitoring artefact** | Rate graphs are triangles, tiles understate rates | Data source scrape interval; use 1s |
| **No cool-down between runs** | Runs touch each other on Run comparison, CPU stays high after a run, manifest start equals previous end | Use the 120 s gap for final results |
| **Hyperthread sharing** | Two sibling CPUs both busy, lower ceiling than expected | Test the server on one CPU, or on CPUs of different physical cores |

---

## 13. Known limitations and gotchas

1. **Data source scrape interval.** `$__rate_interval` is derived from it. Left at the 15 s default, all `rate()` panels use windows of at least 60 s. Set `timeInterval: 1s` in the provisioned data source file.
2. **Event loop lag floor.** prom-client's 10 ms resolution means an idle server shows about 10 ms. Use the "excess" panel or subtract 10 ms mentally.
3. **Histogram resolution on the server side.** The custom `http_request_duration_seconds` histogram needs buckets fine enough for sub-millisecond work. If many values fall in one bucket, the percentile equals a bucket boundary.
4. **Native histograms only.** v3 needs `K6_PROMETHEUS_RW_TREND_AS_NATIVE_HISTOGRAM=true` and Prometheus `--enable-feature=native-histograms`. The `max` latency is not available in this mode.
5. **Peak tiles use the maximum.** They mix runs when `testid` is on All.
6. **Hidden axes in the Saturation overlay.** Read CPU and overflow values from the legend.
7. **SMT.** Logical CPUs that share a physical core do not add capacity linearly. A server on CPUs 0 and 6 has one core of real throughput.
8. **Everything is on one machine.** Loopback hides real network costs, and the monitoring stack shares the host. Record results as "localhost, monitoring on".
9. **Row 5 values are approximate.** `increase()` extrapolates at range edges, and subqueries use a 10 s step.
10. **Do not trust a single run.** p99 varied by more than 2x between repeats in your series. Use medians of at least three repeats, and use p50 to locate the knee.
