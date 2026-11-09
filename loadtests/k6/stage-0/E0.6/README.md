# E0.6 Keep-alive vs new connection per request

## 1. Run the server using `server-runner.sh`

```bash
./labs/stage-0-foundations/script/server-runner.sh
```

### To verify

```bash
# In a different terminal tab
grep 'open files' /proc/<$PID>/limits

# Record these in the notebook. The port range decides when port exhaustion happens.
sysctl net.ipv4.ip_local_port_range net.ipv4.tcp_tw_reuse net.ipv4.tcp_max_tw_buckets net.core.somaxconn

# Baseline socket counts before any run (TIME_WAIT should be near 0)
ss -s
```

## 2. Instrumentations needed

### Default metrics given by prometheus client

1. process_cpu_user_seconds_total
2. process_cpu_system_seconds_total
3. process_open_fds
4. process_max_fds
5. nodejs_eventloop_lag_p99_seconds

### Custom metrics made in server

1. http_requests_total
2. http_request_duration_seconds
3. http_requests_in_flight
4. http_connections_open
5. http_connections_total
6. http_server_errors_total
7. eventpulse_event_loop_delay_seconds

### k6 built in metrics

1. k6_http_reqs_total
2. k6_http_req_connecting_seconds
3. k6_http_req_duration_seconds
4. k6_dropped_iterations_total
5. k6_http_req_failed_rate

The k6 script also adds a `keepalive` tag (`on` or `off`) to every metric, so Grafana can split the two modes.

### Node exporter metrics

1. node_sockstat_TCP_tw
2. node_sockstat_TCP_inuse
3. node_sockstat_TCP_alloc
4. node_netstat_Tcp_CurrEstab
5. node_netstat_Tcp_PassiveOpens
6. node_netstat_Tcp_ActiveOpens
7. node_netstat_TcpExt_ListenOverflows
8. node_netstat_TcpExt_ListenDrops
9. node_netstat_Tcp_RetransSegs
10. node_cpu_seconds_total
11. node_context_switches_total
12. node_nf_conntrack_entries (only if Docker NAT is in the path)

### To verify

```promql
# In Grafana Explore, after any k6 run. Should return one series per keepalive value.
count by (keepalive)(k6_http_reqs_total)

# Native histogram check. Should return data right after a k6 run.
histogram_quantile(0.99, rate(k6_http_req_connecting_seconds[30s]))
```

## 3. Grafana pannels

Job filters (`job="eventpulse-nodejs"` for server metrics, `job="node"` for node exporter) and `route!~"/metrics"` are left out of the queries below to keep them short. `[w]` is `$__rate_interval`.

| #   | Panel                                  | Query                                                                                                                     | What it shows                                                                                      |
| --- | -------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| 1   | Offered (k6) vs handled (server) rps   | `sum by (keepalive)(rate(k6_http_reqs_total[w]))`, `sum(rate(http_requests_total[w]))`                                    | Both lines should match. k6 below the target RATE means the generator is the bottleneck.           |
| 2   | Server latency p50 / p99               | `histogram_quantile(0.99, sum by (le)(rate(http_request_duration_seconds_bucket[w])))`                                    | Server-side handler time only. It does not include TCP connect time.                               |
| 3   | Server errors                          | `rate(http_requests_total{status_code=~"5.."}[w])`, `sum by (code)(rate(http_server_errors_total[w]))`                    | Errors raised by the server when connections churn                                                 |
| 4   | New connections per second             | `rate(http_connections_total[w])`, `rate(node_netstat_Tcp_PassiveOpens[w])`, `rate(node_netstat_Tcp_ActiveOpens[w])`      | Connection rate. Server counter is exact, host counters are a cross-check.                         |
| 5   | Connections per request                | `rate(http_connections_total[w]) / rate(http_requests_total[w])`                                                          | Near 0 with keep-alive on, near 1 with keep-alive off                                              |
| 6   | Socket states                          | `node_sockstat_TCP_tw`, `node_netstat_Tcp_CurrEstab`, `node_sockstat_TCP_inuse`, `node_sockstat_TCP_alloc`                | TIME_WAIT at steady state should be about new connections/s x 60                                   |
| 7   | TIME_WAIT % of ephemeral port range    | `100 * node_sockstat_TCP_tw / 28232`                                                                                      | How close TIME_WAIT is to the port range (32768 to 60999). Edit if the range is changed.           |
| 8   | Open connections and in-flight         | `http_connections_open`, `http_requests_in_flight`                                                                        | Little's Law check. Open connections should differ a lot between the two modes.                    |
| 9   | k6 connect time and latency            | `histogram_quantile(0.99, sum by (keepalive)(rate(k6_http_req_connecting_seconds[w])))`, same for `k6_http_req_duration_seconds` | Client-side cost of the TCP handshake, split by mode                                     |
| 10  | Node process CPU: user vs system       | `rate(process_cpu_user_seconds_total[w])`, `rate(process_cpu_system_seconds_total[w])`                                    | Where the extra work lands: JS (user) or kernel (system)                                           |
| 11  | Host CPU by mode                       | `sum by (mode)(rate(node_cpu_seconds_total{mode=~"user\|system\|softirq\|irq\|iowait\|steal"}[w]))` divided by core count | Whole-machine view, includes k6. softirq is network packet processing.                             |
| 12  | Context switches                       | `rate(node_context_switches_total[w])`                                                                                    | Scheduler activity from connection churn                                                           |
| 13  | Kernel share of Node CPU               | `system / (user + system)` of the process CPU rates                                                                       | Should be higher with keep-alive off (accept, close, epoll_ctl per request)                        |
| 14  | k6 dropped iterations                  | `rate(k6_dropped_iterations_total[w])`                                                                                    | Must be 0. Otherwise k6 is the bottleneck, discard the run.                                        |
| 15  | k6 failed requests                     | `avg by (keepalive)(k6_http_req_failed_rate)`                                                                             | Failures appear if the client runs out of ports                                                    |
| 16  | Host CPU busy                          | `1 - avg(rate(node_cpu_seconds_total{mode="idle"}[w]))`                                                                   | Generator headroom proxy. Keep it well under 70%.                                                  |
| 17  | Event loop delay                       | `eventpulse_event_loop_delay_seconds{quantile="0.99"}`, `{quantile="max"}`, `nodejs_eventloop_lag_p99_seconds`            | Shows whether the event loop is saturated                                                          |
| 18  | Open file descriptors                  | `process_open_fds`, `process_max_fds`                                                                                     | Should stay far from the limit, otherwise E0.5 effects get mixed in                                |
| 19  | Listen overflows / drops               | `rate(node_netstat_TcpExt_ListenOverflows[w])`, `rate(node_netstat_TcpExt_ListenDrops[w])`                                | Accept queue overflow when connections arrive faster than Node accepts them                        |
| 20  | TCP retransmits                        | `rate(node_netstat_Tcp_RetransSegs[w])`                                                                                   | Packet loss or overload signal                                                                     |
| 21  | Conntrack usage                        | `node_nf_conntrack_entries / node_nf_conntrack_entries_limit`                                                             | No data is normal for loopback or host networking                                                  |

## 4. Run k6 script

`runner.sh` must not ask for the result folder name on every call. Apply this change once in section 6 of `runner.sh`:

```bash
if [[ -z "${RESULTS_NAME:-}" ]]; then
    read -r -p "Result folder name [default]: " RESULTS_NAME
fi
RESULTS_NAME="${RESULTS_NAME:-default}"
```

Only one k6 script is needed (`script.js`). `KEEPALIVE=1` reuses connections, `KEEPALIVE=0` opens a new connection per request. `step.sh` runs it for every rate and mode, alternating on/off, and waits for TIME_WAIT to drain before each run.

Commands to watch during a run (in a different terminal tab)

```bash
# Socket summary, watch TIME_WAIT grow
watch -n1 'ss -s'

# Which side holds TIME_WAIT? The side that closes first.
ss -tan state time-wait '( sport = :8000 )' | wc -l   # server side
ss -tan state time-wait '( dport = :8000 )' | wc -l   # client side

# Connection counters and listen queue problems
nstat -az TcpActiveOpens TcpPassiveOpens TcpRetransSegs TcpExtListenOverflows TcpExtListenDrops

# Syscall counts for 10 s. Compare accept4, close and epoll_ctl between modes.
sudo timeout 10 strace -c -f -p $PID
```

1. Trial run. One rate, one on-run and one off-run. Check that both show up in Grafana split by `keepalive`.

   ```bash
   cd loadtests/k6/stage-0
   chmod +x E0.6/step.sh
   DRY_RUN=1 ./E0.6/step.sh
   RATES="500" REPEATS=1 ./E0.6/step.sh
   ```

2. Full matrix. Rates 500, 1000, 2000, 4000, 8000, 3 repeats each, 60 s per run. Press Ctrl+C once keep-alive off starts dropping iterations.

   ```bash
   ./E0.6/step.sh
   ```

   Results go to `results/e0.6/<timestamp>/`, one folder per run.

3. Run a single case by hand

   ```bash
   LABEL=E0.6_manual SCRIPT=E0.6/script.js RATE=1000 KEEPALIVE=0 ./runner.sh
   ```

4. Force port exhaustion. Shrink the port range, run with keep-alive off, then restore the range.

   ```bash
   sudo sysctl -w net.ipv4.ip_local_port_range="32768 33767"
   LABEL=E0.6_port_exhaustion SCRIPT=E0.6/script.js RATE=100 DURATION=30s KEEPALIVE=0 ./runner.sh
   sudo sysctl -w net.ipv4.ip_local_port_range="32768 60999"
   ```

## Analyze results in grafana dashboard

Dashboard - [E0.6 - Keep-alive vs new connection per request.json](../../../../infra/observability/dashboards/stage-0/E0.6%20-%20Keep-alive%20vs%20new%20connection%20per%20request.json)