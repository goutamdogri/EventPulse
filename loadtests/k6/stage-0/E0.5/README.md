# E0.5 File descriptor exhaustion

## 1. Run the server in a subshell with a low file descriptor limit

Executing a script like ./server-runner.sh, spawns a new child process (a bash subshell) to run the script.

```bash
taskset -c 0,6 ./labs/stage-0-foundations/script/server-runner.sh
```

### To verify

```bash
# In a different terminal tab
ulimit -Sn # soft limit
ulinit -Hn # hard limit

# Subshell process's fd limits
grep 'open files' /proc/<PID>/limits
```

## 2. Instrumentations needed

### Default metrics given by prometheus client

1. process_open_fds
2. process_max_fds
3. nodejs_active_handles_total
4. nodejs_eventloop_lag_seconds
5. process_cpu_seconds_total

### Custom metrics made in server

1. http_connections_open
2. http_connections_total
3. http_server_errors_total

### k6 built in metrics

1. k6_http_req_failed_rate
2. k6_vus
3. k6_http_reqs_total

### Node exporter metrics

1. node_filefd_allocated
2. node_filefd_maximum
3. node_sockstat_TCP_inuse
4. node_sockstat_TCP_tw

## 3. Grafana pannels

| #   | Panel                               | Query                                                                                            | What it shows                                                                                                                    |
| --- | ----------------------------------- | ------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Open FDs vs limit (time series)     | `process_open_fds`, `process_max_fds`                                                            | The line flattening at the ceiling. This is the main panel.                                                                      |
| 2   | FD utilization (gauge)              | `process_open_fds / process_max_fds`                                                             | Quick read on how close you are to 100%                                                                                          |
| 3   | Open connections                    | `http_connections_open`, plus `nodejs_active_handles_total`                                      | Connections track fds closely, with a constant offset from stdio, epoll and the listener                                         |
| 4   | Accepted connections rate           | `rate(http_connections_total[10s])`                                                              | New connections stop being accepted at the limit                                                                                 |
| 5   | Server errors                       | `rate(http_server_errors_total[10s])` by `code`                                                  | Shows the `EMFILE` signal if Node surfaces it                                                                                    |
| 6   | Client-side failures (k6)           | `k6_http_req_failed_rate`, `k6_vus`, `k6_http_reqs_total`                                        | Failures begin once VUs pass about 220. Check exact names in Explore, because they depend on your k6 version and trend settings. |
| 7   | Event loop lag                      | `nodejs_eventloop_lag_seconds`                                                                   | Should stay flat, which shows the server is not CPU-bound                                                                        |
| 8   | Process CPU                         | `rate(process_cpu_seconds_total[10s])`                                                           | Should stay low, which rules out CPU saturation                                                                                  |
| 9   | Scrape health                       | `up{job="your-node-job"}`                                                                        | May drop to 0 near saturation, because the scrape also needs an fd. That would be a finding in itself.                           |
| 10  | System-wide context (node_exporter) | `node_filefd_allocated / node_filefd_maximum`, `node_sockstat_TCP_inuse`, `node_sockstat_TCP_tw` | The system-wide fd table stays nowhere near full, which shows the cap is per-process                                             |

## 4. Run k6 script

Change the fd limit of server before each run.

1. FD limit of the server = 256. Run the server.
   Run ramping-vu-test.js

   ```bash
    FD_LIMIT=256 taskset -c 0,6 ./labs/stage-0-foundations/script/server-runner.sh

    LABEL=E0.5_File_descriptor_exhaution SCRIPT=E0.5/ramping-vu-test.js ./runner.sh
   ```

2. FD limit of the server = 65535. Run the server.
   Run ramping-vu-test.js

   ```bash
    FD_LIMIT=65535 taskset -c 0,6 ./labs/stage-0-foundations/script/server-runner.sh

    LABEL=E0.5_File_descriptor_exhaution SCRIPT=E0.5/ramping-vu-test.js ./runner.sh
   ```

3. FD limit of the server = 256. Run the server.
   Run ramping arrival rate

   ```bash
    FD_LIMIT=256 taskset -c 0,6 ./labs/stage-0-foundations/script/server-runner.sh
    LABEL=E0.5_File_descriptor_exhaution SCRIPT=E0.5/ramping-arrival-test.js ./runner.sh
   ```

## Analyze results in grafana dashboard

Dashboard - [E0.5 - File Descriptor Exhaustion.json](../../../../infra/observability/dashboards/stage-0/E0.5%20-%20File%20Descriptor%20Exhaustion.json)
