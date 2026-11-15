# B1 Ramp to collapse: learnings

This experiment is about one question: when I keep raising the request rate on a service that writes to Postgres
inside the request, where does it stop coping, and which resource is responsible? I do not know the answer yet.
This file only holds the concepts and the commands. No results.

The cycle I expect, step by step:

1. k6 sends a request on schedule (open model, it does not wait for the previous answer).
2. Node accepts it, runs Express and Nest (guard, validation pipe), and asks the pool for a connection.
3. If all 10 pool connections are busy, the request waits inside Node (`pool.waitingCount` > 0).
4. The INSERT runs in Postgres. The commit waits for the WAL to be flushed to disk (fsync).
5. Node sends the 202. If anything in 2-4 is saturated, requests queue up, latency rises, and at some point the
   pool timeout (2 s) or k6's own timeout fires and I see 503 or no answer.

Our hypothesis: latency stays flat and then bends at a knee set by the pool, the database commit capacity, or the Node CPU.

See also: `notebook/stage-0/learning/load-testing-science/5. The knee.md`, `4. Little's law.md`, `6. USE method.md`,
`notebook/stage-1/learning/nodeAndNest/1. node-pg-pool.md`, `Postgres/1. postgres-process-per-connection.md`, `2. postgres-write-path.md`.

## Concepts

### The knee

Latency is almost flat while the system has spare capacity, then rises sharply when one resource is nearly fully busy.
The knee is the real capacity, not the highest rps I ever saw. Why it bends: queueing. When utilization rho is near 100%,
waiting time grows roughly like 1 / (1 - rho). Example: at 50% busy a request waits about 1x its service time; at 90% busy about 9x.

### Open vs closed load (from Stage 0)

Open model = k6 `constant-arrival-rate`: requests start on schedule even if earlier ones are still running. A closed model would
send fewer requests when the server slows down and hide the collapse. If k6 cannot start a request because it has no free VU, it
counts `dropped_iterations`.

### Little's Law

In-flight requests = arrival rate x time in the system. Example: 2000 req/s and 5 ms gives 10 in flight. When latency rises,
in-flight rises with it, and that growth is the queue. The dashboard compares measured in-flight with rate x mean latency.

### Utilization of a pool (how I get the prediction)

A pool of c connections, each INSERT holding one for s seconds, arrival rate lambda: utilization rho = lambda x s / c.
Maximum rate before the pool is saturated = c / s. Example: c = 10, s = 2 ms gives 10 / 0.002 = 5,000 req/s.
The same logic for the Node thread: c = 1, s = CPU time per request. Example: 0.25 ms gives 4,000 req/s.
Whichever of these ceilings is lowest should bend first (assuming my guessed service times are right; they are guesses).

### Pool queueing is not database slowness

`pg.Pool` has `max` connections (default 10). Request 11 does not fail; it waits in a client-side queue inside Node
(`waitingCount`) until a connection is free, or until `connectionTimeoutMillis` (2 s here) fires. So request latency =
pool wait + query time + Node work. If wait grows and query time stays flat, Postgres is fine and my pool is the limit.
That is why the server records both `ep_pg_pool_wait_seconds` and `ep_pg_query_duration_seconds`.

### Where the commit cost comes from

With `synchronous_commit = on`, COMMIT returns only after the WAL record is flushed to disk (fsync). Postgres can group several
backends' commits into one fsync (group commit), so more concurrent writers do not always mean proportionally more fsyncs.
A slow disk therefore shows up as higher query time without high CPU.

### Postgres container limits

The container has `cpus: 2` and `mem_limit: 2g`. `cpus: 2` is enforced by the kernel CFS quota: when the container uses its 2 CPUs worth
of time in a period, its processes are paused until the next period ("throttled"). That adds latency while the host still has idle cores.
`cpuset` (what I use for pinning) is different: it says WHICH CPUs the processes may run on.

### cgroups

A cgroup is a kernel group of processes with accounting and limits. Docker creates one per container. The file `cpu.stat` has
`user_usec`, `system_usec` and `throttled_usec`, `memory.current` has the memory, `io.stat` has bytes read/written per device.
`pg-container-exporter.sh` reads these. (cgroup v2 only; I should check `stat -fc %T /sys/fs/cgroup` prints `cgroup2fs`.)

### CPU pinning and SMT

My CPU has 6 cores and 12 logical CPUs (SMT / hyperthreads). CPUs 0 and 6 are the same physical core; they share execution units, so two busy
threads on 0 and 6 each run slower than one alone. I pin each component to whole cores (pairs), so the server, Postgres, k6 and monitoring do not
steal cycles from each other. One Node JS thread on CPUs 0,6 shows up as about 50% in the "host CPU busy" panel.

### Generator headroom

If k6 itself runs out of CPU or VUs, the result is about k6, not about the server. So every step is checked: `dropped_iterations` = 0 and the k6
CPUs below about 70%.

### Test hygiene I apply

- Fresh server per run (empty pool, clean histogram), warm-up before the measured window.
- `TRUNCATE` + `CHECKPOINT` between runs, so table/index growth and a previous run's checkpoint do not contaminate the next run.
- Heavy tools only in separate probe runs, never in the measured ones.
- At least 3 repeats for the headline pass; median and range.
- Everything on one machine, so the numbers are relative (what limits what), not absolute capacity. Loopback / Docker bridge hides real network cost.

### The row-count audit

Every event has a unique id. So number of 202 answers must equal rows in the table. More rows than 202s: committed but the client did not hear
(for example a timeout after the commit). Fewer: an acknowledged event is gone. This catches correctness bugs that latency charts cannot.

## Explanation of each command

### `docker exec -i <pg> psql -U postgres -d eventpulse -X -At -c "..."`

1. `docker exec -i <pg>` runs a command inside the running Postgres container (`-i` keeps stdin open; not strictly needed with `-c`).
2. `psql` is the Postgres command line client.
3. `-U postgres -d eventpulse` user and database.
4. `-X` ignores my `~/.psqlrc`, so output is predictable.
5. `-A` unaligned output, `-t` tuples only (no header/footer): one clean value per line for the script. `pgqt` drops these two flags to get a readable table.
6. `-c "..."` run this one command and exit.

### `TRUNCATE events`

Removes all rows at once, much faster than `DELETE` and without dead tuples. It takes an ACCESS EXCLUSIVE lock, so it must run while the API is idle
(the script runs it after warm-up, before k6 starts). Destructive: do not run it on data I want to keep.

### `SELECT pg_stat_statements_reset()`

Clears the statistics of the `pg_stat_statements` extension (calls, total time per query), so the top-queries view in a probe belongs to this run only.

### `CHECKPOINT`

Forces Postgres to write all dirty pages to disk now. A checkpoint is a periodic flush that causes I/O spikes. Doing one before each run
avoids a leftover flush from the previous run landing in my measured window.

### `pg_stat_activity` wait-event query (probe only)

```sql
select state, wait_event_type, wait_event, count(*) from pg_stat_activity where datname='eventpulse' group by 1,2,3 order by 4 desc
```

Each row is one backend process. `state` = active / idle / idle in transaction. `wait_event_type` and `wait_event` say what an active backend is
waiting for: `IO / WALSync` (fsync of the commit), `LWLock / WALWrite`, `Lock / transactionid` (row locks), `Client / ClientRead` (waiting for Node).
Many `WALSync` = disk-commit limited. Many `ClientRead` with idle = Postgres is waiting for Node, so Postgres is not the limit.
It costs a connection, so I take it only in probe runs.

### Blocked and blocking query (probe only)

`pg_blocking_pids(pid)` returns the pids that block this one. Rows here mean lock contention (should be none in B1; B4 is the lock experiment).

### `pg_stat_statements` top 5 (probe only)

`calls`, `mean_exec_time` (ms per call) and `total_exec_time` per normalized query. For B1 the INSERT should be the top row; its mean time is my "s".

### Before/after `pg_stat_database` and `pg_stat_bgwriter`

These are cumulative counters: `xact_commit` (commits), `tup_inserted`, `blks_read` / `blks_hit` (cache misses/hits), `temp_files`,
`checkpoints_timed` / `checkpoints_req` (checkpoints by timer or by WAL volume). I read them before and after a run and subtract.
(`pg_stat_bgwriter` has the checkpoint columns in Postgres 16; newer versions moved them to `pg_stat_checkpointer`, so this line may print an error there.)

### `curl localhost:8000/debug/stats`

Returns the pool counts (`total`, `idle`, `waiting`), in-flight, event loop delay and memory from the server's own counters.

### `docker update --cpuset-cpus "1,7,2,8" <container>`

1. `docker update` changes limits of a running container without recreating it.
2. `--cpuset-cpus` list of logical CPUs the container may use.
   Restored at the end. Docker's own processes (the daemon, containerd shims) are not pinned, which is a small unpinned cost.

### `taskset -c 0,6 node ...`

`taskset` sets the CPU affinity of a process: `-c 0,6` = only logical CPUs 0 and 6. `exec` in the script keeps the pid so I can `kill` the right process.

### `node --env-file=.env dist/main.js`

Node 20+ flag: reads `KEY=value` lines from `.env` into the environment of the process. Variables that already exist in the environment are not overwritten,
which is how `POOL_MAX` can override `PG_POOL_MAX`.

### Warm-up: `seq 1 100 | xargs -P 10 -I{} sh -c 'curl ...'`

1. `seq 1 100` prints 1..100, one per line.
2. `xargs -P 10` runs up to 10 commands in parallel (10 = the pool size, so all pool connections get opened).
3. `-I{}` runs the command once per input line.
4. `sh -c '...'` is needed so `$(cat /proc/sys/kernel/random/uuid)` creates a new uuid for every call.
5. `curl -s -o /dev/null -X POST ...` quiet, discard the body, POST with the API key.

### `k6 run` through `runner.sh`

`runner.sh` pins k6 with `taskset -c $K6_CPUS`, pushes metrics to Prometheus by remote write, tags the run (`testid`) and writes `--summary-export` json.
Every exported env var that is not a harness variable becomes a k6 `-e NAME=value`, so `RATE`, `DURATION`, `API_KEY`, `PHASE` reach `script.js`.
I name the key `API_KEY` because runner.sh hides values of names containing `API_KEY` in its manifest.

### `iostat -x 1 5` (probe only)

Prints extended disk stats every 1 s, 5 times (the first report is the average since boot, ignore it).

| Column         | Meaning                                                                                                                                                        |
| -------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `w/s`, `wkB/s` | write operations and kilobytes per second                                                                                                                      |
| `w_await`      | average time per write request in ms (queue + service)                                                                                                         |
| `aqu-sz`       | average queue length                                                                                                                                           |
| `%util`        | share of time the device had I/O in flight (near 100% on a spinning disk = saturated; NVMe can serve many in parallel, so 100% does not always mean saturated) |

### `vmstat 1 5` (probe only)

`r` = runnable processes (waiting for CPU; larger than the CPUs available = CPU queue), `b` = blocked on I/O, `cs` = context switches per second,
`us/sy/id/wa/st` = CPU share in user / kernel / idle / waiting for I/O / stolen.

### `pidstat -u -p <pid> 1 5` (probe only)

CPU of one process every second: `%usr`, `%system`, `%CPU` (per process, 100% = one CPU), and `CPU` = which CPU it ran on (should be 0 or 6).

### PromQL used in the panels

- `rate(counter[$__rate_interval])` per-second rate of a counter; `$__rate_interval` lets Grafana pick a window that is at least 4 scrape intervals.
- `histogram_quantile(0.99, sum by (le)(rate(x_bucket[w])))` p99 from a classic histogram: sum the bucket rates, keep `le`, estimate the quantile.
  For k6 native histograms there is no `_bucket`; the same call works on `sum(rate(k6_http_req_duration_seconds[w]))`.
- `1 - avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"0|6"}[w]))` busy share of CPUs 0 and 6: idle seconds per second, averaged, inverted.
- `rate(node_disk_write_time_seconds_total[w]) / rate(node_disk_writes_completed_total[w])` seconds per write = write latency.
- `X or vector(0)` use 0 when a series does not exist (dropped iterations has no series when nothing was dropped).

## How to read the captures

| File block                      | Columns / values                       | What to compare                                                    |
| ------------------------------- | -------------------------------------- | ------------------------------------------------------------------ |
| `pg_stat_database` pre/post     | `xact_commit`, `tup_inserted`          | post - pre should equal the number of committed inserts of the run |
| `pg_stat_bgwriter` pre/post     | `checkpoints_timed`, `checkpoints_req` | did a checkpoint happen inside the run                             |
| `/debug/stats`                  | `pool.total/idle/waiting`, `in_flight` | pool state at the end of the run                                   |
| probe wait events               | `wait_event` counts                    | what active backends wait for at the middle of the run             |
| probe iostat / vmstat / pidstat | see tables above                       | disk, CPU queue, which CPU the Node thread used                    |

## Pitfalls to watch

- k6 or Postgres running out of CPU and me blaming the wrong component (pinning and panel 23 are for this).
- Forgetting the audit: a system that returns 202 but lost the row looks perfect in latency charts.
- Not resetting the table between runs, so index/table growth changes later results.
- Raising the pool size to fix latency without looking at Postgres CPU (that is B2).
- Calling client-side pool queueing "database slowness": compare pool wait with query time.
- Declaring a knee from one repeat. Three repeats, median and range.
- A run with `dropped_iterations > 0` is not a clean step. Decide if it is the generator or the collapse, and say which.
- Results on one machine are relative; I must not quote them as absolute capacity.
- The first seconds of a run include warm-up effects; I read panels after the first 5 s.
