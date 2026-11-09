# E0.9 Payload size scaling: Learnings

`/echo` takes a JSON body, parses it, and sends it back. If I make the body 10 times bigger, the server has more bytes to read from the socket, more bytes to turn into a string, more to parse, more to stringify, and more to write back. The question is how much each of those costs, and which one starts to matter first.

The cycle for one request:

1. The kernel receives the TCP segments of the body into the socket receive buffer.
2. Node's event loop sees the socket is readable and reads the bytes (in chunks, one `read()` syscall per chunk).
3. The chunks are joined into one Buffer and decoded into a string (UTF-8).
4. `JSON.parse` turns the string into a JS object (this runs synchronously on the one JS thread).
5. `JSON.stringify` turns the object back into a string.
6. The string is written to the socket (`write()` syscall); the kernel copies it into the send buffer and sends it.
7. The garbage collector later frees the string, the object and the Buffers.

Our hypothesis: `/echo` throughput falls roughly with payload size once JSON parse and serialize dominate.

See also: `notebook/stage-0/learning/node_internals/2. cpu work blocks everything.md`, `6. gurbage collection.md`, `node-listen-to-handler-explained.md`, `load-testing-science/4. Little's law.md`, `5. The knee.md`, `machine-and-network-fundamentals/6. Syscall cost.md`, `7. cpu-views.md`.

## Concepts

### Fixed cost plus per-byte cost: t(S) = a + b \* S

Every request costs some CPU time that does not depend on the body (parse the HTTP headers, route, create objects, a few syscalls, write the response headers): call it `a`. On top of that every byte costs a little: `b` per KB. So the CPU time per request is `t(S) = a + b * S`. The maximum rate of one thread is `1 / t(S)`.
Why it matters here: for small bodies `a` dominates, so 100 B and 1 KB should look almost the same. For big bodies `b * S` dominates, so the rate falls almost in proportion to the size. The hypothesis is only true in the second region. Example with my guesses (a = 60 us, b = 10 us/KB): 100 B costs 61 us, 100 KB costs 1060 us, so the body is 1000x bigger but the cost is only 17x bigger.

### Bytes per second vs requests per second

At the limit the server does `rate * size` bytes per second. Requests per second goes down with size, bytes per second goes up and then flattens when the CPU is full. Two different bottlenecks: small bodies are limited by requests per second (per-request overhead), big bodies by bytes per second (per-byte cost).

### Why the shape of the JSON matters, not only its size

`JSON.parse` has to look at every character, but it works much faster on one long string (it can scan quickly for the closing quote) than on thousands of small fields (it has to create a property, a key string and a value for each one). So a 100 KB body made of one string field and a 100 KB body made of 4000 small fields can have very different parse cost. That is why I run `SHAPE=obj` first and `SHAPE=str` as a second pass. The bytes are the same, the cost is not (prediction, not verified).

### How Node reads a request body

The kernel hands data to Node in chunks (the read size is usually 64 KB). A 100 B body is one chunk. A 100 KB body is at least two chunks, so at least two `read()` syscalls, and Node must join them (a Buffer concat, which copies the bytes again) before decoding. More bytes means more copies: kernel to user, chunk to joined Buffer, Buffer to string, string to parse, object back to string, string to socket buffer.

### Loopback, MTU and segments

My load generator and server are on the same machine, so traffic goes through the loopback interface `lo`. Its MTU is usually 65536 bytes, so a 100 KB body needs at least 2 TCP segments, while 100 B needs one. Loopback hides the real network cost (no wire, no packet loss, no latency), so the kernel cost I see here is a lower bound for a real network. This experiment gives relative numbers, not capacity numbers.

### Socket buffers

Each TCP socket has a receive buffer and a send buffer in the kernel (sysctl `net.ipv4.tcp_rmem` / `tcp_wmem`, three numbers: min, default, max). If a 100 KB body does not fit in the buffer, the sender has to wait for the receiver to read. This matters only if the server is slow to read, which is what happens near saturation. I record these sysctls in `env.txt`.

### Garbage collection and external memory

Strings and objects live in the V8 heap (`nodejs_heap_size_used_bytes`). The raw Buffers of request bodies live outside the heap (`nodejs_external_memory_bytes`). Bigger bodies mean bigger short-lived allocations, so more frequent and longer GC. I expect the fraction of time spent in GC to rise with size (guess: under 5% for small bodies, maybe 5-15% at 100 KB). Not verified.

### Open model, warm-up and Little's Law

k6 `constant-arrival-rate` keeps starting requests on schedule, even if the server is slow, so latency can grow without limit above the knee (no coordinated omission). By Little's Law, requests in flight = rate x latency. At the knee the in-flight count and the number of k6 VUs start to climb. If `dropped_iterations` is above 0, k6 itself could not keep up and the run is discarded.
Each run starts with a 10 s warm-up at 25% of the rate. Reason: V8 compiles hot functions after they have run a while (JIT), so the first seconds are slower. The warm-up requests are tagged `stage=warmup` and the dashboard filters `stage="steady"` for k6 numbers.

### Why heavy tools are only in probe runs

`strace` and `perf` slow the server (strace a lot, perf a little). In E0.6 strace inside the run polluted latency numbers. So measured runs only read cheap counters (`nstat`, `/proc`) before and after, and the probes are separate runs, one per size, whose latency I never quote.

### What the per-size masking in PromQL does

The server metrics have no `size` label, because the server does not know which size k6 is sending. To draw one line per size I use `(server_expression) and on() (sum(rate(k6_http_reqs_total{size="1000",stage="steady"}[$__rate_interval])) > 0)`. `and on()` keeps the left series only when the right side exists. The right side exists only while k6 is sending that size, so each line is visible only during runs of its size. Caveat: k6 series are not marked stale, so the mask can stay true for up to one `rate()` window after a run ends.

### CPU per request in PromQL

`rate(process_cpu_seconds_total[w])` is CPU seconds used per second (cores). Dividing by requests per second gives CPU seconds per request; times 1 000 000 gives microseconds. This is the `t(S)` of my model, measured. It includes a little CPU for `/metrics` scrapes.

## Explanation of the k6 run command (through runner.sh)

```bash
RESULTS_NAME=e09 LABEL=e09-obj-s1000-r8000-n1 SCRIPT=E0.9/script.js ROUTE=/health \
  RATE=8000 DURATION=60s WARMUP=10s PAYLOAD_BYTES=1000 SHAPE=obj PHASE=measured ./runner.sh
```

1. `RESULTS_NAME` - the results folder under `results/`; setting it skips runner.sh's question.
2. `LABEL` - becomes part of the `testid` tag, so every k6 series in Prometheus can be traced to one run.
3. `SCRIPT` - which k6 script to run, relative to runner.sh.
4. `ROUTE=/health` - only used by runner.sh for its preflight `curl` (a GET). `/echo` needs a POST, so the script has its own `ECHO_ROUTE`.
5. `RATE`, `DURATION`, `WARMUP`, `PAYLOAD_BYTES`, `SHAPE`, `PHASE` - runner.sh forwards every other environment variable to k6 as `-e NAME=value`, so the script reads them with `__ENV`.
6. Inside runner.sh k6 runs under `taskset -c 1,2,3,7,8,9` and pushes metrics to Prometheus with `-o experimental-prometheus-rw`.

## Explanation of the echo check command (preflight)

```bash
curl -sS --max-time 10 -o /dev/null -w '%{http_code} %{size_download}' \
  -H 'Content-Type: application/json' --data-binary @body.json http://localhost:8000/echo
```

1. `-s` - silent, no progress bar. `-S` - but still show errors.
2. `--max-time 10` - give up after 10 s, so a hanging server does not block the script.
3. `-o /dev/null` - throw the response body away.
4. `-w '%{http_code} %{size_download}'` - after the transfer print the HTTP status and how many bytes came back. If I sent 100 000 B and got about 100 000 B back, `/echo` really echoes the whole body.
5. `-H 'Content-Type: application/json'` - tells the server the body is JSON.
6. `--data-binary @body.json` - send the file exactly as it is. (`-d` would strip newlines.) `@` means "read from this file".

How the body file is made: `{ printf '{"data":"'; head -c N /dev/zero | tr '\0' x; printf '"}'; }`. `head -c N /dev/zero` gives N zero bytes, `tr '\0' x` turns each into the letter x, and the printf parts add the JSON around it.

## Explanation of the nstat command

```bash
nstat -az TcpActiveOpens TcpPassiveOpens TcpRetransSegs TcpExtListenOverflows TcpExtListenDrops IpExtInOctets IpExtOutOctets
```

1. `nstat` - prints kernel network counters (from `/proc/net/snmp` and `/proc/net/netstat`).
2. `-a` - absolute values since boot. Without it nstat prints only the change since its last call and keeps history in a file, which would be confusing.
3. `-z` - also print counters that are zero.
4. The names: `TcpPassiveOpens` = connections the server accepted; `TcpActiveOpens` = connections opened by this machine (k6, since it is the same box); `TcpRetransSegs` = retransmitted segments; `ListenOverflows` / `ListenDrops` = accept queue overflowed; `IpExtInOctets` / `OutOctets` = IP bytes in and out (on loopback each payload byte is counted on both sides).
   How to read: the counters are cumulative. I take them before and after a run and subtract (post - pre). Compare `IpExtInOctets` growth with requests x size to check my byte arithmetic. `TcpPassiveOpens` growth should be small (keep-alive). `ListenOverflows`, `ListenDrops` and `TcpRetransSegs` should be 0.
   It is a cheap read, safe inside a measured run.

## Explanation of the /proc server CPU snapshot

```bash
awk '{print "utime_ticks", $14, "stime_ticks", $15}' /proc/$PID/stat
grep -E 'ctxt_switches' /proc/$PID/status
getconf CLK_TCK
```

1. `/proc/<pid>/stat` is one line with many fields. Field 14 is `utime` (time the process spent in user mode) and field 15 is `stime` (kernel mode), both in clock ticks. `awk '{print $14, $15}'` picks them.
2. `getconf CLK_TCK` gives ticks per second (usually 100), so seconds = ticks / 100.
3. `voluntary_ctxt_switches` and `nonvoluntary_ctxt_switches` in `/proc/<pid>/status` count how often the process gave up the CPU by itself (waiting for I/O) or was forced off (preempted).
   How to use: (post - pre) ticks / 100 = CPU seconds in the run; divided by the number of requests = CPU per request, without relying on Prometheus. The thread used by Node's libuv helpers is included in the process total.
   Caveat: the whole process is counted, including the `/metrics` scrapes.

## Explanation of the perf commands (probe runs only)

```bash
sudo -n perf record -F 499 -p $PID -o captures/<testid>.perf.data -- sleep 10
sudo -n perf report -i captures/<testid>.perf.data --stdio --no-children --sort symbol
```

1. `sudo -n` - run as root but never ask for a password (fail instead). Reading another process needs root or a permissive `kernel.perf_event_paranoid`.
2. `perf record` - sample where the CPU is executing.
3. `-F 499` - 499 samples per second. A prime-ish number avoids lining up with timers. 10 s gives about 5000 samples, enough to see big shares.
4. `-p $PID` - only the server process.
5. `-o file` - where to store the raw samples.
6. `-- sleep 10` - the command that defines how long recording lasts: perf records until `sleep` exits.
7. `perf report --stdio` - print text instead of the interactive screen.
8. `--no-children` - show the time in the function itself (self time), not including the functions it calls. Otherwise the top frames always look huge.
9. `--sort symbol` - group the samples by function name.
   Output columns: percentage of samples, then the symbol. Look for V8 JSON functions (`JsonParser`, `JsonStringify`), `memcpy`, `Builtin:` entries, GC functions, kernel symbols such as `copy_user`, `tcp_sendmsg`, and JS function names (these show because the probe server runs with `--perf-basic-prof`, which writes `/tmp/perf-<pid>.map` mapping JIT code addresses to names).
   Warning: `--perf-basic-prof` and perf both slow the server a little. These are probe runs. I never quote latency from them.

## Explanation of the strace command (probe runs only)

```bash
sudo -n timeout 10 strace -c -f -p $PID
```

1. `timeout 10` - stop strace after 10 s (strace by itself runs until I stop it).
2. `strace -c` - do not print every call, only a summary table at the end.
3. `-f` - also follow all threads (Node has the main thread plus libuv and V8 helper threads).
4. `-p $PID` - attach to the running server.
   Output columns: `% time` (share of syscall time), `seconds`, `usecs/call`, `calls`, `errors`, `syscall`. Compare `calls` for `read`, `write` / `writev`, `epoll_wait` between sizes: for a 100 KB body I expect more `read` calls per request than for 100 B (multiple chunks). Divide `calls` by the number of requests in the window to get calls per request.
   Warning: strace slows the process a lot (it stops it at every syscall). Never during a measured run. This is exactly the E0.6 lesson.

## Explanation of the ss command

```bash
ss -s
```

Prints a summary of all sockets (total, TCP established, closed, orphaned, TIME_WAIT). Cheap enough for probes. In `settle` the script uses `ss -Htan state time-wait | wc -l`: `-H` removes the header line (otherwise the count is off by one), `-t` TCP, `-a` all states, `-n` numbers instead of names, `state time-wait` filter. It waits until fewer than 200 TIME_WAIT sockets remain.

## Explanation of the PromQL used for the verdict

- `histogram_quantile(0.99, sum by (size)(rate(k6_http_req_duration_seconds{stage="steady"}[$__rate_interval])))` - k6 sends its duration as a native histogram, so the series has no `_bucket` suffix and no `le` label. `rate()` over a native histogram gives the rate of observations per bucket, `sum by (size)` combines series, `histogram_quantile` reads the p99.
- `histogram_quantile(0.99, sum by (le)(rate(http_request_duration_seconds_bucket{route="/echo"}[...])))` - the classic form for the server's own histogram; `le` must stay in the `by`. The result cannot be above the highest finite bucket (clamping).
- `100 * (1 - rate(node_cpu_seconds_total{mode="idle",cpu=~"0|6"}[w]))` - `node_cpu_seconds_total{mode="idle"}` counts idle seconds per CPU. `rate` is the idle fraction, `1 - idle` is busy, times 100 gives percent. `cpu=~"0|6"` is a regex match on the CPU number.

## How to read the output tables

| Source           | Column / line                     | Meaning                                                                |
| ---------------- | --------------------------------- | ---------------------------------------------------------------------- |
| `nstat -az`      | `IpExtInOctets` post - pre        | bytes received at IP level during the run (loopback counts both sides) |
| `nstat -az`      | `TcpPassiveOpens` post - pre      | connections accepted; small = keep-alive worked                        |
| `/proc/pid/stat` | utime / stime ticks               | user and kernel CPU of the server, 1/100 s each                        |
| `strace -c`      | `calls`                           | number of syscalls in the 10 s window                                  |
| `strace -c`      | `% time`                          | share of time spent inside the kernel for that syscall                 |
| `strace -c`      | `usecs/call`                      | average time per call (inflated by strace itself)                      |
| `perf report`    | first column                      | share of CPU samples in that function (self time)                      |
| k6 summary       | `http_req_duration{stage:steady}` | latency of the measured part only                                      |
| k6 summary       | `dropped_iterations`              | requests k6 could not start on time; must be 0                         |

## Pitfalls to watch in this experiment

- Same rates for every size: a 100 KB request cannot reach 16 000/s, a 100 B request does not show a knee at 1 000/s. That is why every size has its own ladder.
- The generator can become the bottleneck for big bodies (copying and sending 100 KB x 1000/s = 100 MB/s from k6). Check the generator CPU panel (keep under 70%) and `dropped_iterations`.
- Testing only one JSON shape and concluding "bytes cost X". Shape changes the cost (second pass `SHAPE=str`).
- Body size limit in the server: a rejected 100 KB request is fast and would look like extra capacity. The preflight echo check and the failed-rate panel guard against that.
- Loopback hides the real network, so the results are relative, not capacity numbers.
- strace, perf, or `NODE_OPTIONS=--perf-basic-prof` inside measured runs.
- Heap left large by a previous 100 KB run affecting the next small run. The script alternates sizes and waits at least 8 s between runs; I should still watch the heap panel.
- Warm-up: the first seconds of each run are tagged `stage=warmup`; panels that are not filtered by stage (all server-side panels) still include them, so I read the steady part only.
- Stale k6 series: the last value of a k6 series can linger for minutes after the run.
- Only one repeat, or the best repeat: report median and range of 3.
- Instrumentation cost: if I add the parse/stringify timers, they must stay on for all sizes.
