# E0.8 Accept queue and backlog: Learnings

This experiment is about what happens to a new TCP connection **before** my Node server even knows it exists. When a client connects, the kernel does the TCP handshake on its own and puts the finished connection in a queue (the accept queue). Node later takes connections out of that queue with `accept()`. If Node is busy, the queue fills up, and when it is full the kernel starts refusing new connections. A burst of connections can therefore fail or be delayed even though the average load is tiny.

The cycle I want to see:

1. The event loop is blocked (my `/stall` route), so Node does not call `accept()`.
2. A burst of clients sends SYN packets. The kernel answers, finishes the handshakes and puts connections in the accept queue.
3. The queue reaches its limit (backlog). The kernel drops the next SYN (or the last ACK of a handshake) and increments `ListenOverflows` and `ListenDrops`.
4. The dropped clients wait for their SYN retransmit timer (1 s, then 2 s more, ...). Their `connect()` takes seconds.
5. The loop becomes free, Node accepts everything in the queue, and the late retries finally succeed.

Our hypothesis: a burst bigger than the backlog gives resets or timeouts even when the average load is low. (My prediction in the README is that, with default settings, I will mostly see **delays**, not resets.)

See also (my earlier notes): `notebook/stage-0/learning/machine-and-network-fundamentals/2. TCP-Lifecycle.md`, `3. listen-backlog.md`, `8. TCP-sysctls.md`, and `notebook/stage-0/learning/node_internals/1. event loop.md`, `2. cpu work blocks everything.md`.

## Concepts

### The TCP handshake and the two queues

- **What it is.** SYN (client) -> SYN-ACK (server) -> ACK (client). The Linux kernel keeps two queues per listening socket: the **SYN queue** (half-open connections that got a SYN and are waiting for the final ACK) and the **accept queue** (fully established connections waiting for the application to call `accept()`).
- **Why it matters here.** The application is not involved in the handshake. A client can see "connected" while the server application has never heard of it.
- **The number that matters:** the accept queue size = the backlog. When it is full, new connection attempts are not accepted.
- **Example.** Backlog 16, burst of 200, loop blocked: the first 17 get in; the other 183 are refused.

### listen(backlog), somaxconn and the real queue size

- `server.listen(port, host, backlog)` in Node calls the C function `listen(fd, backlog)`. Node's default backlog is **511**.
- The kernel uses **min(backlog, net.core.somaxconn)**. So asking for 511 with somaxconn 16 gives 16. The kernel does this silently, no error. That is why I check the real value with `ss -ltn` (column Send-Q) instead of trusting my code (cell B1).
- The kernel reads somaxconn when `listen()` is called, so changing the sysctl later does not change an already running server. This is why `step.sh` restarts the server for every cell.
- The queue holds **backlog + 1** connections: the kernel's check is "is the queue length greater than the max", so one more fits. (From my reading of the kernel; I will see it in the peak Recv-Q.)

### What is Send-Q and Recv-Q

Both are related to accept queue.

#### 1. For a listening socket

Example: `ss -ltn`

Output:

| State  | Recv-Q | Send-Q | Local Address:Port |
| ------ | ------ | ------ | ------------------ |
| LISTEN | 5      | 10     | 0.0.0.0:3000       |

- **Recv-Q:** Number of completed connections currently waiting for the application to call accept().
- **Send-Q:** Maximum accept queue size configured for the listening socket (effective backlog).

In this example:

- Recv-Q = 5: Five connections are waiting to be accepted.
- Send-Q = 10: The configured accept queue limit is 10.

#### 2. For an established TCP connection

For a connected socket, the meanings are different:

- **Recv-Q:** Bytes received from the remote peer but not yet read by the local application.
- **Send-Q:** Bytes sent by the local application but not yet acknowledged by the remote peer.

For example:

| State | Recv-Q | Send-Q | Local Address:Port | Peer Address:Port |
| ----- | ------ | ------ | ------------------ | ----------------- |
| ESTAB | 2048   | 4096   | 127.0.0.1:3000     | 127.0.0.1:50000   |

- Recv-Q = 2048: 2 KB waiting for your application to read.
- Send-Q = 4096: 4 KB waiting for TCP acknowledgment from the peer.

### What the kernel does when the accept queue is full

- **A new SYN arrives:** the kernel drops it silently (no reply). The client thinks the packet was lost and retries.
- **The last ACK of a handshake arrives (connection was already in the SYN queue):**
  - `tcp_abort_on_overflow = 0` (default): the kernel ignores the ACK. The client believes it is connected (it already got the SYN-ACK) and sends data, but the server side is still waiting; the server retransmits SYN-ACK, hoping for a new ACK later.
  - `tcp_abort_on_overflow = 1`: the kernel sends a **RST**, so the client sees "connection reset".
  `tcp_abort_on_overflow`: nothing to do with syn  request accept or reject.
- So: resets are not the default behaviour. With the default setting I expect **delay**, not resets. Cell B2 tests the other setting.
- Counters: `TcpExtListenOverflows` counts "accept queue was full", `TcpExtListenDrops` counts packets dropped because of a listen problem (it is at least the overflows). Both are system-wide.

### SYN retransmission timers

- When the client's SYN gets no answer it sends it again after about **1 s**, then after **2 s** more (3 s since the start), then **4 s** more (7 s), doubling each time (Linux default `tcp_syn_retries=6`, about 127 s in total).
- This is why a dropped connection shows up as a connect time of about 1 s, 3 s, 7 s... and not as a random delay.
- My prediction maths: stall from 0 to 1.5 s, burst at 0.2 s. Retry at 1.2 s: queue still full (stall not finished) so dropped again. Retry at 3.2 s: loop is free and the queue has been drained, so it works. Connect time about 3.0 s.

### Why a blocked event loop causes it

- Node accepts connections from the event loop (libuv polls the listening socket and calls `accept()`). A synchronous 1.5 s loop means 1.5 s with no `accept()`. The kernel keeps accepting handshakes into the queue during that time, up to the backlog.
- Connections already in the queue are fine but their request is only read after the stall. Expected waiting about 1.5 - 0.2 = 1.3 s.
- A free loop drains the queue quickly, so the same burst without a stall should not overflow (control cells B3a and B3b). I do not know yet what happens with backlog 4 and no stall.

### Burst vs steady state

- 200 connections within 1.5 s is about 133 connections per second on average: nothing for a server. What matters is the **peak**: all 200 arrive in a few milliseconds while the loop cannot accept. The queue is a buffer for exactly this, and it is small.
- Server-side metrics cannot see the problem: `http_request_duration_seconds` starts when Node has parsed the request, after the queue wait. Only the client (k6) sees the connect time. This is a reminder that a healthy server dashboard does not mean healthy clients.

### k6 concepts used

- `noConnectionReuse: true`: every request opens a new TCP connection. Without it each VU would keep one connection and there would be no burst of new connections.
- `per-vu-iterations` with `vus: N, iterations: 1`: N virtual users, each runs the function once, and they all start together. `startTime` delays a scenario, so the `stall` scenario (t=0) and the `burst` scenario (t=200 ms) start in order in one k6 run.
- Not an open-model arrival rate: here I look at N specific connections that arrive together, not a steady rate. Because of that `dropped_iterations` does not apply; I check `k6_iterations_total` equals the burst size (+1 for the stall request).
- `res.timings.connecting` is the TCP connect time in ms; `res.timings.waiting` is the time from sending the request to the first byte.
- The k6 `timeout` covers the whole request including the connect, so a 2 s timeout cuts off a connection that needs 3 s to connect (B4).

### Prediction maths (copied from the README)

Overflow count about 2 x (N - backlog - 1): the connections that did not fit are dropped twice (first SYN at 0.2 s, retry at 1.2 s) while the loop is still blocked. For N=200 and backlog 16: 2 x 183 = 366. Slow-connect connections about 183. B4 timeouts about 200 - 17 = 183.

## Explanation of the accept queue command: `ss -tln '( sport = :8000 )'`

```bash
ss -Htln "( sport = :8000 )"
```

1. `ss` shows socket statistics (the modern `netstat`).
2. `-H` no header line, so scripts can parse the output.
3. `-t` TCP only.
4. `-l` only listening sockets.
5. `-n` numbers instead of names (no DNS lookups, faster).
6. `( sport = :8000 )` filter: source port is 8000, which for a listening socket is its own port.

Columns for a **listening** socket (this is different from a normal connection):

| Column             | Meaning for LISTEN                                                   |
| ------------------ | -------------------------------------------------------------------- |
| State              | LISTEN                                                               |
| Recv-Q             | how many connections are in the accept queue right now               |
| Send-Q             | the maximum queue size the kernel is using = min(backlog, somaxconn) |
| Local Address:Port | where it listens                                                     |

What to compare: Send-Q against the number I asked for (does the kernel clamp it?), and the peak of Recv-Q against Send-Q + 1. Cheap command, safe in measured runs (my sampler runs it 10 times per second on a free CPU).

## Explanation of the connection state census: `ss -Htan`

```bash
ss -Htan "( sport = :8000 or dport = :8000 )" | awk '{c[$1]++} END{for (s in c) print s, c[s]}'
```

1. `-t -a -n`: TCP, all states (not only listening), numeric.
2. `( sport = :8000 or dport = :8000 )`: sockets where either end uses port 8000, so I see both the server side and the clients (loopback).
3. `awk '{c[$1]++} ...'`: the first column is the state; count how many sockets are in each state.

States I expect: `SYN-SENT` (client waiting for the SYN-ACK: its SYN was dropped), `SYN-RECV` (server got a SYN or a late ACK, still waiting), `ESTAB`, `TIME-WAIT`. During the stall I expect many SYN-SENT; after the stall (at about 2 s) they should still be SYN-SENT, waiting for the 3.2 s retry. **This is a heavy command** (it walks all sockets), so it runs only in the probe run.

## Explanation of the counters command: `nstat -asz`

```bash
nstat -asz TcpExtListenOverflows TcpExtListenDrops TcpExtTCPSynRetrans TcpExtTCPTimeouts TcpOutRsts
```

1. `nstat` reads the kernel's network counters from `/proc/net/netstat` and `/proc/net/snmp`.
2. `-a` absolute values (without it nstat prints the difference since its last call and keeps a history file).
3. `-s` do not update that history file, so my runs do not disturb each other.
4. `-z` also print counters that are zero (otherwise a counter that did not move is hidden).
5. The names are kernel counter names: `TcpExtListenOverflows` accept queue full; `TcpExtListenDrops` listen drops; `TcpExtTCPSynRetrans` SYN retransmissions; `TcpExtTCPTimeouts` TCP retransmit timer timeouts; `TcpOutRsts` RST packets sent.

Output columns: name, value (a cumulative counter), rate. I take the command **before and after** a run and subtract (`deltas.tsv`). It is cheap, so it is allowed inside measured runs. The counters are system-wide.

## Explanation of the sysctl commands

```bash
sudo -n sysctl -qw net.core.somaxconn=16 net.ipv4.tcp_abort_on_overflow=1
sysctl -n net.core.somaxconn
```

1. `sudo -n` never ask for a password (fail instead); I get the ticket once with `sudo -v` in preflight.
2. `sysctl -w` writes a kernel parameter; `-q` quiet; `-n` print only the value when reading.
3. `net.core.somaxconn`: upper limit of the accept queue for every listener. `net.ipv4.tcp_abort_on_overflow`: 0 ignore the ACK, 1 send RST.
4. These are global for the machine (this network namespace), so the script saves the old values and restores them on exit.

## Explanation of the sampler (`EPOCHREALTIME`, `taskset`)

```bash
taskset -c 4 bash -c 'while :; do read -r _ rq sq _ < <(ss -Htln "( sport = :$PORT )"|head -1); printf "%s,%s,%s\n" "$EPOCHREALTIME" "$rq" "$sq"; sleep 0.1; done'
```

1. `taskset -c 4` pins the sampler to CPU 4, a core that neither the server (0,6) nor k6 (1,2,3,7,8,9) uses, so sampling cannot steal CPU from them.
2. `EPOCHREALTIME` is a bash 5 variable with the time in seconds with microseconds. With a comma-decimal locale it would print `,`, so the script sets `LC_ALL=C`.
3. `read -r _ rq sq _` splits the `ss` line: state (ignored), Recv-Q, Send-Q, the rest.
4. Why: Prometheus scrapes every 1 s but the whole event lasts about 1.5 s, so this 10 Hz CSV is the real record of the queue.

## Explanation of the packet capture command (probe only)

```bash
sudo timeout 30 tcpdump -i lo -nn -tt -U -w file.pcap 'tcp port 8000 and (tcp[tcpflags] & (tcp-syn|tcp-rst) != 0)'
tcpdump -nn -tt -r file.pcap
```

1. `-i lo` capture on the loopback interface (everything here is local).
2. `-nn` no name or port-name lookups; `-tt` print timestamps as seconds since the epoch so I can compute the gaps between retries.
3. `-U` write each packet immediately (packet-buffered) so nothing is lost if the process is stopped.
4. `-w file` write a pcap file; `-r file` read it back.
5. The filter: TCP port 8000 and packets that have the SYN or RST flag set. That keeps only handshake starts, SYN-ACKs and resets (small file).
6. `timeout 30` stops it after 30 s because it runs in the background.
7. What I read from it: how many SYNs each client source port sent (1, 2 or 3), the gap between them (about 1 s and 2 s), SYN-ACKs sent more than once to the same client (the dropped-ACK case), and RST packets.
   It needs root, and it slows the machine slightly, so it is only used in the probe run.

## Explanation of the PromQL used in the panels

- `histogram_quantile(0.99, sum by (cell)(rate(k6_http_req_connecting_seconds{kind="burst"}[$__rate_interval])))`: the k6 histogram is a **native histogram** in this setup (no `_bucket` series in my inventory), so I give `histogram_quantile` the rate of the histogram metric itself. `by (cell)` keeps one line per matrix cell. `kind="burst"` removes the single `/stall` request.
- `rate(node_netstat_TcpExt_ListenOverflows{job="node"}[$__rate_interval])`: events per second. `$__rate_interval` is Grafana's automatic window, at least a few scrape intervals. With a 1 s scrape the window is about 4 s, so a 1.5 s event is smeared; the stat panels use `increase(...[$__range])` for totals instead.
- `up{job="eventpulse-nodejs"}`: 1 if the last scrape worked, 0 if it failed. During the stall `/metrics` cannot answer in time (scrape_timeout 1 s), so it drops to 0. That is indirect proof of the blocked loop.
- `100 * (1 - avg(rate(node_cpu_seconds_total{mode="idle",cpu=~"1|2|3|7|8|9"}[$__rate_interval])))`: busy percent of the pinned k6 CPUs.

## How to read the output files

| File                   | Column or line      | Meaning                                                               |
| ---------------------- | ------------------- | --------------------------------------------------------------------- |
| `runs.tsv`             | `eff_sendq`         | backlog the kernel really used (should equal min(backlog, somaxconn)) |
| `runs.tsv`             | `peak_recvq`        | highest accept queue length seen by the 10 Hz sampler                 |
| `runs.tsv`             | `cfg_check`         | `ok` or `CONFIG_MISMATCH` (suspect cell)                              |
| `deltas.tsv`           | `delta`             | counter after minus before for that run (system-wide)                 |
| `captures/*.queue.csv` | epoch, recvq, sendq | the queue over time at 10 Hz                                          |
| `captures/<probe>.txt` | state census        | sockets per TCP state during and after the stall                      |
| `captures/<probe>.txt` | pcap summary        | SYNs per client port, retry gaps, SYN-ACK repeats, RSTs               |

## Pitfalls I should watch for

- The kernel silently clamps the backlog to somaxconn. Always check `Send-Q`, not the number in my code.
- Changing somaxconn does nothing for a server that is already running.
- A free event loop empties the queue almost instantly, so without the stall I may see no overflow at all.
- The burst must arrive **while** the loop is blocked: the stall request goes first and the burst follows 200 ms later.
- k6 must not be the bottleneck: 1000 VUs need memory and CPU. I check `k6_iterations_total` equals the burst size and the CPU of the k6 cores stays under about 70%.
- Heavy tools (`tcpdump`, full `ss -tan` dumps) only in the probe run; they would change the overflow and retransmit counts (E0.6 lesson).
- The counters are system-wide, so any other traffic on this machine is in them. Compare with the idle baseline.
- Loopback hides real network latency and loss. Only the kernel timers are realistic.
- The server's `/metrics` is unreachable during the stall, so the server series has a gap. That is expected.
- One repeat is a trial. I cannot say anything about variance until I run `REPEATS=3`.
- I have not verified how k6 words the error for a reset or a timeout. The script classifies by text and prints a warning for anything unclassified.
