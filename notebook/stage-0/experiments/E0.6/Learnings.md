# Learnings for experiment E0.6

This experiment is about understanding what a new TCP connection per request costs, compared with reusing one connection (keep-alive).

With keep-alive, a client opens a connection once and sends many requests on it.
Without keep-alive, every request does the full cycle:

1. TCP handshake (SYN, SYN-ACK, ACK).
2. The request and response.
3. Closing the connection (FIN and ACK from both sides).
4. The side that closed first keeps a TIME_WAIT socket for 60 s.

Our hypothesis:
With keep-alive off, the maximum rps drops, TIME_WAIT sockets pile up, and more CPU is spent in the kernel.

See also: `8. TCP-sysctls.md` (port range, TIME_WAIT, `tcp_tw_reuse`, `tcp_max_tw_buckets`, `somaxconn`).

## Explanation of the socket summary command

```bash
watch -n1 'ss -s'
```

1. `watch`: Runs a command again and again and shows the latest output on the screen, so you see numbers change during the load test.
2. `-n1`: Interval of 1 second between runs. (`-n` is the interval, `1` is the number of seconds.)
3. `'ss -s'`: The command to run, in quotes so that `watch` receives it as one piece.
4. `ss`: "Socket statistics". It reads the kernel's socket tables. It is the modern replacement for `netstat`.
5. `-s`: Print a summary only, not one line per socket. It shows the total number of sockets and the TCP counts: `estab` (established), `closed`, `orphaned`, `timewait`.

Use: watch the `timewait` number grow when keep-alive is off, and stay near 0 when it is on.

## Explanation of the TIME_WAIT side commands

```bash
ss -tan state time-wait '( sport = :8000 )' | wc -l   # server side
ss -tan state time-wait '( dport = :8000 )' | wc -l   # client side
```

1. `ss`: Socket statistics (see above).
2. `-t`: Show TCP sockets only (not UDP or unix sockets).
3. `-a`: Show all sockets, both listening and non-listening ones.
4. `-n`: Numeric output. Do not resolve names, so you get `8000` and not `http-alt`, and it is faster.
5. `state time-wait`: Filter. Only show sockets in the TIME_WAIT state. Other states exist too, like `established` or `syn-sent`.
6. `'( sport = :8000 )'`: Filter on the port. It is quoted so the shell does not treat the brackets as special characters.
   - `sport`: The source port, which is the **local** port of the socket.
   - `dport`: The destination port, which is the **remote** port of the socket.
   - `= :8000`: Equals port 8000.
7. `| wc -l`: Pipe the output into `wc`. `-l` counts lines, so it counts the sockets.

Why two commands:

- `sport = :8000` matches sockets whose local port is 8000. These belong to the **server**.
- `dport = :8000` matches sockets whose remote port is 8000. These belong to the **client** (k6), which has a random local port.
- A TIME_WAIT socket is held by the side that closed the connection first, so the two counts show who closed first.

Note: `ss` prints a header line first. In `step.sh` the command is `ss -Htan ...`. `-H` (`--no-header`) removes the header, otherwise `wc -l` counts one line too many.

## Explanation of the kernel counters command

```bash
nstat -az TcpActiveOpens TcpPassiveOpens TcpRetransSegs TcpExtListenOverflows TcpExtListenDrops
```

1. `nstat`: Shows the kernel's network counters (from `/proc/net/snmp` and `/proc/net/netstat`). By default it shows only the change since the last time you ran it, and hides counters that are zero.
2. `-a`: Show the absolute value (the total since boot) and not the change since the last run.
3. `-z`: Show counters with value zero too, so the output always has the same rows.
4. The names at the end: Only show these counters.
   - `TcpActiveOpens`: Connections this machine started (it connected to someone). Here this is the k6 side.
   - `TcpPassiveOpens`: Connections this machine accepted (someone connected to a listening socket). Here this is the Node server side.
   - `TcpRetransSegs`: TCP segments that were sent again because no acknowledgement came. A rise means packet loss or an overloaded receiver.
   - `TcpExtListenOverflows`: Times a connection finished the handshake but the accept queue (the listen backlog) was already full.
   - `TcpExtListenDrops`: Connections or SYN packets dropped because of listen socket problems. It includes the overflows, so it is equal to or bigger than `ListenOverflows`.

The counters are for the whole machine and count since boot. So take the command before and after a run and subtract (`pre`, `mid` and `post` in the captured logs).

## Explanation of the syscall count command

```bash
sudo timeout 10 strace -c -f -p $PID
```

1. `sudo`: Attaching to another process needs root permission.
2. `timeout 10`: Runs the command and stops it after 10 seconds, by sending it SIGTERM. `strace` detaches from the process when it gets this signal and prints its summary.
3. `strace`: A Linux debugging utility that traces the system calls (syscalls) a process makes.
4. `-c`: Count. Do not print every call. Instead print a summary table at the end with the number of calls, errors and time for each syscall.
5. `-f`: Follow threads and child processes. Node has several threads, so without `-f` you would only see the main thread.
6. `-p $PID`: Attach to the already running process with this process id (the Node server).

How to read the table:

| Column       | Meaning                                                                                   |
| ------------ | ----------------------------------------------------------------------------------------- |
| `% time`     | Share of the total time spent in this syscall                                             |
| `seconds`    | Total time inside the syscall. It includes waiting, so `futex` and `epoll_pwait` look big |
| `usecs/call` | Average time per call in microseconds                                                     |
| `calls`      | Number of times the syscall was made. This is the useful column here                      |
| `errors`     | Number of calls that returned an error                                                    |

Syscalls to look for in this experiment:

- `accept4`: Accept a new connection on the listening socket.
- `epoll_ctl`: Add or remove a socket from the epoll set (the list of sockets that Node waits on).
- `setsockopt`: Set a socket option on the new connection.
- `read` and `write`: Read the request and write the response.
- `shutdown`: Close one direction of the connection. It sends the FIN to the client.
- `close`: Release the file descriptor and the socket.
- `epoll_pwait`: Wait until some socket is ready.
- `io_uring_enter`: Submit work to the io_uring interface. It appeared as often as `epoll_ctl`, so it is probably used by libuv for that call (not verified).
- `futex`: Threads waiting for each other. A large time here is waiting, not work.

What to compare: divide the number of calls by the number of requests in the same 10 seconds. That gives syscalls per request for keep-alive on and off.

Warning: `strace` stops the process at every syscall, so it slows a busy server a lot. Do not run it inside a run whose latency or error numbers you want to trust.
