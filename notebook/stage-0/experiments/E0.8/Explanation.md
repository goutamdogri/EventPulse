# E0.8 Experiment explanations

## Hypothesis

A sudden burst of connections beyond the backlog causes connection resets or timeouts even though the average load is low.

## Predictions

Facts I used: the kernel uses `min(backlog, somaxconn)`; the queue holds `backlog + 1` connections; when it is full a new SYN is dropped silently and the client retries it after 1 s, then 2 s later (so at +1 s and +3 s), because the first retransmit timeout is 1 s and it doubles.

Timeline of one overflow cell: stall starts t=0, burst lands t=0.2 s, loop is free at t=1.5 s.

- Connections that fit in the queue: connect is instant, but the request waits until the loop is free: waiting about 1.5 - 0.2 = **1.3 s**.
- Connections that did not fit: SYN dropped at 0.2 s, retry at 1.2 s dropped too (loop still blocked), retry at 3.2 s succeeds: connecting about **3.0 s**.
- The server's own request duration stays tiny because it starts when Node parses the request.

Counts, with `overflows ~ 2 x (N - backlog - 1)` when positive:

| backlog \ N |       50 |       200 |       1000 |
| ----------: | -------: | --------: | ---------: |
|         511 |        0 |         0 |  about 976 |
|         128 |        0 | about 142 | about 1742 |
|          16 | about 66 | about 366 | about 1966 |
|           4 | about 90 | about 390 | about 1990 |

Also: peak `Recv-Q` = backlog + 1 when N > backlog + 1; `Send-Q` = min(backlog, somaxconn); connections needing a SYN retry about N - backlog - 1; ListenDrops >= ListenOverflows.

- B1: `Send-Q` shows 16, not 511, and the cell behaves like `A_b16_n200`.
- B2: some resets, only for connections that got past the SYN stage; I did not know how many.
- B3b: 0 overflows. B3a: I did not know, somewhere between 0 and a few.
- B4: client timeout 2 s is shorter than 3.0 s: about 200 - 17 = 183 timeouts and 17 ok.
- Probe: ports with 3 SYNs about 183; gap SYN1 to SYN2 about 1.0 s, SYN2 to SYN3 about 2.0 s; about 183 sockets in SYN-SENT during the stall, and still SYN-SENT at about 2.0 s.
- Expected verdict: half right. Overflow is real, but with default settings it shows up as connect delays, not resets; failures need a short client timeout (B4) or abort_on_overflow=1 (B2).

## Setup

| Item                      | Value                                                                                                                                                                                                                                                                                                                               |
| ------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Server                    | Stage 0 lab server (Node v24.21.0), pinned to CPUs 0,6, port 8000, restarted for every cell with `LISTEN_BACKLOG`, route `/stall?ms=1500` blocks the loop                                                                                                                                                                           |
| Kernel                    | 7.0.0-38-generic, native Linux, loopback only. `somaxconn` 4096 (original), `tcp_abort_on_overflow` 0 (original), `tcp_max_syn_backlog` 1024, `tcp_syncookies` 1, `tcp_syn_retries` 6, `tcp_synack_retries` 5, `ulimit -n` 1024                                                                                                     |
| `tcp_syn_linear_timeouts` | 4 (read after the experiment, not in `env.txt`)                                                                                                                                                                                                                                                                                     |
| Generator                 | k6 v2.3.0 via `runner.sh`, pinned to CPUs 1,2,3,7,8,9. `per-vu-iterations`, 1 iteration per VU, `noConnectionReuse`                                                                                                                                                                                                                 |
| Queue sampler             | `ss` every 0.1 s, pinned to CPU 4                                                                                                                                                                                                                                                                                                   |
| Matrix                    | 17 measured cells + 1 probe, **REPEATS=1 (trial)**, STALL_MS=1500, burst at +200 ms after the stall request. Part A: backlog {511,128,16,4} x N {50,200,1000} with somaxconn 4096, abort 0, client timeout 10 s. Part B: B1 (somaxconn 16, backlog 511), B2 (abort 1), B3a/B3b (no stall, backlog 4 / 511), B4 (client timeout 2 s) |
| Prometheus                | scrape every 1 s (server scrape timeout 1 s); used only as context                                                                                                                                                                                                                                                                  |
| Dashboard                 | `E0.8 - Accept queue and backlog.json`, screenshot below                                                                                                                                                                                                                                                                            |
| Machine                   | 12 logical CPUs, 6923 MiB available at start, git `e470cdc` (11 dirty files)                                                                                                                                                                                                                                                        |

## Method

1. `step.sh` ran all cells in the order A (interleaved by N, then backlog), B1, B2, B3a, B3b, B4, then the probe last. Each cell: wait until no half-open sockets, set the sysctls, restart the server with the cell's backlog, check the kernel's `Send-Q`, attach the 10 Hz sampler and an `nstat` snapshot, run k6 (stall request at t=0, burst at +200 ms), take the second `nstat` snapshot.
2. The probe run `P_b16_n200` repeats backlog 16 / N=200 with `tcpdump` (SYN and RST packets only) and `ss -tan` state counts at about 0.8 s and 2.0 s. It is not used for headline latency numbers.
3. Every run was kept: `cfg_check` was `ok` in all 18 and `n` equals the burst size in all of them. I changed nothing during the run and discarded nothing.
4. After the run I read `net.ipv4.tcp_syn_linear_timeouts`, and for `A_b16_n200` and `B3a_nostall_b4` I read the timeout rows and the connect-time histograms from `req.csv`.

## Results and observations

![E0.8 dashboard over the whole matrix](E0.8%20-%20Accept%20queue%20and%20backlog.png)

### All 18 runs (from `summary.tsv`)

Connect-time columns count connections by k6's `http_req_connecting`. "wait" is `http_req_waiting` (request sent to first byte).

| cell              |    N |   ok | timeout | reset | connect <0.2 s | 0.2-2 s | 2-5 s | >=5 s | connect p50 / p99 (ms) | wait p50 / p99 (ms) | peak Recv-Q | overflows |
| ----------------- | ---: | ---: | ------: | ----: | -------------: | ------: | ----: | ----: | ---------------------- | ------------------- | ----------: | --------: |
| A_b511_n50        |   50 |   50 |       0 |     0 |             50 |       0 |     0 |     0 | 2 / 4                  | 1329 / 1341         |          51 |         0 |
| A_b128_n50        |   50 |   50 |       0 |     0 |             50 |       0 |     0 |     0 | 1 / 3                  | 1330 / 1345         |          51 |         0 |
| A_b16_n50         |   50 |   50 |       0 |     0 |             17 |       0 |    33 |     0 | 2067 / 2074            | 210 / 1322          |          17 |        94 |
| A_b4_n50          |   50 |   50 |       0 |     0 |              6 |       0 |    44 |     0 | 2078 / 2080            | 851 / 6850          |           6 |       245 |
| A_b511_n200       |  200 |  200 |       0 |     0 |            200 |       0 |     0 |     0 | 2 / 7                  | 1356 / 1391         |         200 |         0 |
| A_b128_n200       |  200 |  200 |       0 |     0 |            129 |       0 |    71 |     0 | 5 / 2066               | 1330 / 1372         |         129 |       142 |
| A_b16_n200        |  200 |  188 |      12 |     0 |             18 |       0 |   182 |     0 | 2078 / 3102            | 821 / 6907          |          18 |       935 |
| A_b4_n200         |  200 |   91 |     109 |     0 |              5 |       0 |   195 |     0 | 3073 / 3097            | 6907 / 7953         |           5 |      1373 |
| A_b511_n1000      | 1000 | 1000 |       0 |     0 |            512 |       0 |   488 |     0 | 143 / 2092             | 1216 / 1379         |         512 |       976 |
| A_b128_n1000      | 1000 | 1000 |       0 |     0 |            129 |       0 |   871 |     0 | 2058 / 3063            | 443 / 1712          |         129 |      3854 |
| A_b16_n1000       | 1000 |  780 |     220 |     0 |             17 |       0 |   983 |     0 | 2067 / 3114            | 957 / 7958          |          17 |      6065 |
| A_b4_n1000        | 1000 |  266 |     734 |     0 |              5 |       0 |   995 |     0 | 2082 / 3112            | 6946 / 7966         |           5 |      7346 |
| B1_clamp_s16_b511 |  200 |  200 |       0 |     0 |             19 |       0 |   181 |     0 | 2086 / 3106            | 458 / 3910          |          18 |      1030 |
| B2_abort1_b16     |  200 |  185 |       0 |    15 |             23 |       0 |   177 |     0 | 2030 / 3050            | 214 / 1729          |          18 |       880 |
| B3a_nostall_b4    |  200 |  127 |      73 |     0 |             26 |      57 |   109 |     8 | 2077 / 5147            | 2306 / 8949         |           0 |      1232 |
| B3b_nostall_b511  |  200 |  200 |       0 |     0 |            200 |       0 |     0 |     0 | 6 / 10                 | 48 / 92             |          53 |         0 |
| B4_timeout2s_b16  |  200 |   17 |     183 |     0 |            200 |       0 |     0 |     0 | 0 / 5                  | 0 / 1323            |          17 |       944 |
| P_b16_n200        |  200 |  189 |      11 |     0 |             18 |       0 |   182 |     0 | 2086 / 3112            | 847 / 7917          |          18 |      1024 |

Totals over the matrix: N = 6200 connections, 4843 ok, 1342 timeouts, 15 resets (dashboard legends say 4.84 K / 1.34 K / 15). ListenDrops equals ListenOverflows in every run.

### Overflows: measured vs predicted (Part A)

| backlog \ N |            50 |             200 |             1000 |
| ----------: | ------------: | --------------: | ---------------: |
|         511 |    0 (pred 0) |      0 (pred 0) |   976 (pred 976) |
|         128 |    0 (pred 0) |  142 (pred 142) | 3854 (pred 1742) |
|          16 |  94 (pred 66) |  935 (pred 366) | 6065 (pred 1966) |
|           4 | 245 (pred 90) | 1373 (pred 390) | 7346 (pred 1990) |

Where the prediction held exactly: backlog 511 with N=1000 (976) and backlog 128 with N=200 (142). Everywhere else the measured number is higher, 1.4x to 3.7x.

### Accept queue over time (changes only, from the 10 Hz CSVs, seconds are relative to the sampler start)

```txt
A_b16_n200       0.84s recvq=18   2.14s recvq=13   2.26s recvq=0
B1_clamp         0.70s recvq=18   2.11s recvq=0    2.73s recvq=10   2.85s recvq=0
B2_abort1        0.82s recvq=18   2.13s recvq=0
B4_timeout2s     0.84s recvq=17   2.17s recvq=0
A_b511_n1000     0.97s recvq=489  1.10s recvq=512  2.28s recvq=385  2.40s recvq=94  2.52s recvq=0
                 3.12s recvq=237  3.24s recvq=55   3.36s recvq=0
B3b_nostall_b511 0.86s recvq=53   0.99s recvq=0
B3a_nostall_b4   (no change seen: recvq stayed 0 at 10 Hz)
```

### Probe run (`P_b16_n200`)

| Observation                              | Value                                                                                                                |
| ---------------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| Client source ports that sent a SYN      | 202                                                                                                                  |
| Ports that sent 1 / 3 / 4 SYNs           | 20 / 147 / 35                                                                                                        |
| Gap SYN 1 to SYN 2                       | avg 1.059 s (min 1.054, max 1.064), 182 ports                                                                        |
| Gap SYN 2 to SYN 3                       | avg 1.024 s, 182 ports                                                                                               |
| Ports that got a SYN-ACK more than once  | 2 of 202                                                                                                             |
| RST packets seen                         | 134                                                                                                                  |
| `ss -tan` during the stall (about 0.8 s) | SYN-SENT 182, ESTAB 39, FIN-WAIT-1 274, TIME-WAIT 898, LISTEN 1 (Recv-Q 18, Send-Q 16)                               |
| `ss -tan` after the stall (about 2.0 s)  | SYN-SENT 182, ESTAB 1, FIN-WAIT-1 195, TIME-WAIT 785 (Recv-Q 0)                                                      |
| `nstat` after minus before               | ListenOverflows +1024, ListenDrops +1024, TCPSynRetrans +401, TCPTimeouts +857, TcpOutRsts +125, TcpRetransSegs +996 |

No socket was in SYN-RECV at either census.

### Client-side detail for two cells (from `req.csv`)

| Cell             | Connect-time histogram (250 ms buckets, number of connections) | Outcomes                                               |
| ---------------- | -------------------------------------------------------------- | ------------------------------------------------------ |
| `A_b16_n200`     | 0 ms: 18, 2000 ms: 94, 3000 ms: 88                             | 188 ok, 12 timeout (max `http_req_duration` 6907.8 ms) |
| `B3a_nostall_b4` | 0 ms: 26, 1000 ms: 57, 2000 ms: 84, 3000 ms: 25, 5000 ms: 8    | 127 ok, 73 timeout (max 8949.9 ms)                     |

Timeout rows (columns: blocked, connecting, sending, waiting, receiving, duration, in ms):

```txt
A_b16_n200      3102.7  3102.7  0.2  6897.5  0.0  6897.7   -> connecting + duration = 10000.4
B3a_nostall_b4  2086.3  2084.7  0.7  7913.6  0.0  7914.3   -> connecting + duration = 9999.0
```

The first five timeout rows of each cell start within 3 ms of each other (A_b16_n200: epoch ms ...928 to ...931; B3a: all ...472).

### Highest dashboard values (legends)

| Panel                                                    | Value                                                             |
| -------------------------------------------------------- | ----------------------------------------------------------------- |
| Connect time p99 (max)                                   | 5.18 s                                                            |
| Waiting time p99 / "total request time" p99 (max)        | 8.71 s / 8.71 s                                                   |
| Accept queue length (max) / backlog in use (max)         | 512 / 511                                                         |
| ListenOverflows rate (max)                               | 1.52 K/s; total over the range 26.7 K (my sum of `deltas`: 26140) |
| SYN retransmits (max)                                    | 764 /s                                                            |
| Accepted by Node (max) / completed by k6 (max)           | 322 /s / 300 /s                                                   |
| Server-side p99 (max, mean) / client-side p99 (max)      | 9.90 ms, 1.60 ms / 8.71 s                                         |
| Event-loop lag max                                       | 1.52 s                                                            |
| Node CPU user (max) / system (max)                       | 65.6% / 4.58%                                                     |
| Context switches (max)                                   | 19.2 K/s                                                          |
| CPU busy of k6 cores / server cores / sampler core (max) | 40.2% / 55.8% / 64.7% (mean 9.3% / 11.6% / 11.7%)                 |
| TIME_WAIT (max)                                          | 3.03 K                                                            |
| Open fds (max)                                           | 46                                                                |
| k6 failed rate (last value)                              | 100%                                                              |

### What I saw

1. The kernel used exactly `min(backlog, somaxconn)` in all 18 runs. B1 asked for 511 with somaxconn 16 and `Send-Q` was 16.
2. In the stall cells the queue was full for about 1.3 s: 0.84 s to 2.14 s in `A_b16_n200` (1.30 s), 0.97 s to 2.28 s in `A_b511_n1000` (1.31 s), 0.82 s to 2.13 s in B2 (1.31 s). That matches stall (1.5 s) minus burst delay (0.2 s).
3. Peak `Recv-Q` was backlog + 1 for 511 (512), 128 (129), most 16 cells (17) and most backlog 4 cells (5). It was backlog + 2 in `A_b16_n200`, B1, B2 and the probe (18) and in `A_b4_n50` (6).
4. Connections that got into the queue waited about 1.3 s for the first byte (`A_b511_n200`: wait p50 1356 ms, `A_b511_n50`: wait p99 1341 ms).
5. Connections that did not fit connected after about **2.07 s**, not 3.0 s (connect p50 2030 to 2086 ms in the cells where more than half of the connections overflowed; 3073 ms in `A_b4_n200`), and a second group connected after about 3.1 s (connect p99 3050 to 3114 ms in the larger cells; in `A_b16_n200` 94 connections at 2.0 s and 88 at 3.0 s).
6. The probe's SYN retries were about 1 s apart both times (1.059 s and 1.024 s), not 1 s then 2 s. `net.ipv4.tcp_syn_linear_timeouts` is 4 on this machine. 147 ports needed 3 SYNs and 35 needed 4.
7. The number of connections that needed a SYN retry matched my prediction `N - backlog - 1` in the cells I can compare: for example `A_b16_n200` 182 (pred 183), `A_b16_n1000` 983 (983), `A_b4_n1000` 995 (995), `A_b4_n200` 195 (195).
8. Overflow counts matched my formula only for `A_b511_n1000` (976, and TCPSynRetrans was also 976) and `A_b128_n200` (142). For small backlogs they were 1.4x to 3.7x higher, and `TCPSynRetrans` stayed near my predicted number (`A_b16_n1000`: 6065 overflows, 2245 SYN retransmits).
9. In the probe, the dropped SYNs I can count from the pcap are 601 sent minus 202 successes = 399, but ListenOverflows rose by 1024 and TcpRetransSegs by 996.
10. Timeouts happened even with a 10 s client timeout: 12, 109, 220 and 734 in `A_b16_n200`, `A_b4_n200`, `A_b16_n1000`, `A_b4_n1000`. In the two cells I looked at, every timed-out request had finished its connect at 2.08 s or 3.10 s, then got no first byte, and `connecting + duration` is 10 s. They started within a few milliseconds of each other and (in the five rows printed) finished connecting at the same moment.
11. `B4_timeout2s_b16`: exactly 183 timeouts and 17 ok.
12. `B2_abort1_b16`: 15 resets, 185 ok, 0 timeouts. All other 17 runs had 0 resets.
13. `B3b` (no stall, backlog 511): 0 overflows, the queue reached 53 and was empty 0.13 s later. `B3a` (no stall, backlog 4): 1232 overflows, 73 timeouts, and the 10 Hz sampler never saw a non-zero queue.
14. Server-side p99 peaked at 9.90 ms (mean 1.60 ms) while client-side p99 peaked at 8.71 s.
15. Event-loop lag peaked at 1.52 s.
16. Equal-configuration cells (effective backlog 16, N=200, stall 1500: A, B1, B4, B2 and the probe) gave 935, 1030, 944, 880 and 1024 overflows, and 12, 0, 183 (2 s timeout), 0 and 11 timeouts.

## Explanation

**Why the queue filled and for how long.** While `/stall` blocks the event loop, Node does not call `accept()`. The kernel still finishes handshakes and parks the connections in the accept queue, up to backlog + 1. I saw exactly that: 512, 129, 17 and 5 for backlogs 511, 128, 16 and 4. The queue emptied about 1.3 s after it filled, which is the part of the stall that remained after the burst arrived.

**Why the overflowed connections connected at 2.07 s and not 3.0 s.** A dropped SYN is retried by the client. I predicted a doubling timer (+1 s, +3 s). The probe shows the first retry at +1.06 s and the second at +1.02 s after that, so on this kernel the early retries are 1 s apart, and `tcp_syn_linear_timeouts` is 4. With the burst at t=0 and the stall ending at +1.3 s: the retry at +1.0 s hits a full queue and is dropped again, the retry at +2.0 s finds the loop free, so the connection completes at about 2.0 s (observed 2.07 s). My doubling assumption was wrong for this kernel. (The exact meaning of the sysctl value is from my memory of kernel documentation; I did not check it, but the observed gaps agree.)

**Why a third and fourth wave exist.** The connections that were dropped all retry at the same instant. In `A_b16_n200` that is 182 SYNs against a queue of 17, so about half of them (88 of 182) are dropped again and connect one second later, at 3.1 s. In the probe 147 ports needed 3 SYNs and 35 needed 4. Each retry wave is itself a burst, so with a small queue the retries keep overflowing. This is why the overflow counts are above my two-wave formula for small backlogs. It is also why the formula was exact for backlog 511 with N=1000: the 488 retries at +2.0 s all fit in a queue of 512. The queue samples show it: 512 at about 1.1 s, 237 at 3.12 s (the retry wave arriving about 2 s later), then empty. (Inferred.)

**Why overflow events are more than dropped SYNs.** The pcap shows about 399 dropped SYNs in the probe, but ListenOverflows rose by 1024 and TcpRetransSegs by 996. The difference is about 625 refused events that were not SYNs, and 996 - 401 (SYN retransmits) = 595 retransmitted segments that were not SYNs. These numbers are close, which fits a picture where connections that got a SYN-ACK but not a place in the queue keep retransmitting packets that are refused each time. I did not capture data or ACK packets, so I cannot confirm it. (Likely, not verified.)

**Why timeouts appeared even with a 10 s limit.** k6's `http_req_duration` does not include the connect time (the timeout rows show `connecting + duration` = 10.0 s). The timed-out requests connected at 3.10 s (A_b16_n200) or 2.08 s (B3a), sent the request (sending 0.2 to 0.7 ms) and then received nothing for 6.9 s or 7.9 s until k6's 10 s timeout. The five rows of each cell that I looked at finished connecting at the same moment, so they seem to belong to one retry wave. My reading is that the client's handshake completed but the server never took the connection into the accept queue (the final ACK was dropped), so no one answered the request. This fits the wave timing and the extra refused packets, but it conflicts with the probe, where only 2 of 202 ports got a repeated SYN-ACK while 11 requests timed out. I cannot resolve that with the data I have. (Not resolved.)

**Why B3a overflowed with a free event loop.** The 200 connection attempts started within about 3 ms of each other (`start_ms` of the timeout rows). From the queue samples of `A_b511_n1000` Node drained roughly 1,500 to 2,400 connections per second (385 to 94 in 0.12 s, 237 to 55 in 0.12 s). At that speed 200 connections need roughly 80 to 130 ms, while a queue of 5 holds 5 of them. So a small queue overflows on a burst even when the loop is free, and the kernel counters (1232) caught it while the 10 Hz sampler (queue stayed 0) could not. The `B3b` queue reached 53 at the sample and drained within 0.13 s, which fits the same speed. (Inferred.)

**Why the server saw nothing.** The server starts timing a request when Node has parsed it. Everything that happened in the queue or in SYN retries is before that point, so server p99 stayed under 10 ms while the client waited up to 8.7 s.

**About abort_on_overflow=1.** In B2 the connections that would otherwise hang got a reset: 15 resets and 0 timeouts. But B1, with the same effective backlog and `abort=0`, also had 0 timeouts, so one run cannot tell me that abort=1 prevents timeouts. (Not resolved.)

## Status of each claim

| #   | Claim                                                                                                  | Status                                                           |
| --- | ------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------- |
| 1   | The kernel uses min(backlog, somaxconn)                                                                | Observed (18 of 18 runs, B1)                                     |
| 2   | The queue is full for about 1.3 s during the stall                                                     | Observed                                                         |
| 3   | Peak queue is backlog + 1                                                                              | Observed in most cells; +2 in five runs, reason Not resolved     |
| 4   | SYN retries come at about 1 s steps                                                                    | Observed (pcap gaps 1.059 s, 1.024 s)                            |
| 5   | The 1 s steps come from `tcp_syn_linear_timeouts` = 4                                                  | Likely, not verified (value observed, meaning from memory)       |
| 6   | Retry waves overflow again when the queue is small (3rd and 4th SYN)                                   | Inferred (147/35 SYN counts, 94/88 histogram, queue samples)     |
| 7   | My overflow formula holds only when the retry wave fits                                                | Observed for 2 cells, Inferred for the rest                      |
| 8   | The extra overflow events are refused non-SYN packets                                                  | Likely, not verified (arithmetic only, no data packets captured) |
| 9   | Timeouts are k6's 10 s limit, after the connect had finished                                           | Observed (timeout rows, 3102.7 + 6897.7 = 10000.4)               |
| 10  | The cause of "no first byte" is a dropped final ACK                                                    | Not resolved (conflicts with 2 of 202 SYN-ACK repeats)           |
| 11  | A free loop with a 4-slot queue still overflows on a burst                                             | Observed (B3a counters); the drain-speed reason is Inferred      |
| 12  | Node drains about 1,500 to 2,400 connections per second                                                | Inferred (100 ms samples)                                        |
| 13  | abort_on_overflow=1 turns hangs into resets                                                            | Observed 15 resets; effect on timeouts Not resolved              |
| 14  | Server-side metrics cannot see the queue wait                                                          | Observed (9.9 ms vs 8.71 s)                                      |
| 15  | RST counts (6, 441, 125, 1) and FIN-WAIT-1 (274) are inflated by leftover sockets of the previous cell | Likely, not verified                                             |
| 16  | The 8 connections at about 5.1 s in B3a                                                                | Not resolved (not a 1 s step, and linear timers should give 6 s) |

## Prediction vs observation

| Prediction                                                                          | Observed                                            | Verdict                           |
| ----------------------------------------------------------------------------------- | --------------------------------------------------- | --------------------------------- |
| Send-Q = min(backlog, somaxconn); B1 shows 16                                       | 16 in B1, all cells matched                         | Matches                           |
| Peak Recv-Q = backlog + 1                                                           | +1 mostly, +2 in five runs                          | Partly                            |
| Queued connections wait about 1.3 s                                                 | 1330 to 1356 ms                                     | Matches                           |
| Overflowed connections connect after about 3.0 s                                    | about 2.07 s (second group 3.1 s)                   | Wrong                             |
| SYN gaps about 1 s then about 2 s                                                   | 1.059 s then 1.024 s                                | Wrong                             |
| Overflows about 2 x (N - backlog - 1)                                               | exact in 2 cells, 1.4x to 3.7x higher in the others | Partly (wrong for small backlogs) |
| Connections needing a SYN retry about N - backlog - 1                               | 182/183, 983/983, 995/995, 195/195                  | Matches                           |
| ListenDrops >= ListenOverflows                                                      | equal in all 18 runs                                | Matches                           |
| Probe: about 183 sockets in SYN-SENT during and after the stall                     | 182 and 182                                         | Matches                           |
| Probe: ports with 3 SYNs about 183                                                  | 147 with 3, 35 with 4                               | Partly                            |
| B1 behaves like A_b16_n200                                                          | 1030 overflows, 181 in the 2-5 s group, 0 timeouts  | Matches                           |
| B2: some resets, unknown count                                                      | 15                                                  | Matches                           |
| B3b: 0 overflows                                                                    | 0                                                   | Matches                           |
| B3a: 0 to a few overflows                                                           | 1232 overflows, 73 timeouts                         | Wrong                             |
| B4: about 183 timeouts, 17 ok                                                       | 183 and 17                                          | Matches                           |
| Server-side p99 stays tiny                                                          | 9.9 ms vs 8.71 s client                             | Matches                           |
| With default settings: delays, not failures, unless the timeout is short or abort=1 | 1075 timeouts in Part A with a 10 s timeout         | Wrong                             |
| Event-loop lag about 1.3 to 1.5 s                                                   | max 1.52 s                                          | Matches                           |

## Verdict on the hypothesis

Confirmed with corrections. A burst larger than the backlog does hurt even at low average load: the average here is about 133 connections per second for N=200 (and `B3a` failed with no stall at all). But the failure is mostly **timeouts, not resets**. With the default `tcp_abort_on_overflow=0` there were no resets in 17 runs. A connection that does not fit first suffers a delay of about 2 s (3 s for the second group), and a part of the connections ended as a timeout even with a 10 s limit (1075 of the 5000 connections in Part A, and 734 of 1000 in `A_b4_n1000`). A bigger backlog moves the cliff but does not remove the stall: with backlog 511 and N=1000 nothing failed, but 488 connections still waited about 2.09 s.

## Limitations

- **One repeat per cell.** Equal-configuration cells differ: timeouts 12, 0 and 11 (A, B1, probe), RSTs 6, 441 and 125, overflows 880 to 1030. The timeout and RST numbers are not reliable; the connect-time groups and the overflow counts are stable to about +/-10% in those cells.
- **Possible contamination between cells.** `settle` waits only for SYN-state sockets, not for FIN-WAIT or established leftovers. The probe census showed 274 FIN-WAIT-1 sockets right after B4 (183 timeouts), and B1 (RSTs 441) ran right after `A_b4_n1000` (734 timeouts). The RST and counter details of those runs may include the previous cell.
- **Probe last, with `tcpdump` attached.** Its latency numbers are not used. The probe's pcap only had SYN and RST packets, so I cannot see data retransmissions.
- **The 10 Hz sampler is too slow for B3a.** It saw no queue while the counters show 1232 overflows. The peak values may be higher than sampled.
- **My dashboard label was wrong.** The panel "Total request time p99" is k6's `http_req_duration`, which does not include the connect time. The time a client really felt is `connecting + duration`. All "dur_p99" numbers in `summary.tsv` have the same meaning.
- **Loopback only.** There is no real network delay or loss. The kernel timers are real, but I did not test other kernels, so the linear 1 s retries may not hold elsewhere.
- **System-wide counters.** ListenOverflows, RSTs and retransmits include any other traffic on this machine. The idle baseline panel stayed flat before the first cell.
- **Prometheus at 1 s** cannot resolve the 1.5 s event, and the server's `/metrics` is unreachable during the stall. I used it only as context. The sampler core reached 64.7% at max (mean 11.7%), so it is not idle (probably kernel network work shares that core; not verified).
- **I did not measure** the kernel memory cost of a bigger backlog, the latency cost of the sampler, or other stall lengths (the `STALL_MS=5000` mini-matrix was not run).

## Cost

- **What the burst costs a client:** a connection that gets into the queue waits about 1.3 s for the first byte. One that does not connects after about 2.07 s, or after about 3.1 s in the second group, and then has a chance of being stuck until the client timeout (10 s here, then a failed request that the client must retry).
- **What it costs the server:** nothing visible in its own metrics (p99 under 10 ms). The cost is in the kernel counters (up to 7346 overflow events for 1000 connections with backlog 4) and in 1342 failed connections out of 6200.
- **What the fix costs:** a bigger backlog (511 instead of 4) turned 734 of 1000 failures into 0 for the same burst (`A_b4_n1000` vs `A_b511_n1000`), but those 488 connections still paid about 2.09 s, and the loop stall is still there. The memory cost of a larger queue was not measured.

## Takeaways

1. After changing backlog or somaxconn, always read `Send-Q` from `ss -ltn`; the kernel silently uses the smaller of the two.
2. The backlog is a buffer for bursts that arrive while the loop cannot accept. Size it for the largest burst I expect during the longest stall; with 511 one 1.5 s stall absorbed 512 connections without failures.
3. `ListenOverflows` and `ListenDrops` must be in my saturation signals. My server metrics cannot show this problem (9.9 ms vs 8.71 s).
4. Measure with a client and add `connecting` to `http_req_duration`; k6's duration alone hides the connect delay.
5. A dropped SYN costs at least 1 s on this kernel, and retrying clients arrive together, so a small queue overflows again on each retry wave. Clients should retry with jitter, and a stall should be avoided at the source (E0.3 and E0.4).
6. For events shorter than the scrape interval use kernel counters (`nstat` before and after), not Prometheus or a 10 Hz sampler.
7. Before I trust the RST and FIN-WAIT numbers I need a clean re-run with a stricter settle step and three repeats.

## Implications for Stage 1

In Stage 1 a request that waits for Postgres does not block the event loop, so the accept queue should stay empty until the loop itself saturates. When it does, the first symptom will be connect delays and timeouts that the server's own latency metrics do not show. I should record `k6_http_req_connecting_seconds` and the `ListenOverflows` delta in every Stage 1 run and check `Send-Q` of the listening socket.

## Follow-ups

1. A clean re-run with `REPEATS=3` and a settle step that also waits for FIN-WAIT and established leftovers.
2. A probe that captures all packets of the ports in question (not only SYN and RST) to settle claims 8 and 10.
3. The `STALL_MS=5000` mini-matrix for a view Prometheus can resolve.
4. Fix the label of the "Total request time" panel in the dashboard and README.
