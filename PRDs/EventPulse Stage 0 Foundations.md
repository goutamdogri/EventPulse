# EventPulse Stage 0: Foundations

*Duration: 2-3 weeks. Goal: learn to measure a server honestly before building anything on top of it.*

## 1. Objective and Motivating Pain

Every later stage depends on one skill: producing a trustworthy measurement and explaining it. If your numbers are wrong, every conclusion in Stages 1-8 is wrong.

By the end of Stage 0 you must be able to:

1. Run a load test that does not lie (open-model load, percentiles, no coordinated omission, load generator not the bottleneck).
2. Find the saturation point of a simple server and prove, with tool output, which resource caused it.
3. Explain why a single blocking call in Node stalls everything, and demonstrate the fix.
4. Read the OS-level signals (file descriptors, sockets, CPU, run queue, syscalls) that explain network server behavior.

**There is no EventPulse feature in this stage on purpose.** The product is your measurement method.

## 2. Prerequisites and Setup

| Item | Requirement |
| --- | --- |
| OS | Linux strongly preferred (native, a VM, or a cheap cloud VM). WSL2 works for development but its networking and CPU scheduling differ, so treat its numbers as indicative only |
| Node | Current LTS, TypeScript |
| Tools | k6, wrk2, htop, `ss`, `strace`, `perf` (linux-tools), `pidstat`/`sysstat`, `curl`, `jq` |
| Profilers | `0x` or `clinic flame`, and `node --cpu-prof` |
| Repo | Initialize the monorepo skeleton from the blueprint: `services/`, `loadtests/`, `notebook/`, `docs/` |
| Notebook | Create `notebook/` and commit one file per experiment from day one |

**Two-machine rule:** whenever possible run the load generator on a different machine (or at minimum pin it to different CPU cores with `taskset`) than the server. Record which you used.

## 3. Learning Plan

### Week 1: Machine and network fundamentals

Learn by doing, not just reading. For each topic, run the command and write what you observed.

- **File descriptors:** every socket is an fd. Run `ulimit -n`, `cat /proc/<pid>/limits`, `ls /proc/<pid>/fd | wc -l`.
- **TCP lifecycle:** SYN, SYN-ACK, ACK, data, FIN, TIME\_WAIT. Run `ss -tan` and watch states change under load.
- **Listen backlog and accept queue:** `net.core.somaxconn`, `ss -ltn` (Recv-Q on a listening socket is the accept queue).
- **Ephemeral ports and TIME\_WAIT:** how a client can run out of ports. `sysctl net.ipv4.ip_local_port_range`.
- **epoll:** why one thread can serve thousands of connections. Read the man page, then `strace -f -e trace=epoll_wait,accept4,read,write -p <pid>` on your server.
- **Syscall cost:** `strace -c -f -p <pid>` summarizes where syscall time goes.
- **CPU views:** `htop`, `pidstat -u -p <pid> 1`, `vmstat 1` (run queue `r`, context switches `cs`).

### Week 2: Load testing science

- **Percentiles:** p50, p90, p99, p999 and why averages hide pain. A service with a 5 ms mean and 2 s p999 is broken for 1 in 1000 users.
- **Closed vs open load models.** A closed model (fixed number of virtual users who wait for each response) slows its own request rate when the server slows down, which hides the problem. An open model sends requests on a schedule regardless of responses, like real traffic.
- **Coordinated omission:** when a closed-loop tool stalls waiting on a slow response it stops recording the requests it should have sent during the stall, so the stall barely shows up in the percentiles. wrk2 corrects latency against the intended send schedule.
- **Little's Law:** concurrency = arrival rate x latency. If you push 2000 rps and latency is 50 ms, about 100 requests are in flight. Use it to sanity check results.
- **The knee:** latency stays flat as load rises, then bends sharply upward. The knee is your real capacity, not the peak rps.
- **USE method** for each resource: Utilization, Saturation, Errors.

### Week 3: Node internals and the write-up

- **Event loop phases:** timers, pending callbacks, poll, check, close. Know where `setTimeout`, I/O callbacks, and `setImmediate` run.
- **Why CPU work blocks everything:** one JS thread runs all callbacks. A 200 ms synchronous function means no other request makes progress for 200 ms.
- **libuv thread pool:** used for fs, DNS lookup, some crypto. Default size 4 (`UV_THREADPOOL_SIZE`). It is not a general CPU pool for your JS.
- **Worker threads:** real parallelism for CPU work, with message passing cost.
- **Cluster / multi-process:** one process per core as the basic scaling unit.
- **GC basics:** young and old generation, why large allocations cause pauses (`--trace-gc`).

### Reading (continues through the project)

*Designing Data-Intensive Applications*, Kleppmann. For Stage 0, read **Chapter 1** (reliability, scalability, maintainability, including the load parameters and percentile discussion) carefully, and skim chapters 2-4 for vocabulary you will need in Stages 1-4. Take notes in `notebook/ddia.md`: for each chapter, three things you learned and one thing you do not yet believe.

## 4. What You Build

A small instrumented HTTP server plus a reusable load-testing harness. Neither is throwaway: the harness and conventions carry through all stages.

### Server requirements

| Route | Behavior | Purpose |
| --- | --- | --- |
| `GET /health` | Returns `{ok:true}` immediately | Measures raw framework and network overhead |
| `GET /cpu?n=30` | Synchronous recursive Fibonacci | Demonstrates event-loop blocking |
| `GET /cpu-worker?n=30` | Same work in a worker thread | Demonstrates the fix |
| `GET /io?ms=50` | Awaits a timer | Simulates a slow downstream without CPU use |
| `POST /echo` | Parses JSON body and returns it | Measures JSON cost and payload scaling |
| `GET /stall?ms=2000` | Blocks the loop synchronously for a duration | Demonstrates coordinated omission |
| `GET /debug/stats` | Event-loop delay percentiles, memory, active handles | Internal visibility |

### Starter server (TypeScript, Fastify)

```ts
import Fastify from 'fastify';
import { monitorEventLoopDelay } from 'node:perf_hooks';

const app = Fastify({ logger: false });
const loopDelay = monitorEventLoopDelay({ resolution: 10 });
loopDelay.enable();

const fib = (n: number): number => (n < 2 ? n : fib(n - 1) + fib(n - 2));

app.get('/health', async () => ({ ok: true }));

app.get('/cpu', async (req) => {
  const n = Number((req.query as any).n ?? 30);
  return { result: fib(n) };
});

app.get('/io', async (req) => {
  const ms = Number((req.query as any).ms ?? 50);
  await new Promise((r) => setTimeout(r, ms));
  return { ok: true };
});

app.post('/echo', async (req) => req.body);

app.get('/stall', async (req) => {
  const ms = Number((req.query as any).ms ?? 2000);
  const end = Date.now() + ms;
  while (Date.now() < end) {}
  return { stalled: ms };
});

app.get('/debug/stats', async () => ({
  loopDelayMs: {
    p50: loopDelay.percentile(50) / 1e6,
    p99: loopDelay.percentile(99) / 1e6,
    max: loopDelay.max / 1e6,
  },
  memory: process.memoryUsage(),
}));

app.listen({ port: 3000, host: '0.0.0.0' });
```

You will write `/cpu-worker` yourself using `node:worker_threads`: spawn a small pool at startup, post `{n}` to a worker, and resolve the request when the worker replies. Notice how much more code and how many new failure modes (queueing, worker crash, message copy cost) this adds.

### Load harness requirements

- `loadtests/k6/` with scripts parameterized by route, rate, and duration through environment variables.
- `loadtests/wrk2/` with a shell script that runs wrk2 at stepped rates and writes raw output to a results directory.
- `loadtests/results/` with a naming convention: `YYYYMMDD-HHMM_route_rate_machine.txt`.
- A script that runs a **step test**: for example 500, 1000, 2000, 4000, 8000 rps, 60 seconds each, saving percentiles for every step so you can plot latency vs rate.

### Open-model k6 example

```js
import http from 'k6/http';

export const options = {
  scenarios: {
    steady: {
      executor: 'constant-arrival-rate',
      rate: Number(__ENV.RATE || 1000),
      timeUnit: '1s',
      duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 200,
      maxVUs: 2000,
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],
    http_req_duration: ['p(99)<100'],
  },
};

export default function () {
  http.get(`${__ENV.BASE || 'http://localhost:3000'}${__ENV.ROUTE || '/health'}`);
}
```

Watch the `dropped_iterations` metric. If k6 cannot start requests on schedule, **the load generator is the bottleneck**, not the server. Discard that run.

### wrk2 example

```
wrk -t2 -c100 -d60s -R2000 --latency http://SERVER:3000/health
```

`-R` sets the target rate. Compare its latency distribution with k6 and with a closed-loop tool at the same load.

## 5. Step-by-Step Build Plan

1. **Repo and notebook.** Create the structure, add a README with the one-line goal, create `notebook/_template.md`.
2. **Server v0.** `/health` only. Confirm it responds with `curl`.
3. **Instrument.** Add event-loop delay stats and process memory. Optionally add basic Prometheus-style counters (you will standardize this in Stage 7).
4. **Harness v0.** One k6 script and one wrk2 script working against `/health`.
5. **Baseline.** Run the step test on `/health`. Plot latency percentiles vs rate. Locate the knee.
6. **Add routes.** `/cpu`, `/io`, `/echo`, `/stall`, then `/cpu-worker`.
7. **Run the break-it catalog** (section 7) one experiment at a time, writing the notebook entry before and after.
8. **Profile.** Capture a flame graph at the knee and at saturation; compare them.
9. **Write the benchmark report** (section 10).
10. **Review exit criteria** and tag the repo `stage-0-complete`.

## 6. Test Plan

Stage 0 testing is about validating the measurement setup itself.

| Test | Purpose | Pass condition |
| --- | --- | --- |
| Harness sanity | Known server behavior gives known results | `/io?ms=50` p50 is about 50 ms plus small overhead |
| Load generator headroom | Generator is not the bottleneck | k6 `dropped_iterations` is 0 and generator CPU is under about 70% |
| Repeatability | Same test, same result | Three runs agree within a small variance you define and record |
| Little's Law check | Numbers are internally consistent | In-flight concurrency matches rate x latency within reason |
| Warm-up effect | Cold vs warm difference | Document how first 10 seconds differ (JIT warm-up) and exclude warm-up from results |
| Clock sanity | Timing is reliable | Compare latency from k6, wrk2, and a server-side timer for the same run |
| Unit test | Basic correctness | `/echo` round-trips JSON; `/cpu` returns correct Fibonacci values |

## 7. Break-It Experiment Catalog

For each experiment: write the hypothesis first, run it, record what happened, and explain any difference from your prediction.

### E0.1 Find the knee (baseline)

- **Hypothesis:** `/health` latency stays flat until CPU on one core saturates.
- **How:** step test with rising rate.
- **Observe:** p50/p99 vs rate, server CPU per core (Node uses one core for JS), event-loop delay.
- **Explains:** the real capacity of a single Node process.

### E0.2 Coordinated omission demonstration

- **Hypothesis:** a closed-loop tool will report a far better p99 than wrk2 when the server stalls.
- **How:** run steady load on `/health`, call `/stall?ms=2000` once mid-test, repeat with a closed-loop tool (for example a fixed-VU k6 scenario) and with wrk2.
- **Observe:** the stall barely appears in closed-loop percentiles but dominates wrk2's.
- **Explains:** why you will use open-model load for the rest of the project.

### E0.3 Event-loop blocking

- **Hypothesis:** while `/cpu?n=38` runs, `/health` latency spikes to roughly the duration of the CPU call.
- **How:** hit `/cpu` at low rate from one terminal, `/health` at moderate rate from another.
- **Observe:** `/health` p99, event-loop delay, CPU graph.
- **Explains:** the cost of synchronous work in a single-threaded server.

### E0.4 Worker threads fix

- **Hypothesis:** moving CPU work to workers keeps `/health` flat but throughput of `/cpu-worker` is bounded by worker count and message overhead.
- **How:** repeat E0.3 against `/cpu-worker` with 1, 2, and core-count workers.
- **Observe:** `/health` latency, `/cpu-worker` throughput, queue growth when overloaded.
- **Explains:** isolation vs added complexity, and what happens when the worker queue is unbounded (memory growth).

### E0.5 File descriptor exhaustion

- **Hypothesis:** with a low fd limit the server fails new connections with `EMFILE` once the limit is reached.
- **How:** `ulimit -n 256` before starting the server, open many concurrent connections (k6 with many VUs or a small script holding sockets open).
- **Observe:** errors in server output, `ls /proc/<pid>/fd | wc -l`, client connection failures.
- **Explains:** connection limits and why production services raise `ulimit` and use keep-alive.

### E0.6 Keep-alive vs new connection per request

- **Hypothesis:** disabling keep-alive reduces max rps and creates many TIME\_WAIT sockets.
- **How:** run the same load with and without keep-alive (`--http1.1` connection reuse settings in the tool).
- **Observe:** `ss -s`, count of TIME\_WAIT sockets, p99, CPU split between user and kernel.
- **Explains:** TCP handshake cost, ephemeral port pressure, and why connection pooling matters for every client in later stages.

### E0.7 Load generator as the bottleneck

- **Hypothesis:** running k6 and the server on the same cores lowers both measured capacity and result trustworthiness.
- **How:** run both on one machine unpinned, then pin each with `taskset` to separate cores, then use two machines if available.
- **Observe:** capacity numbers across the three setups, `dropped_iterations`.
- **Explains:** why benchmark methodology matters and why you always check generator headroom.

### E0.8 Accept queue and backlog

- **Hypothesis:** a sudden burst of connections beyond the backlog causes connection resets or timeouts even though average load is low.
- **How:** lower `somaxconn` and the listen backlog, send a burst of new connections.
- **Observe:** `ss -ltn` Recv-Q, client connect errors, `nstat` or `netstat -s` listen overflow counters.
- **Explains:** burst behavior vs steady-state behavior.

### E0.9 Payload size scaling

- **Hypothesis:** `/echo` throughput falls roughly with payload size once JSON parse and serialize dominate.
- **How:** run `/echo` at 100 B, 1 KB, 10 KB, 100 KB bodies.
- **Observe:** rps at the knee per size, CPU profile showing `JSON.parse` and `JSON.stringify` share.
- **Explains:** why serialization format matters later (Stage 3 and Stage 8).

### E0.10 Garbage collection pressure

- **Hypothesis:** allocating large temporary objects per request causes visible GC pauses in p99/p999.
- **How:** add a route that allocates and discards big arrays, run with `node --trace-gc`.
- **Observe:** GC pause timestamps lined up against latency spikes, heap size over time.
- **Explains:** why tail latency often comes from GC rather than your code's average path.

### E0.11 Multi-process scaling

- **Hypothesis:** running one Node process per core increases capacity close to linearly until another resource limits it.
- **How:** run 1, 2, 4, N processes (cluster mode or separate ports behind Nginx) and step-test each.
- **Observe:** rps at the knee per process count, per-core CPU, where scaling bends.
- **Explains:** the first small scaling curve in the project, and an early taste of what limits horizontal scaling.

## 8. Metrics to Capture

For every run record: date, git commit, machine and instance type, kernel and Node versions, command line, target rate, duration, and where the generator ran.

Per step: achieved rps, error rate, p50, p90, p99, p999, max latency, server CPU (total and per core), event-loop delay p99, memory RSS, open fds, socket state counts, generator CPU, `dropped_iterations`.

Plot at minimum: latency percentiles vs offered rate (the knee), CPU vs rate, and event-loop delay vs rate.

## 9. Lab Notebook Template

Create `notebook/_template.md` and copy it for every experiment.

```
# Experiment: <ID and name>
Date:
Commit:

## Hypothesis
What I expect to happen and why (written BEFORE the run).

## Setup
Machine, OS, Node version, server config, generator config, where each ran.

## Method
Exact commands. Rate steps. Duration. Warm-up handling.

## Results
Tables, charts, raw file paths.

## Explanation
Why it happened, supported by tool output (ss, strace, perf, profile).
Where my hypothesis was wrong.

## Follow-ups
What to test next.
```

## 10. Artifact: Benchmark Report

Write `docs/benchmarks/stage-0.md` with these sections:

1. **Setup:** hardware, OS, versions, tools, topology of generator and server.
2. **Method:** open-model load, warm-up, repeat count, how generator headroom was verified.
3. **Baseline:** the `/health` step test, with the latency-vs-rate chart and the identified knee.
4. **Coordinated omission result:** closed-loop vs wrk2 comparison on the same stall.
5. **Event-loop blocking and worker threads:** before/after data and the cost of the fix.
6. **OS limits:** fd exhaustion, keep-alive and TIME\_WAIT, backlog findings.
7. **Payload and GC findings.**
8. **Multi-process scaling curve.**
9. **What I got wrong:** hypotheses that failed and why.
10. **Implications for Stage 1:** what these findings predict about a server that writes to Postgres on each request.

**Optional blog post:** "Why my load test lied to me" (coordinated omission with your own data). This is a strong first public artifact.

## 11. Common Pitfalls

- Testing `localhost` on one machine and trusting the numbers. Loopback hides real network costs and the generator steals CPU.
- Reporting average latency or only the best run.
- Using a closed-loop tool without realizing it throttles itself under pressure.
- Judging capacity by peak rps instead of the knee.
- Forgetting to warm up, so JIT compilation skews the first minute.
- Running with debug logging, `console.log` per request, or a profiler attached while measuring baseline.
- Changing several variables between runs, then not knowing what caused the difference.
- Ignoring the load generator's own CPU and `dropped_iterations`.
- Treating WSL2 or a laptop on battery as a stable benchmark platform (CPU frequency scaling and thermal throttling change results).

## 12. Exit Criteria

Stage 0 is complete only when every item below is true:

- [ ] Server and harness run with one command each, and results are reproducible within recorded variance.
- [ ] You have the latency-vs-rate chart for `/health` and can point to the knee.
- [ ] You can explain, using `ss`, `strace`, a profiler, or CPU graphs, exactly why the server saturates where it does.
- [ ] Experiments E0.1 through E0.11 each have a notebook entry with hypothesis, result, and explanation.
- [ ] You demonstrated coordinated omission with your own numbers.
- [ ] You can explain the event loop, the libuv thread pool, and worker threads without notes.
- [ ] The benchmark report is written and committed.
- [ ] DDIA Chapter 1 notes are written.
- [ ] Repo is tagged `stage-0-complete`.

**Self-test questions (answer aloud, without notes):**

1. Why can a service with a great average latency still be unusable?
2. What is coordinated omission and how does wrk2 correct for it?
3. If I push 5000 rps and p50 latency is 20 ms, roughly how many requests are in flight, and why does that matter?
4. What happens to `/health` latency while a synchronous 300 ms function is running, and why?
5. What resource limits did you hit first on this machine, and how did you prove it?

## 13. Handoff to Stage 1

Stage 1 puts a Postgres write in the request path of `POST /events`. Stage 0 gives you:

- the harness to load it,
- the vocabulary to describe how it fails (knee, saturation, tail latency, queueing),
- the OS-level tools to look inside it,
- and a prediction to test: a handler that awaits a slow downstream behaves like `/io`, so latency will track database latency and in-flight requests will pile up according to Little's Law until a pool or limit is exhausted.

Write that prediction at the end of your Stage 0 report. In Stage 1 you will check whether you were right.

When you are ready, say **Stage 1**.
