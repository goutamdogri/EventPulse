# EventPulse Stage 8: Performance Engineering

*Duration: 3-4 weeks. This is your differentiator. Goal: profile and tune the ingest path, build the same endpoint in Go for an honest comparison, prove how throughput scales from 1 to 8 ingest nodes, find where it stops, and turn the measurements into a cost model.*

## 1. Objective and Motivating Pain

Stage 7 left a platform you can deploy, observe, and break. You still do not know how efficient it is. You cannot say where CPU time goes per request, how much the framework and serialization cost, whether Node is the right tool for the hot path, where horizontal scaling stops, or what a million events costs. Every strong claim on your resume ("N events/sec on 4 nodes, p99 under X ms, linear scaling to 8 nodes, $Y per billion events") comes from this stage.

By the end of this stage you must be able to:

1. Decompose the ingest path's cost with an "overhead ladder" and a flame graph, and explain every layer's share.
2. Make measured optimizations, each with a hypothesis and before/after data, and say which ones mattered and which did not.
3. Compare Node (NestJS on Express, and Fastify) with Go on equal CPU and **equal semantics**, and defend the language choice with data.
4. Chart scaling at 1, 2, 4, and 8 ingest nodes, compute scaling efficiency, and prove which component becomes the limit.
5. Produce a cost per million events and a sourced cost model extrapolating to 1M rps, with sensitivity ranges.

**Predictions to write down first:** per-node ceiling of NestJS on Express with the full ingest semantics; the speedup from Fastify and from Go; what fraction of CPU is JSON, validation, the Redis limiter call, and Kafka produce; which component limits scaling first.

## 2. Prerequisites and Setup

| Item | Requirement |
| --- | --- |
| Stages 0-7 | Complete: reproducible infrastructure, observability, audit tooling, delivery guarantees, benchmark methodology |
| Profilers (Node) | `0x` or `clinic flame`, `clinic doctor`, `node --cpu-prof`, Linux `perf` with `--perf-basic-prof` |
| Profilers (Go) | `net/http/pprof`, `go tool pprof`, `go test -bench` for microbenchmarks |
| Languages | Go toolchain (Rust optional, as a stretch goal) |
| Load generation | k6 plus wrk2 on dedicated generator machines. **First measure the generator's own ceiling** against a trivial server so you know when it stops being trustworthy |
| Cloud shape | Fixed-performance instance types only (avoid burstable or shared-core families, whose CPU credits distort results). Record instance type, region, zone, kernel, and `lscpu` for every run |
| Cost controls | Everything from Stage 7: Terraform apply and destroy, tags, billing alerts, spot instances, a budget plan per session |
| Tracking | `notebook/` entry per experiment, plus a single optimization log table |

### Session budget plan (adjust to your credits)

| Session | Purpose | Footprint | Rough cost |
| --- | --- | --- | --- |
| A | Single-node profiling and the overhead ladder | 1 ingest node, 1 generator, small Kafka | low, a few dollars |
| B | Node vs Go at equal CPU | 2 ingest nodes of the same type, generator, Kafka | low |
| C | Scaling run 1, 2, 4, 8 nodes | up to about 12-15 machines for a few hours | the main burst, roughly $15-40 |
| D | Soak | 1-2 ingest nodes for 2-4 hours | low |

Price every session before running it, and destroy and leak-check immediately after.

## 3. Learning Plan

### Performance methodology

- **Measure, hypothesize, change one thing, measure again.** Keep an optimization log with every change.
- **Queueing intuition:** as utilization approaches 100%, latency explodes roughly like 1/(1 - utilization). The safe operating point is well below the knee, often 60-70% of it.
- **Amdahl's Law:** speeding up part of the path limits total gain by the remainder.
- **Universal Scalability Law:** throughput at N nodes is C(N) = N / (1 + a(N-1) + bN(N-1)), where a models contention (serialization on a shared resource) and b models coherency cost (crosstalk). Fit your 1, 2, 4, 8 data to estimate them.
- **Reading flame graphs:** width is time share. Look for wide frames in your own code, in serialization, in GC, and in the kernel. Distinguish on-CPU from off-CPU time (waiting).
- **Tail latency:** p99 and p999 come from queueing, GC pauses, lock contention, and noisy neighbors, not from the average path.

### Node and V8

- **JIT and GC:** hidden classes, deoptimization, young-generation scavenges vs old-generation mark-compact, `--max-old-space-size`.
- **Cost centers on a request path:** `JSON.parse` and `JSON.stringify`, validation (decorator-based `class-validator` is convenient and relatively heavy; compiled schema validators are faster), object allocation, logging, middleware and DI layers.
- **Framework overhead ladder:** raw `http` server, then Fastify, then Express, then NestJS on Express, then NestJS on Fastify. Know that NestJS can run on either adapter.
- **HTTP:** keep-alive, HTTP/1.1 vs HTTP/2, gRPC and Protobuf for internal paths, compression CPU cost.
- **Multi-core:** one Node process uses one core for JS. Scale with one process per core (cluster, or one pod per core) and measure.
- **Kafka client choice:** `kafkajs` (pure JS, no `linger.ms`, you built a micro-batcher) vs a librdkafka-based client (native batching, compression, lower CPU).

### Go (and Rust as a stretch)

- **Goroutines and the scheduler**, `net/http` vs lower-level HTTP servers (understand the trade-offs in features and safety), JSON encoding cost and faster alternatives, `sync.Pool`, escape analysis and allocation reduction, GC tuning with `GOGC` and `GOMEMLIMIT`, profiling with pprof.
- **Kafka clients for Go:** compare options (a pure Go client and a cgo librdkafka wrapper) rather than assuming.

### Systems and cloud realities

- **vCPU is not a core:** on many x86 instance types a vCPU is a hyperthread, so 4 vCPUs may be 2 physical cores.
- **Network limits:** instances have bandwidth and packets-per-second ceilings, and baseline vs burst allowances. Small events at high rates hit packets-per-second limits before bandwidth.
- **CPU steal and noisy neighbors** on shared hosts; run-to-run variance across days and instances.
- **Linux knobs:** listen backlog, file descriptor limits, ephemeral ports, interrupt and NIC queue distribution. Tune only after a measurement points to them.
- **Cross-AZ traffic:** replication and producer traffic across zones costs money and adds latency; note what your test topology omits.

### Cost modeling

- **Cost per million events** = (hourly cost of all machines involved) / (events per hour at the operating point) x 1,000,000, measured at a utilization you would actually run (not at the knee).
- Include ingest, brokers, ClickHouse, Redis, Postgres, load balancer, storage, and network transfer. At large scale storage and network often dominate compute, so model them explicitly.
- Report ranges and sensitivities (spot vs on-demand, instance family, utilization target, payload size).

### Reading

Brendan Gregg's material on flame graphs and the USE method, and relevant chapters of *Systems Performance*. Gil Tene's talk on how not to measure latency. Neil Gunther's work on the Universal Scalability Law. Kafka and Confluent engineering posts on producer and broker tuning and benchmarking methodology. Node and V8 documentation on profiling and GC, and the Go blog on pprof. In `notebook/perf-notes.md`, explain why a latency curve rises sharply near saturation using a small queueing example.

## 4. Requirements

| ID | Requirement |
| --- | --- |
| S8-F1 | A documented profiling workflow with baseline flame graphs (on-CPU) at 70% of the knee and at saturation, for the Stage 7 ingest service |
| S8-F2 | An overhead ladder measuring single-node rps and CPU per request as layers are added: raw HTTP, plus JSON parse, plus validation, plus auth and rate limit call, plus Kafka produce with `acks=all`, plus the full NestJS stack |
| S8-F3 | An optimization log: each change has a hypothesis, the change, before and after numbers, and a keep or revert decision |
| S8-F4 | A Go ingest service with the same semantics as the Node service, passing the same contract tests and audit |
| S8-F5 | Differential tests proving equivalence between implementations (responses and Kafka message content) |
| S8-F6 | Scaling runs at 1, 2, 4, and 8 ingest nodes for the best Node build and for Go, with the first bottleneck identified at each size |
| S8-F7 | A soak test of 2-4 hours at about 70% of the measured knee with no unexplained degradation |
| S8-F8 | A cost model with cost per million events and a projection to 1M rps, with ranges |
| S8-F9 | A reproducibility package: Terraform, scripts, configs, versions, raw data, and one command per experiment |

### Semantic equivalence checklist (both implementations must match)

| Behavior | Must be identical |
| --- | --- |
| Authentication | Same API key lookup and tenant resolution |
| Validation | Same schema, same error codes and bodies |
| Rate limiting | Same Redis token bucket call per request (or same lease strategy) |
| Idempotency | Same dedup check |
| Serialization | Same Avro schema id and byte-for-byte equivalent messages |
| Durability | Same Kafka settings: `acks=all`, `min.insync.replicas`, idempotent producer |
| Response | 202 only after broker acknowledgment, same headers |
| Limits | Same max payload, same timeouts |

An optimization that changes semantics (for example dropping `acks=all` or skipping validation) is not an optimization; it is a different product. Record it as a trade-off experiment, not a win.

### Targets (hypotheses, replace with data)

| Area | Hypothesis |
| --- | --- |
| NestJS on Express, full semantics | low thousands to roughly ten thousand rps per 4-vCPU node |
| Optimized Node (Fastify adapter, schema validation, native Kafka client) | 1.5x to 3x the baseline |
| Go with equal semantics | a further 2x to 5x, with lower tail latency |
| Scaling efficiency | near-linear to 4 nodes; the break point is wherever your data shows |

These are guesses to be proved wrong or right. The point is the explained delta.

## 5. Design

### Experiment topology (cloud test)

```
[ generator nodes x2-3 ] -> [ Nginx/Envoy LB node ] -> [ ingest nodes x1,2,4,8 ]
                                                         |
                              Kafka x3 (fixed type, fast disk) , Redis, Postgres (small),
                              ClickHouse x1, observability node
All in one zone for the test; record the omission of cross-zone effects.
```

- Ingest nodes use one fixed instance type for every run.
- Kafka, Redis, ClickHouse, and the LB must not be the limit before ingest is, until you deliberately find the limit. Verify each backend's ceiling independently first (Kafka with `kafka-producer-perf-test.sh` at your event size, the LB with a trivial upstream, the generator with a trivial server).
- Place the generators on separate machines, never on the system under test.

### The experiment ladder

| Phase | Question | Output |
| --- | --- | --- |
| A | Where does one node's capacity go? | Overhead ladder, flame graphs |
| B | What does each optimization buy? | Optimization log |
| C | How does Go compare at equal CPU and semantics? | Comparison report |
| D | How does throughput scale with nodes, and what stops it? | Scaling chart, USL fit, bottleneck proof |
| E | Is it stable over hours, and what does it cost? | Soak report, cost model |

### Overhead ladder rungs (single node, fixed CPU)

1. Minimal HTTP handler returning 202 (the framework ceiling).
2. Plus JSON body parse.
3. Plus schema validation.
4. Plus auth and tenant lookup from cache.
5. Plus Redis rate limit call.
6. Plus Avro serialization.
7. Plus Kafka produce with `acks=all` and micro-batching.
8. The full NestJS-on-Express service.

Measure rps at the knee and CPU microseconds per request at each rung. The differences are your cost breakdown, and they tell you what is worth optimizing.

### Go service sketch

Keep the structure parallel to the Node service so the comparison is fair: handler, auth and tenant cache, limiter client, validator, serializer, producer, and the same metrics and health endpoints. Use a bounded worker or batcher for produce, and handle SIGTERM exactly as in Stage 7.

### Repo additions

```
services/ingest-api/ (Node: baseline, optimized variants as branches or flags)
services/ingest-go/ (Go implementation)
bench/ (ladder harness, scenarios, result archives, plotting scripts)
bench/differential/ (equivalence tests)
deploy/terraform/envs/perf/ (burst cluster definition)
docs/benchmarks/stage-8/ (report, flame graphs, optimization log, cost model)
```

## 6. Step-by-Step Build Plan

1. **Verify the measurement setup:** generator ceiling, Kafka ceiling at your event size, LB ceiling.
2. **Session A: baseline and ladder.** Run the overhead ladder and capture flame graphs at 70% of the knee and at saturation.
3. **Optimization loop.** One change at a time: adapter swap, validation, serialization, Kafka client, batching, HTTP settings, GC settings. Update the log after every run.
4. **Multi-core and process model.** Compare one process per core vs fewer processes and per-pod CPU limits.
5. **Build the Go service** and the differential and audit tests; confirm equivalence before any benchmark.
6. **Session B: Node vs Go** at equal CPU and semantics.
7. **Session C: scaling runs** at 1, 2, 4, and 8 ingest nodes for the best Node build and for Go. Find the bottleneck at each size.
8. **Fix or characterize** the first bottleneck (for example Redis limiter call volume, Kafka partitions, LB, network packets per second).
9. **Session D: soak** at about 70% of knee for 2-4 hours.
10. **Cost model:** compute cost per million events from measured data and project 1M rps with ranges.
11. **Write the report and the reproducibility package.**

## 7. Test Plan

| Layer | Tests | Pass condition |
| --- | --- | --- |
| Measurement validity | Generator headroom (no `dropped_iterations`, CPU below about 70%), backend ceilings measured, repeatability | Every published number passes these checks |
| Statistical rigor | At least 3 runs (5 for headline numbers), warm-up excluded, median and range reported | Variance recorded and stated |
| Equivalence | Contract tests, the Stage 3 audit, and differential tests feeding identical requests to Node and Go | Same responses and equivalent Kafka messages |
| Correctness under load | Row-count and dedup audit after every scaling run | Reconciles exactly |
| Optimization safety | Run the Stage 3 correctness suite after each optimization | No guarantee silently weakened |
| Workload realism | Payload size mix, Zipf-distributed tenants, a fraction of duplicates | Documented and used in headline runs |
| Soak | 2-4 hours at 70% of knee | No unexplained drift in p99, memory, or lag |
| Regression | A small nightly benchmark on one node | Alerts on a threshold regression |

## 8. Break-It Experiment Catalog

Write the hypothesis first and keep the raw output of every run.

### P1 The overhead ladder

- **Hypothesis:** the framework and validation layers consume a large share of CPU, and Kafka produce plus the Redis call are significant but not dominant on one node.
- **How:** build each rung on the same machine and measure rps at the knee and CPU per request.
- **Observe:** the delta per rung, and the flame graph share of `JSON.parse`, validation, Nest layers, GC, kernel network time.
- **Explains:** where optimization effort is worth spending.

### P2 Adapter and framework layers

- **Hypothesis:** switching NestJS from Express to Fastify improves throughput noticeably; removing Nest entirely helps further; the gain is bounded by the remaining layers.
- **How:** run the full semantics on NestJS-Express, NestJS-Fastify, raw Fastify.
- **Observe:** capacity, p99, CPU per request, code complexity cost.
- **Explains:** what the framework costs, and a decision about whether the hot path should live outside Nest.

### P3 Validation and serialization

- **Hypothesis:** decorator-based validation and double JSON handling cost measurable CPU; compiled schema validation and a single parse reduce it; Avro or Protobuf on the Kafka path costs CPU but saves bytes and enforces schema.
- **How:** swap validators; skip redundant parse and stringify steps; compare JSON, Avro, and Protobuf on the produce path.
- **Observe:** CPU per request, message size, Kafka bytes in.
- **Explains:** the true cost of correctness features, connecting to Stage 3's tax measurements.

### P4 Kafka client and producer tuning at scale

- **Hypothesis:** a librdkafka-based client cuts CPU versus a pure-JS client; larger batches and compression (lz4 or zstd) raise throughput until broker or network limits appear.
- **How:** sweep batch size, wait time, compression, `acks`, in-flight limits, and partition count with the same load.
- **Observe:** ingest CPU, broker CPU, bytes on the wire, p99, produce latency.
- **Explains:** which Kafka settings matter for the ingest tier and which for brokers.

### P5 HTTP layer

- **Hypothesis:** keep-alive tuning and the LB configuration affect capacity; HTTP/2 or gRPC reduces per-request overhead for clients that multiplex, with a CPU cost in framing and TLS.
- **How:** vary upstream keepalive, connection counts, LB worker settings, HTTP/1.1 vs HTTP/2; with and without TLS termination at the LB.
- **Observe:** capacity, LB CPU, connection counts, latency.
- **Explains:** whether the LB or the connection model is limiting you.

### P6 GC and memory

- **Hypothesis:** allocation-heavy request handling causes GC pauses visible at p99 and p999; heap sizing and reducing allocations improve the tail more than the median.
- **How:** `--trace-gc`, allocation profiling, heap size sweeps; compare the Go service with `GOGC` and `GOMEMLIMIT` variations.
- **Observe:** GC pause distribution vs latency percentiles, RSS.
- **Explains:** tail latency sources.

### P7 Utilization and the latency knee

- **Hypothesis:** p99 rises sharply above roughly 70-80% of the knee, so a sensible operating point is lower than peak.
- **How:** step load finely near the knee on one node.
- **Observe:** p50, p99, p999 vs utilization; where autoscaling thresholds should sit.
- **Explains:** why cost per million events must be computed at the operating point, not at peak.

### P8 vCPU reality, process model, and CPU limits

- **Hypothesis:** doubling vCPUs does not double throughput if they are hyperthreads; one Node process per core scales better than a large single process; Kubernetes CPU limits cause throttling that hurts p99.
- **How:** run 1, 2, 4 processes on a 4-vCPU node; compare pod CPU limits against none; pin processes.
- **Observe:** throughput per vCPU, throttling metrics, tail latency.
- **Explains:** the process model you should deploy.

### P9 Go vs Node at equal CPU and semantics

- **Hypothesis:** Go delivers higher throughput per vCPU and a better tail, with lower memory; the gap narrows when Redis and Kafka calls dominate.
- **How:** identical node type, identical workload, equivalence verified first; compare per-node rps at the same p99 target, memory, and p999.
- **Observe:** capacity, CPU per request, memory, tail, plus an honest note on development effort and maintainability.
- **Explains:** a language decision you can defend either way, including "stay on Node" if the data supports it.

### P10 Scaling 1, 2, 4, 8 nodes

- **Hypothesis:** throughput scales close to linearly until a shared resource (limiter store, brokers, LB, network packets per second, generator) saturates.
- **How:** repeat the same scenario at each size with enough generator capacity; keep backends constant, then scale a backend to confirm the diagnosis.
- **Observe:** total rps, scaling efficiency (rps(N) divided by N times rps(1)), the saturating resource's metrics, a USL fit.
- **Explains:** where and why scaling bends, proven by metrics on the guilty component and by relief when you scale it.

### P11 The limiter as a shared bottleneck

- **Hypothesis:** with a Redis call on every request, the limiter store becomes a bottleneck near some node count; token leasing (Stage 6) removes it at a measured accuracy cost.
- **How:** repeat the scaling run with direct Redis checks and with leasing.
- **Observe:** Redis ops and CPU, ingest capacity, limit accuracy.
- **Explains:** shared state as the enemy of linear scaling.

### P12 Brokers, partitions, and the network

- **Hypothesis:** at larger ingest sizes broker CPU, disk, or the instance network limits (bandwidth or packets per second) cap throughput; more partitions help only up to a point.
- **How:** watch broker and NIC metrics while scaling; vary partition count and broker size.
- **Observe:** per-broker bytes and request rates, NIC saturation signs, producer errors and latency.
- **Explains:** the next ceiling after ingest, and the cost of removing it.

### P13 Cloud variance

- **Hypothesis:** the same benchmark on different days or instances varies by several percent, and burstable or shared-core instances vary much more.
- **How:** repeat a headline run on fresh instances on different days; optionally include one burstable type to see credit exhaustion.
- **Observe:** spread in rps and p99, CPU steal.
- **Explains:** the confidence interval you must attach to every claim.

### P14 Soak

- **Hypothesis:** over hours, memory growth, GC drift, connection leaks, or disk and merge activity degrade tail latency even though short tests look flat.
- **How:** 2-4 hours at about 70% of the knee, sampling continuously.
- **Observe:** p99 over time, RSS, heap, open handles, lag, disk use.
- **Explains:** whether the headline number is sustainable.

### P15 Payload size and mix

- **Hypothesis:** throughput in events per second falls as payload grows and eventually becomes bandwidth-bound rather than CPU-bound.
- **How:** run 200 B, 500 B, 2 KB, 10 KB, and a realistic mix.
- **Observe:** rps, bytes per second, which resource saturates first.
- **Explains:** why 1M rps is meaningless without a stated event size.

### P16 Cost sensitivity

- **Hypothesis:** cost per million events changes more with utilization target and instance family than with micro-optimizations; spot pricing helps ingest but needs interruption handling.
- **How:** recompute the model across instance families, spot vs on-demand, and utilization targets from your measurements.
- **Observe:** cost per million events and projected monthly cost ranges.
- **Explains:** which levers matter economically.

## 9. Metrics and Derived Numbers

Per run record: date, commit, instance types and counts, kernel and runtime versions, event size, tenant mix, offered rate, achieved rps, error rate, p50, p99, p999, ingest CPU, memory, GC time, event loop delay (Node), broker CPU and disk, NIC bytes and packets, Redis ops, generator CPU, `dropped_iterations`.

Derived:

- **rps per vCPU** and **CPU microseconds per request**
- **scaling efficiency** = rps(N) / (N x rps(1))
- **USL parameters** a (contention) and b (coherency) from a fit of your 1, 2, 4, 8 data
- **bytes per event** on the wire and on disk
- **cost per million events** = (total hourly cost of the stack) / (events per hour at the operating point) x 1,000,000

### Illustrative cost arithmetic (not measurements)

Suppose an ingest node costs $0.08 per hour on spot and sustains 14,000 rps at your chosen operating point. Reaching 1,000,000 rps needs about 72 such nodes, which is about $5.76 per hour or roughly $4,200 per month for ingest compute alone. At 500 bytes per event, 1M rps is about 500 MB/s of ingress, and Kafka replication, storage retention, and cross-zone transfer can easily exceed that compute cost. Replace every number with your measured values and cited prices, and state what the model omits.

## 10. Artifacts

1. **Benchmark report** with the overhead ladder, optimization log, before and after flame graphs, Node vs Go comparison, scaling charts for 1, 2, 4, 8 nodes, bottleneck proofs, soak results, and variance statements.
2. **Cost model** (spreadsheet or script) with inputs, formulas, cost per million events, and a projected monthly cost at 1M rps with ranges and sensitivities.
3. **Reproducibility package:** Terraform, configs, versions, scripts, and raw data, so another person can rerun the headline result.
4. **ADRs:** language choice for the hot path, serialization format, Kafka client, LB choice, process model, each with the measurement that justified it.
5. **Resume-ready claim** built only from measured numbers, for example: "Sustained N events/sec on K nodes (p99 under X ms) with at-least-once delivery and idempotent processing; scaling efficiency Y% to 8 nodes; \~$Z per billion events."
6. **Blog posts:** "How I found my p99 latency spike", "Node vs Go for an ingest path: what I measured", "Where my linear scaling stopped."
7. **Demo clip** of the scaling run with dashboards.

## 11. Common Pitfalls

- Benchmarking burstable or shared-core instances and publishing the credit-boosted number.
- A generator that is itself the bottleneck, then blaming the server.
- Comparing Node and Go with unequal semantics (different durability, validation, or limiter behavior).
- Optimizing without a profile, or tuning the wrong layer because it is easiest to change.
- Changing several things at once, then not knowing what helped.
- Using the best single run instead of the median and range.
- Treating vCPUs as cores.
- Running everything in one zone and omitting that fact from claims.
- Reporting peak throughput instead of the throughput at a sensible operating point.
- Forgetting to run the correctness audit after each optimization.
- Skipping the soak test.
- Not saving raw data and scripts, so results cannot be reproduced.
- Overshooting the cloud budget by forgetting teardown.
- Claiming 1M rps as achieved rather than modeled.

## 12. Exit Criteria

- [ ] Measurement validity established: generator and backend ceilings known, repeatability shown.
- [ ] Overhead ladder complete with flame graphs, and an optimization log with keep or revert decisions.
- [ ] Optimized Node build and an equivalent Go service pass the same contract tests, audit, and differential tests.
- [ ] Node vs Go comparison documented with a defended decision.
- [ ] Scaling charts at 1, 2, 4, 8 nodes with scaling efficiency, a USL fit, and the first bottleneck proven with metrics.
- [ ] Soak test of at least 2 hours completed with no unexplained drift.
- [ ] Experiments P1 through P16 have notebook entries.
- [ ] Cost per million events and the 1M rps projection documented with ranges and stated omissions.
- [ ] Reproducibility package published and one result re-run from scratch.
- [ ] Repo tagged `stage-8-complete`.

**Self-test questions (answer aloud, without notes):**

1. Where does a request's CPU time go in your Node ingest path, and which change bought the most?
2. Why is latency so much worse at 90% utilization than at 60%?
3. What made the Node vs Go comparison fair, and what would have made it unfair?
4. At 4 nodes your scaling flattened. Which component was the limit, and what evidence proves it rather than suggests it?
5. How do you compute cost per million events, and why must you not use the peak throughput?
6. Why can you not claim 1M rps, and how do you state what you can claim?

## 13. Handoff to Stage 9

Stage 8 completes the backend story: measured performance, scaling behavior, and cost. What remains is making the system visible to a person:

- There is no product surface showing events, aggregates, and alerts in real time.
- The dashboard must not receive every event; it needs server-side aggregation and throttled pushes.
- You need a demo that survives a spike and a chaos event, and a clean story for a 5-minute video.

Stage 9 is deliberately light (about 2 weeks): a React dashboard over WebSockets or SSE, with throttling and backpressure. Before starting, write down how many updates per second a browser can reasonably render and what your server will do when a client cannot keep up.

When you are ready, say **Stage 9**.
