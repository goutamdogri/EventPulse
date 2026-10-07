# EventPulse Stage 10: Depth Project and Final Wrap-Up

*Duration: 4-6 weeks. Goal: implement ONE small distributed or storage component yourself, prove it correct under faults, and publish the write-up. This is what moves the project from excellent to rare. The stage also closes the whole project: README, blog posts, video, resume line, and interview preparation.*

## 1. Objective and Motivating Pain

Stages 0-9 show that you can design, measure, break, and operate a distributed platform using strong tools. A skeptical interviewer can still ask: "You used Kafka and ClickHouse. Do you understand what is inside them?" Stage 10 answers that question with evidence. You build a core piece yourself, with an adversarial test setup that finds your own bugs.

By the end of this stage you must be able to:

1. Explain your component's design down to its invariants, and say which tests establish each invariant.
2. Show a fault-injection suite (crashes, partitions, delays, corruption) with reproducible failures and the bugs it found in your own code.
3. Publish a write-up that a stranger could learn from and that shows the project is real.
4. Connect the depth project back to EventPulse where there is an honest connection.

**Rule:** pick ONE option, scope it with a written contract, and finish it. A finished smaller component with excellent tests beats three half-built ones.

## 2. Choosing Your Option

| Option | What you build | Signal to an interviewer | Difficulty | Main risk | Fits EventPulse via |
| --- | --- | --- | --- | --- | --- |
| A. Raft replicated log | Leader election, log replication, persistence, a replicated key-value layer, optionally snapshots | Very high: consensus is the canonical hard problem | High | Subtle bugs, time sink on liveness issues | Replicating the shard directory or control-plane config (Stage 6) |
| B. LSM storage engine | Memtable, WAL, SSTables, compaction, crash recovery | High: storage engine internals | Medium-high | Scope creep into performance tuning | A state store for the Stage 4 processor |
| C. Mini windowing engine | Dataflow operators, event-time windows, watermarks, checkpoint barriers, exactly-once with idempotent sinks | High and closest to your project | Medium | Overlap with work already done unless you go deeper | Replacing the custom processor in Stage 4 |
| D. Internals study | Experiments revealing how Kafka or ClickHouse work inside, with write-ups | Medium-high: depth of understanding, less "I built it" | Low-medium | Reads as passive if experiments are shallow | Explaining your own earlier measurements |

**Recommendation for your stated goal:** choose **A (Raft)** if you can commit 5-6 weeks and enjoy hard debugging; it is the strongest differentiator and aligns with your Go work from Stage 8. Choose **C** if you want a safer, well-connected project. Choose **B** if storage internals interest you more. Choose **D** only if time is very short, and make the experiments rigorous. Decide within two days; write the choice and your reasons in `docs/adr/depth-project.md`.

### Scope contract (write before coding)

- What is in scope and, explicitly, what is out.
- The invariants you will test.
- The fault model you will test against.
- The definition of done: components working, tests passing, number of fault schedules run, write-up published.
- A cut line: what you drop first if time runs out.

## 3. Shared Methodology (what makes the result credible)

1. **Specification first.** Write the invariants as plain statements before the code. Optionally write a small formal model (for example in TLA+) of the core protocol and model-check it; treat this as a stretch.
2. **Deterministic simulation.** Run your component with a simulated clock, simulated network, and simulated disk driven by a seeded random generator. Any failure is reproduced by its seed. This is the most valuable testing investment in the whole stage.
3. **Fault injection:** crash and restart at arbitrary points, partition and heal the network (including asymmetric and one-way partitions), drop, delay, reorder, and duplicate messages, lose unsynced disk writes, tear writes, corrupt bytes.
4. **Invariant checking:** after every simulated step or at the end of each run, assert the invariants. Safety properties must always hold; liveness properties hold only when the network is stable long enough.
5. **History checking:** record client operations with start and end times and check the history against a model, for example linearizability for a key-value store. Use an existing checker (for Go, Porcupine) rather than writing your own, and verify what is currently maintained.
6. **Fuzzing and race detection:** randomized operation sequences, `go test -race` or equivalent, and a fuzzer for any parser.
7. **A bug log:** every bug found by tests, with the seed, the root cause, and the fix. This becomes the most interesting part of your write-up.
8. **Benchmarks** after correctness, never before.

**Language:** Go is the natural choice for options A and C (concurrency model, tooling, and Raft learning material); Go or Rust for B. TypeScript is workable for C but weaker for deterministic concurrency testing.

### Suggested six-week shape

| Weeks | Focus |
| --- | --- |
| 1 | Read, write the scope contract and invariants, build the simulation harness first |
| 2-3 | Core implementation with tests added alongside |
| 4 | Fault injection campaign, bug fixing, invariants hardened |
| 5 | Stretch features and benchmarks, optional integration with EventPulse |
| 6 | Write-up, cleanup, final deliverables from the wrap-up section |

## 4. Option A: Raft Replicated Log

### Scope

- Leader election with randomized election timeouts and heartbeats.
- Log replication with the AppendEntries consistency check and fast backtracking of conflicting logs.
- Persistence of current term, vote, and log before replying to RPCs.
- Commit and apply logic, including the rule that a leader commits entries from earlier terms only indirectly.
- A replicated key-value service on top, with client sessions and request ids so retried requests are not applied twice.
- Stretch: log compaction with snapshots (and InstallSnapshot), pre-vote, ReadIndex or lease reads, cluster membership changes.

### Learning resources

The extended Raft paper (study its summary figure closely), Ongaro's dissertation chapters on snapshots, membership changes, and client interaction, the MIT 6.824 (now 6.5840) lab descriptions and course notes, the Raft visualization, a published student guide to implementing Raft, and Jepsen write-ups for how real systems fail. Verify which lab materials are currently public.

### State and rules to implement

| Piece | Notes |
| --- | --- |
| Persistent state | `currentTerm`, `votedFor`, `log[]`, persisted before answering any RPC |
| Volatile state | `commitIndex`, `lastApplied`; on leaders `nextIndex[]`, `matchIndex[]` |
| RequestVote | Grant only if the term is current, the vote is free, and the candidate's log is at least as up to date |
| AppendEntries | Reject if the previous index and term do not match; truncate conflicts; append; advance commit |
| Commit rule | A leader counts replicas only for entries from its own term |
| Timers | Randomized election timeout, shorter heartbeat interval, reset rules matter |
| Applier | Separate loop applying committed entries to the state machine in order |

### Invariants to test

- **Election Safety:** at most one leader per term.
- **Leader Append-Only:** a leader never overwrites or deletes its log entries.
- **Log Matching:** if two logs contain an entry with the same index and term, the logs are identical up to that index.
- **Leader Completeness:** a committed entry is present in the logs of leaders of all later terms.
- **State Machine Safety:** no two servers apply different commands at the same index.
- **Linearizability** of the key-value service as observed by clients (with duplicates handled through sessions).
- **Liveness** (conditional): with a stable majority and network, a leader is elected and commands commit.

### Break-it experiments

- **R1 Leader crash loop:** kill and restart leaders repeatedly under client load. Expect no committed entry lost, and elections that eventually stabilize.
- **R2 Partitions:** 3 vs 2 in a five-node cluster, isolate the leader, asymmetric and flapping partitions. Expect the minority side to make no progress and a stale leader unable to commit.
- **R3 Message chaos:** random delay, reordering, duplication, and loss. Expect safety to hold regardless.
- **R4 Crash during persistence:** simulate crashes between persisting term, vote, and log and replying. Expect no double votes in a term.
- **R5 The "unsafe commit" scenario:** reproduce the situation where an old-term entry replicated on a majority is later overwritten, and show your commit rule prevents the violation. Remove the rule and show the invariant failure.
- **R6 Skipped fsync:** make the simulated disk lose unsynced data on crash and show acknowledged data can disappear when persistence is wrong.
- **R7 Duplicate client requests:** retry a write across a leader change; without sessions it applies twice, with sessions once.
- **R8 Stale reads:** a deposed leader serves a read; show the stale result, then fix with ReadIndex or leases and prove it with the history checker.
- **R9 Disruptive rejoin:** a partitioned node with an inflated term rejoins and forces an election; observe impact and, if you built it, the effect of pre-vote.
- **R10 Slow follower and long divergence:** very long conflicting logs, and snapshot installation for a lagging follower.
- **R11 Performance:** throughput and latency versus batching and fsync policy, versus cluster size, compared against a single-node baseline.

### Optional EventPulse integration

Run the shard directory or control-plane configuration from Stage 6 on your Raft cluster and compare operationally with a Postgres-backed directory. Report honestly what you gained and what you would never do in production by replacing a mature system.

### Deliverables

Source, a design document listing invariants and how each is tested, a simulation harness with seeds, a test report (number of seeds, fault schedules, bugs found), benchmark results, and a blog post such as "Bugs my Raft implementation taught me."

## 5. Option B: LSM Storage Engine

### Scope

- Write path: write-ahead log, in-memory sorted memtable, flush to immutable sorted files (SSTables).
- SSTable format: data blocks, index block, footer, checksums, bloom filter.
- Read path: memtable, immutable memtables, then SSTables from newest to oldest, with bloom filters and a block cache.
- Deletes via tombstones and range iteration with a merging iterator.
- Compaction: size-tiered or leveled (implement one, compare with the other as a stretch).
- Manifest or version set for crash-safe file bookkeeping.
- Recovery: replay the WAL and recover the manifest.
- Stretch: compression, snapshots (MVCC), write batches.

### Learning resources

The LSM-tree concept paper, DDIA Chapter 3, LevelDB and RocksDB design documentation, a database internals text covering storage engines, and an open mini-LSM tutorial if you want a guided path (verify what is currently available).

### Invariants and tests

- **Model-based testing:** random operation sequences (put, get, delete, scan, flush, compact, crash, restart) checked against an in-memory ordered map after every step.
- **Crash consistency:** a fault-injecting filesystem that drops unsynced writes and tears partial writes at every step of flush, manifest update, and compaction install. After recovery, all acknowledged writes are present and no deleted data reappears.
- **Iterator correctness:** merging iterators across levels return sorted, deduplicated, newest-wins results; property tests.
- **Corruption:** flipped bits are detected by checksums.

### Break-it experiments

- **L1:** crash at each step of the write and flush path.
- **L2:** crash during compaction before and after the manifest update.
- **L3:** torn writes in the WAL tail and the SSTable footer.
- **L4:** tombstone accumulation causing slow scans, and how compaction cleans up.
- **L5:** write stalls when compaction cannot keep up.
- **L6:** bloom filter size versus false positive rate versus read cost.
- **L7:** write, read, and space amplification for size-tiered versus leveled compaction under different workloads.
- **L8:** skewed and sequential key patterns; large values.
- **L9:** comparison with an established engine on a mixed workload using the same harness, with honest framing.

### Optional EventPulse integration

Use the engine as the processor state store from Stage 4 and compare checkpoint cost with the Postgres blob approach.

### Deliverables

Source, format specification, crash-consistency test report, amplification charts, benchmark versus a baseline, and a write-up such as "What I learned making a database survive power loss."

## 6. Option C: Mini Stream-Processing Engine

### Scope

- A small dataflow API: source, map, filter, keyBy, windowed aggregate, sink, with parallel tasks over partitions.
- Event-time processing with watermarks and allowed lateness, building on the Stage 4 design.
- A state backend interface with in-memory and persisted implementations, organized by key groups so state can be redistributed on rescaling.
- Checkpointing with barriers injected at sources, aligned at multi-input operators, with state snapshots and offsets committed together.
- Recovery from the last complete checkpoint and replay from sources.
- End-to-end exactly-once effects through idempotent or transactional sinks.
- Backpressure between operators.
- Stretch: session windows, joins, unaligned checkpoints, rescaling.

### Learning resources

The Chandy-Lamport snapshot algorithm, the paper on lightweight asynchronous snapshots for dataflows, the Apache Flink architecture paper, the Dataflow Model paper, and the *Streaming Systems* book. Compare your design with Flink's documentation on checkpoints and state.

### Invariants and tests

- **Reference equivalence:** outputs equal a batch computation over the same input for any permutation of input within the lateness bound.
- **Recovery equivalence:** crash at any point, restart, and the final output equals an uninterrupted run.
- **Watermark monotonicity** and correct handling of idle sources.
- **Checkpoint consistency:** a restored snapshot represents a consistent cut (no event counted twice or lost across operators).
- **Sink idempotency** under replay.
- Deterministic simulation of the whole job with seeded failures.

### Break-it experiments

- **W1:** crash at every checkpoint phase (barrier injected, partially aligned, state written, offsets committed).
- **W2:** barrier alignment under backpressure; measure the stall and compare with unaligned checkpoints if built.
- **W3:** idle source freezing the watermark.
- **W4:** very large state and checkpoint pause time; incremental snapshots as a fix.
- **W5:** late-data policies and corrections.
- **W6:** rescaling a stateful job by redistributing key groups, and the consistent-hashing connection to Stage 6.
- **W7:** skewed keys and partial aggregation.
- **W8:** comparison with your Stage 4 processor and, optionally, Flink on the same workload.

### Deliverables

Source, design document with the checkpoint protocol and consistency argument, simulation report, benchmarks, and a write-up such as "Implementing checkpoint barriers and what they taught me about exactly-once."

## 7. Option D: Internals Study (Kafka or ClickHouse)

### Scope

Pick one system and produce six to eight experiment-backed write-ups, each following the pattern: question, prediction, experiment, observation, explanation, and a link to documentation or source. Read the source for the parts you study and cite files and functions.

### Experiment ideas (Kafka)

- On-disk layout: inspect segments, offset and time indexes with the log dump tool and predict lookup behavior.
- Page cache and zero-copy: drop caches and compare consumer throughput; trace the sendfile path.
- Replication protocol: high watermark, leader epochs, and truncation; reproduce a scenario where data is lost or truncated under weak settings and show how stronger settings prevent it.
- Controller and failover timing in KRaft mode.
- Consumer group coordinator and rebalance protocol versions; cooperative versus eager behavior.
- Log compaction semantics and edge cases.

### Experiment ideas (ClickHouse)

- What a part looks like on disk: columns, marks, granules, primary index.
- The read path: how a query selects granules and what skipping indexes change.
- Merge selection and write amplification under different insert patterns.
- Why many tiny parts hurt, shown with the part log.
- Projections and materialized views at the storage level.

### Deliverables

A series of write-ups with reproducible scripts and your measurements. To avoid reading as passive, include at least two experiments where your prediction was wrong and explain why.

## 8. Break-It Discipline and Reporting (all options)

For each experiment record: hypothesis, seed or procedure, observation, root cause, fix, and the test that now guards it. Report totals honestly: seeds run, schedules explored, bugs found, bugs found late. Do not claim correctness; claim what you tested and what you did not.

## 9. Final Wrap-Up: Closing the Whole Project

### Repository

- A README with a one-paragraph pitch, an architecture diagram, a results summary linking to reports, one-command local setup, and a map of the docs.
- Docs index: PRD, delivery guarantees, SLOs, runbooks, postmortem, benchmark and scaling reports, cost model, ADRs, chaos report, depth project design.
- Tags for each stage and a clean history. No secrets, and a license.
- A fresh-clone test: someone else runs the quick start on a clean machine and it works.

### Blog series (5-8 posts)

| Post | Source stage |
| --- | --- |
| Why my load test lied to me | 0 |
| Why writing to Postgres in the request path stops scaling | 1 |
| What happens when a Kafka broker dies mid-batch | 2 |
| Exactly-once is a lie (and what to build instead) | 3 |
| Why my dashboard went from seconds to milliseconds | 4 |
| I benchmarked four rate limiting algorithms | 5 |
| I implemented consistent hashing and measured the rebalance | 6 |
| How I found my p99 latency spike / Where my linear scaling stopped | 8 |
| Depth project write-up | 10 |

Publish consistently, include graphs and your raw data links, and keep each post focused on one finding.

### Demo video

Record the 5-minute video from the Stage 9 script and attach it to the README.

### Resume and profile

Use only measured numbers. Template: "Built a distributed event pipeline sustaining \[N\] events/sec on \[K\] nodes (p99 under \[X\] ms) with at-least-once delivery and idempotent processing verified under broker failure; linear scaling validated to \[M\] nodes; \~$\[Z\] per billion events. Implemented \[depth component\] and verified it with \[number\] deterministic fault schedules." Add a short bullet on the postmortem and the delivery guarantees document.

### Interview preparation

Build a story bank with one story per stage in the form: what I measured, how it broke, what I changed, what I learned. Practice follow-ups three levels deep. Sample deep-dive questions to rehearse aloud:

1. Where does your design lose or duplicate an event, and how do you prove it does not?
2. Why that partition count and key, and what would you change at 10x scale?
3. Walk me through your window and watermark behavior with late data.
4. How does your rate limiter behave when Redis fails?
5. What was your scaling bottleneck and how did you prove it?
6. How did you compute cost per million events, and what does it omit?
7. Describe a failure you caused and what you changed afterwards.
8. Why Go or Node for the hot path, and what did you measure?
9. How would you shard Postgres, and how did you test migration safety?
10. Explain your depth project's hardest bug.
11. What would break first if you ran in multiple regions?
12. What did you claim that you could not measure, and how did you state it?

Also prepare an honest limitations list: 1M rps is modeled, not measured; tests ran in one region and zone; some components ran on a single machine for cost reasons.

## 10. Common Pitfalls

- Choosing the most impressive option without the time to finish it.
- Writing the implementation first and the test harness later; the simulation harness should come first.
- Declaring success on happy-path tests.
- Non-reproducible failures: always record seeds.
- Optimizing before correctness is established.
- Scope creep into stretch features while core invariants are unproven.
- Publishing nothing: private work cannot be verified by an interviewer.
- Overclaiming correctness or performance in the write-up.
- Neglecting the final deliverables because the depth project absorbed all the time.

## 11. Exit Criteria

**Stage 10:**

- [ ] Scope contract and invariants written before coding.
- [ ] Simulation or fault-injection harness with reproducible seeds.
- [ ] Core component complete and passing invariant checks across a recorded number of seeds.
- [ ] Break-it experiments for the chosen option recorded with root causes and guarding tests.
- [ ] Bug log with at least several real bugs found by your own tests.
- [ ] Public write-up published.
- [ ] Repo tagged `stage-10-complete`.

**Whole project (final definition of done):**

- [ ] Stage tags 0 through 10 exist, with the matching reports.
- [ ] README, diagrams, one-command setup, and fresh-clone test passed.
- [ ] Benchmark report, Node vs Go comparison, scaling proof, and cost model published.
- [ ] Delivery guarantees document, chaos report, SLOs, runbooks, and one postmortem published.
- [ ] 5-8 blog posts published, and the 5-minute video recorded.
- [ ] Resume line and story bank written, using only measured numbers.
- [ ] Mock interviews done at least twice with someone who will push three levels deep.

**Self-test questions (answer aloud, without notes):**

1. What are the invariants of your component, and which test establishes each?
2. Show me a seed that found a real bug. What was the root cause?
3. What fault does your harness not cover, and what could that hide?
4. What would you need to change to run this in production?
5. How does this depth project explain something you observed in an earlier stage?

## 12. After the Project

Once everything above is published, consider: presenting the work at a meetup or on a recorded talk, contributing a small fix or documentation improvement to an upstream project you studied, and writing a short retrospective on what you would build differently. Visibility compounds: the more your measurements, write-ups, and code can be found and verified by others, the more the project does for you.

This is the last stage guide. If you want, I can help you plan the order of the final deliverables around your deadlines, draft the README structure, or run mock interview questions against your story bank.
