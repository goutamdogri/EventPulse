# K4 Learnings: what the durability settings protect, and what a dead broker does

Kafka keeps every partition on several brokers so that one machine dying does not lose data. The price is that a write has to wait for the other copies, and when too many copies are gone the write has to be refused instead of accepted unsafely. K4 kills brokers while the API is taking writes, so I need to know who is in charge of what, and what each setting says about waiting.

How a write is accepted and what happens when a broker dies:

1. The API's producer sends a batch to the **leader** of each partition.
2. The leader appends it to its log. The **followers** fetch from the leader and append the same records.
3. The leader answers "ok" as soon as the producer's `acks` setting is satisfied (see below).
4. If a broker dies, the **controller** notices that the broker stopped sending heartbeats, marks it fenced, takes it out of the ISR of its partitions, and picks a new leader for the partitions it led.
5. The producer learns about the new leader from a metadata refresh and sends the records again.
6. When the broker comes back it catches up from the leaders and joins the ISR again.

See also: `notebook/stage-2/learning/Kafka/` (listeners and the cluster layout), and [Explanation.md](Explanation.md) for the hypothesis and the predictions.

## Concepts

**Replication factor (RF).** The number of copies of a partition. Here RF = 3 on 3 brokers, so every broker holds a copy of every partition.

**Leader and follower.** One copy of a partition is the leader: all produce and consume requests go to it. The others only copy it. With 6 partitions on 3 brokers I expect each broker to lead about 2 partitions (not verified; the run records it).

**ISR (in-sync replicas).** The set of copies that are keeping up with the leader. The leader is always in it. A follower that falls behind (or dies) is removed, and it is added again after it has caught up. The ISR is not decided by the leader alone: a change has to be accepted by the controller.

**`min.insync.replicas` (min ISR).** The smallest ISR size for which the leader accepts a write that was sent with `acks=all`. Here it is 2. If the ISR has 1 member, an `acks=all` write is refused with a "not enough replicas" error. It does nothing for `acks=1`.

**`acks`.** What the producer waits for before it counts a write as done.
- `acks=1`: the leader has appended it (to its own log, not necessarily to disk, and not yet copied).
- `acks=all` (`-1`): every member of the current ISR has it, and the ISR must have at least min ISR members.
- `acks=0`: nothing (not used in K4).
The consequence for loss: with `acks=1` the leader can answer "ok" and die before any follower copied the record; the new leader does not have it and it is gone. With `acks=all` and min ISR 2, an acknowledged record exists on at least 2 brokers, so losing one broker cannot lose it. Losing two brokers can, if those two are the only copies; that is why the cluster must refuse writes when the ISR is smaller than 2.

**Idempotent producer.** The producer numbers its records per partition so that a retry of the same batch is not written twice. Kafka only allows it with `acks=all`. That is why the `acks1` variant also turns it off, and why a retry with `acks=1` can create a duplicate (the audit counts distinct event ids, so duplicates do not show).

**Unclean leader election.** Electing a leader that is not in the ISR (it may miss records). It is off here, so a partition with no live ISR member has no leader and is unavailable rather than silently wrong. Availability is traded for correctness.

**Controller and KRaft quorum.** In KRaft mode the cluster metadata (who leads what, who is in the ISR) is a replicated log kept by controllers, and a majority of controllers has to be alive to change it. Here all 3 brokers are also controllers, so a majority is 2 of 3. Killing one broker keeps the majority. Killing two breaks it: nobody can elect a leader or change an ISR until a second broker is back. This is a different failure from "the ISR is too small", and I expect it to look different.

**Broker failure detection.** A broker sends heartbeats to the controller. If they stop for `broker.session.timeout.ms` (I believe the default is 9 s; I should check the broker log), the controller fences the broker. So after a `docker kill` there is a delay before the leaders of that broker are replaced. `docker kill` sends SIGKILL, so the broker gets no chance to hand over its leaders (a normal stop would do a controlled shutdown).

**The API's own protection.** The ingest-api has a background health check (every 2 s) that reads the topic's metadata and calls Kafka unhealthy if a partition has no leader or an ISR smaller than min ISR (2 for `acks=all`, 1 otherwise). An unhealthy check opens the circuit breaker, and the API answers 503 immediately instead of waiting for a send timeout (5 s). So the 503s of this experiment have two sources: fast ones from the breaker and slow ones from a send timeout.

## Signals and what they mean

- **Leader per partition** (instantaneous). A change is an election. A value of -1 means no leader.
- **ISR size per partition** (instantaneous). Starts at 3. A fall is a shrink, a rise is a follower catching up. Under min ISR (2) means writes with `acks=all` are refused on that partition.
- **Under-replicated partitions** (instantaneous). ISR smaller than the replica list; it stays above 0 for as long as a broker is down.
- **Brokers seen** (instantaneous). How many brokers are in the cluster metadata the exporter reads.
- **Answers by status** (cumulative counter, take a rate). 202 accepted, 503 refused. Status 0 means no answer before k6's timeout.
- **Health check, breaker, producer connected** (instantaneous 0/1/2). The API's view of Kafka.
- **Send errors by type** (cumulative). The name of what Kafka or the client said: a timeout, not enough replicas, no leader.
- **Audit**: the number of 202 answers against the number of distinct event ids found in Postgres afterwards. 202 but missing = an acknowledged event was lost. Present but answered 503 = the write succeeded although the client was told it failed (it will retry; with a fresh id that is a duplicate in business terms).

## Pitfalls

- The kills must not hit the active controller, or an election of the controller is mixed into the numbers. The script picks two brokers that are not the controller and records which one was.
- Killing 2 of 3 brokers also loses the controller quorum, so "two down" is a quorum loss and not only a small ISR.
- The ISR panel is read from the exporter, which needs a live broker to answer; during the quorum loss it may show a gap or a stale value.
- The segment edges in the k6 counts are the planned times; the real kill is within about a second of them.
- With 2000 req/s and a 5 s send timeout up to about 10,000 requests can wait at once, so the load generator needs many virtual users. Dropped iterations during the two-down segment are then part of the result, but dropped iterations in the steady segment make a run invalid.
- Postgres must be empty of the run's rows before the audit; the script deletes them by run id.
- A trial is only plumbing and is never quoted.
