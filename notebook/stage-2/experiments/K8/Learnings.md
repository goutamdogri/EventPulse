# K8 Learnings: consumer groups, rebalances and the timers behind them

A consumer group is a set of consumers that share the partitions of a topic: every partition is read by exactly one member at a time. Membership changes (a member starts, leaves or dies) and "a member is too slow" both force the group to redistribute the partitions. That redistribution is called a rebalance. While it runs, some or all of the members cannot read, and rows that were being processed can be processed again by the next owner.

How a rebalance goes (eager protocol):

1. Something changes the group: a new member sends a join request, a member leaves, or the coordinator decides a member is dead.
2. The coordinator (one broker per group) tells the members, usually in the answer to their next heartbeat.
3. Every member stops fetching, finishes or abandons what it is doing, gives up all its partitions (revoke) and sends a new join request.
4. When all members have joined, the coordinator picks a leader, the leader computes the new assignment, and the coordinator sends each member its partitions (sync).
5. Members start reading again from the last committed offset of each partition they now own.

See also: `notebook/stage-2/experiments/K3/Learnings.md` (partitions and consumers), `notebook/stage-2/experiments/K5/Learnings.md` (offsets, at-least-once, idempotent writes), `notebook/stage-2/learning/Kafka`, and `Explanation.md` in this folder for the hypothesis and predictions.

## Concepts

**Group coordinator.** One broker keeps the member list and the generation number of a group. It is the one that starts a rebalance and answers join and sync requests. A member that talks with an old generation number is rejected, which is how a removed member finds out it was removed.

**Heartbeat.** A small request every `heartbeat.interval.ms` that says "I am alive". It also carries the news that a rebalance has started, so the heartbeat interval is the delay before a member learns about a rebalance. Rule of thumb: at most a third of the session timeout, so a few lost heartbeats do not kill a healthy member. In this client the heartbeat is sent by a background thread of the Kafka library, so it keeps going while my own code is busy.

**Session timeout.** If the coordinator hears no heartbeat for `session.timeout.ms` it declares the member dead and starts a rebalance. This is the upper bound on how long a crashed or frozen member keeps its partitions. Small value: quick recovery, but a pause in the process (a long garbage collection, a frozen VM) can get a healthy member kicked out. Large value: a dead member holds its partitions for a long time (the default here is 45 s).

**Max poll interval.** The maximum time between two polls of the consumer. A member that does not poll within this time is considered stuck: the library leaves the group on its behalf, even though heartbeats are still arriving. It exists because a member can be alive (heartbeats) and still make no progress. In this project the setting is called the rebalance timeout (`CONSUMER_REBALANCE_TIMEOUT_MS`) and the code comment says that with this client it also limits how long one batch may be processed. Whether processing really counts against it in this client is something I have to see, not assume.

**Batch size and processing time.** One batch has to be fully processed before the next poll. So the time needed for the biggest batch (rows per batch times time per row, plus the database write and the commit) has to stay clearly below the max poll interval. Bigger batches are efficient but make this limit easier to break. Example: 1000 rows at 1 ms each is 1 s of work; at 20 ms each it is 20 s, which breaks an 8 s limit.

**Eager rebalance.** The protocol above: every member revokes everything, then everybody joins again, then everything is assigned again. Simple, but the whole group stops even when only one partition has to move. The `roundRobin` and `range` assignors work this way.

**Cooperative (incremental) rebalance.** The members keep the partitions that stay with them and only revoke the ones that must move. This takes two rounds: first the partitions to be moved are revoked, then they are assigned to their new owner. The `cooperativeSticky` assignor does this, and "sticky" means it tries to leave each partition where it is. The price is more rounds; the gain is that members whose partitions do not move keep reading.

**Offset commit and duplicates.** A consumer commits the offset after it has written the rows. If the commit does not happen (the member was removed, the process froze, the commit was rejected), the next owner starts at the old offset and reads those messages again. Reading twice is not a bug of the protocol, it is what at-least-once delivery means; whether the duplicate shows up in the database depends on the write being idempotent (see K5). The number of possible duplicates is bounded by the amount of work done since the last commit, that is at most one chunk per partition in flight.

**Graceful leave versus silence.** A member that shuts down cleanly sends a leave request and the group rebalances at once. A member that crashes or freezes says nothing, so the group waits for the session timeout first. The two cost very different amounts of time.

**Zombie.** A member the group has already removed but that is still running (for example a frozen process that wakes up). It may finish work it started before it was removed and try to commit it, which the coordinator rejects. Its in-flight work is the classic source of duplicates.

## Signals and what they mean

- **Members of the group** (instantaneous, from the broker side): changes when someone joins, leaves, or is removed. A member that disappears only after the session timeout is a silent failure; one that disappears at once left on purpose.
- **Partitions owned per consumer** (instantaneous): a settled group owns all partitions in total. A member that drops to 0 during a rebalance is the visible cost of the eager protocol.
- **Revoke callbacks and partitions revoked** (cumulative, take a difference): how often and how much a member lost. Eager protocols revoke everything the member had, cooperative ones only what moves.
- **Assign callbacks** (cumulative): how many times a member received partitions. It includes the first join.
- **Revoke to assign gap** (per member): time between giving partitions up and getting partitions again. It only covers the part of the pause the member itself can see; the time before the revoke (waiting for the heartbeat, for the other members to rejoin) is outside it.
- **Rows read per second, group lag, freshness** (rates and levels): the effect a user sees. A pause is a drop of the read rate to 0 while the lag grows; the recovery is the lag falling again.
- **Commit errors** (cumulative): commits the coordinator rejected, usually because the member no longer owned the partition. Each one is a place where duplicates can start.
- **Rows stored twice** (audit result, not a live metric): rows minus distinct event ids in a table that has no primary key. It is reprocessing made visible.

## Pitfalls

- With `SINK_DEDUP=false` the duplicates are stored and visible. With the normal table the same reprocessing happens but `ON CONFLICT` hides it, so a clean-looking table does not mean there was no rebalance.
- The group lag is read from the broker every few seconds, so a short pause looks smaller on the lag panel than in the rows-read panel.
- A member that is frozen with a signal cannot answer its own metrics endpoint, so its panels have a hole; its counters can only be read once it is continued.
- The consumer metrics live in the process: a stopped consumer takes its counters with it. Anything needed afterwards must be copied before the stop.
- If the consumer CPUs are saturated, processing gets slower and can look like a timeout problem. The CPU panels are there to rule that out.
- A rebalance caused by a late poll and one caused by a silent member look similar in the member count but have different causes and timers; do not mix them in one explanation.
- The cooperative protocol needs all members to use cooperative assignors; mixing eager and cooperative members in one group is a different experiment.
