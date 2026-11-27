# K3 Learnings: partitions, consumer groups and the unit of parallelism

A Kafka topic is split into partitions, and a partition is an ordered log that only one consumer of a group reads at a
time. That one rule decides how much parallelism a consumer group can have. In EventPulse the consumer-sink is one member
of the group `sink-postgres`; it reads `events.raw` and writes rows to Postgres. The cycle when several consumers share
a topic is:

1. Consumers start and each one asks the group coordinator (a broker) to join the group.
2. The coordinator waits a moment for more joiners (for a new group), then picks a leader among the members.
3. The leader runs the assignor, which splits the partitions between the members, and the coordinator sends every member its list.
4. Each member fetches only from its own partitions, hands a batch of messages to the handler, and after the database commit it commits the offsets of that partition.
5. When a member joins, leaves or dies, the group rebalances: steps 2 and 3 run again and ownership is shuffled.

See also: `notebook/stage-2/learning/Kafka/listeners.md` (how clients reach the brokers), `notebook/stage-2/experiments/K2/Learnings.md` (the producer side), `Explanation.md` in this folder for the hypothesis and predictions.

## Concepts

**Partition.** One append-only log, with its own offsets, on one leader broker (and followers for replication). Order is
only guaranteed inside one partition. Why it matters here: it is the smallest piece of a topic that can be given to one
consumer, so it is the unit of parallelism. Example: a topic with 6 partitions can be read by at most 6 consumers of a
group at the same time.

**Consumer group.** A set of consumers with the same group id that share the work of a topic. Each partition has exactly
one owner in the group; a consumer can own several partitions. A second group with another id reads the whole topic again
on its own, which is how this experiment reads the same backlog many times (a new group id every run).

**More consumers than partitions.** The extra consumers get an empty assignment. They stay in the group (they heartbeat,
they use memory) and do nothing, until another member leaves and a rebalance gives them a partition. This is the reason
the partition count is a ceiling and not a hint.

**Fewer consumers than partitions.** Every consumer owns about P / N partitions. One consumer here works on one partition
at a time (`CONSUMER_PARTITIONS_CONCURRENTLY=1`), so its throughput is the same as with one partition; the partitions
only take turns. With 6 partitions and 4 consumers the split is 2, 2, 1, 1. The group is finished only when the consumers
with 2 partitions are finished, so the speed is set by the most loaded consumer, not by the average.

**Assignor.** The algorithm that splits partitions. `roundRobin` deals the partitions one by one to the members and gives
a very even split (counts differ by at most 1). `range` splits per topic in contiguous ranges. `cooperativeSticky` tries to
move as few partitions as possible during a rebalance. This experiment uses `roundRobin` only.

**Rebalance.** The group re-does the assignment. With the eager protocol used by `roundRobin` every member first gives up
all its partitions and waits for the new assignment, so the whole group stops reading for a moment. A new group also waits
for a short initial delay (the broker setting `group.initial.rebalance.delay.ms`, default 3 s) to collect joiners before
the first assignment. Starting N consumers one after another causes several rebalances; starting them together causes
fewer. A consumer that is too slow to heartbeat (session timeout 45 s here) is thrown out and causes another one.

**Why the partition count is hard to change.** Adding partitions to an existing topic is possible, but a record's partition
is chosen as hash(key) mod partitions, so after the change the same key goes to a different partition: ordering per key is
broken across the boundary and consumers that keep per-key state see keys move. Removing partitions is not possible at all
without creating a new topic and copying. So the number is chosen at creation, from the highest parallelism wanted
later, and a topic with too few partitions cannot be sped up by adding consumers.

**Lag.** For one partition, the log end offset minus the group's committed offset: messages written but not yet processed
(and committed). The sink commits only after the database commit, so lag shows work that is not safely stored. Total lag
is the sum over partitions. For a fixed backlog it starts at the backlog size and falls to 0.

**What can limit the group besides partitions.** A consumer is a single Node thread, so it can be CPU-bound. All consumers
write to one Postgres, which has its own CPU and I/O limit; when the database is the limit, adding consumers (even if
partitions allow it) adds no throughput. `SINK_PROCESSING_DELAY_MS` adds a fixed wait per batch inside the consumer, which makes the consumer, not the
database, the limit: the rows per second of one consumer is then about batch size / (delay + work time).

## Signals and what they mean

- **Rows inserted (counter, cumulative):** rows really written. Take a difference or a `rate()`. Sum over consumers is the group throughput.
- **Messages consumed (counter):** records of completed chunks, equal to inserted + duplicates + DLQ. It reaches the backlog size when the group is done.
- **Assigned partitions (gauge, per consumer):** how many partitions this member owns right now. Zero means idle. The sum over members equals P while the group is stable.
- **Rebalances (counter, per consumer):** assignments received. A burst at the start is normal; a rise during the drain means a member joined, left or was kicked out.
- **Group lag per partition (gauge):** the same value on every member, so read it with `max by (partition)`. It is polled (every second here), so it steps rather than being smooth.
- **Batch size and chunk processing time:** how many messages one handler call got and how long parse, insert and commit took. The database write time is the part inside Postgres.
- **CPU of the consumer CPUs, Postgres and the brokers:** where the cost lands; a value near the limit of a resource means that resource, not the partition count, is the limit.
- **Event loop lag of a consumer:** how late timers fire in that Node process; high values mean the thread is saturated.

## Pitfalls

- The group is not stable at the start: partitions are assigned a few seconds after the consumers start and rebalances may follow. A number taken over the whole run mixes startup with the steady drain, so the run keeps both the whole-run rate and the rate from the moment the group is stable.
- The same topic is read many times, so the Postgres rows of the previous drain must be removed first (the sink would count them as duplicates and write nothing). Dead rows left by the delete can slow the next insert, hence the vacuum.
- Consumers that share CPUs can compete with each other (and with the brokers in the drain part); a CPU-bound consumer group looks like a partition limit if it is not checked.
- Postgres can be the limit before the consumers are. That is why the delay sweep exists: it moves the limit into the consumer so the partition effect can be seen separately.
- The consumer lag series of a finished run stay in Prometheus and in Grafana for a few minutes; read each run only in its own time window.
- Counters of a consumer start at 0 when the process starts, so every run has fresh counters and nothing needs to be subtracted.
