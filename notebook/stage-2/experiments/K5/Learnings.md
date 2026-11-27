# K5 Learnings: at-least-once delivery, offsets and idempotent writes

A consumer that reads from Kafka and writes to a database has to do two things that cannot happen in one atomic step: store the result in the database, and tell Kafka "I am done up to here" (commit the offset). Kafka and Postgres do not share a transaction, so whatever order I choose, a crash between the two leaves them disagreeing. The order the consumer-sink uses is the cycle below, and the rest of this note is about what each crash point does to it.

1. The consumer fetches a batch of messages from one partition it owns.
2. It splits the batch into chunks (at most 1000 messages).
3. For one chunk: it parses every message, then writes the good rows in one database transaction, then commits the transaction.
4. Only after the database commit it marks the last offset of the chunk as done and commits that offset to Kafka (the group's committed offset).
5. If the process dies at any point, the group gives the partition to a consumer that starts reading at the last committed offset. Everything after that offset is read again.

See also: `notebook/stage-2/experiments/K3/Learnings.md` (consumer groups, partitions, the rebalance), `notebook/stage-2/experiments/K2/Learnings.md` (the producer side), `Explanation.md` in this folder for the hypothesis and predictions.

## Concepts

**Offset and committed offset.** Every message in a partition has a number, its offset. The committed offset of a group is a bookmark stored in Kafka: "this group has finished everything before this number". It is the only thing a new consumer knows about the old one's progress. Example: committed offset 500 means a replacement starts at offset 500, even if the old consumer had already written the rows of 500 to 620 to the database.

**Delivery guarantees.** At-most-once means a message is handled zero or one times (commit the offset first, then work; a crash loses the work). At-least-once means one or more times (work first, then commit; a crash repeats the work). Exactly-once would need both steps to be one atomic action. The sink chooses at-least-once because losing an event is worse than seeing one twice, and then tries to make seeing it twice harmless.

**Crash points.** Three places where the process can die, each with a different result. Before parse and after parse: nothing is in the database yet and the offset has not moved, so the chunk is simply read again and written once. After the database commit but before the offset commit: the rows are stored and the bookmark is old, so the chunk is read again and written a second time. That last one is the only place where a duplicate can appear.

**SIGKILL.** A signal that the kernel handles by removing the process immediately. The process cannot catch it: no shutdown hook, no flush, no last offset commit, no goodbye to the group. This is the closest software model of a power loss or an out-of-memory kill. A normal stop (SIGTERM) lets the consumer finish its chunk and leave the group, which is why it would not show any of this.

**Idempotent write.** A write that gives the same result if it is done twice. In the events table `event_id` is the primary key and the sink inserts with `ON CONFLICT (event_id) DO NOTHING`: the second attempt to insert the same id is skipped. The duplicate is still counted (attempted minus inserted), so reprocessing is visible and not hidden. This only works because the event id is created once, before the message enters Kafka. Without a key (no primary key, no ON CONFLICT) the same message becomes two rows.

**Session timeout and the dead member.** A consumer group member proves it is alive by sending heartbeats. If the member stops, the broker (the group coordinator) waits for the session timeout before it removes it and gives its partitions to someone else. A replacement that joins earlier has to wait for the rebalance to finish, which cannot finish while the old member is still counted. So after a SIGKILL the time to resume is roughly the session timeout, no matter how fast the new process starts. The default here is 45 s; the experiment lowers it to 10 s.

**Time to resume and catch-up.** Time to resume is the gap with no processing at all (kill until the replacement handles its first chunk). Catch-up is the time after that to work off the backlog that built up (the producer never stopped). Backlog = input rate x gap. Catch-up time = backlog / (consumer speed - input rate).

## Signals and what they mean

- **Consumer lag** (instantaneous): log end offset minus committed offset, per partition. Rises while the consumer is dead, falls during the catch-up, 0 when everything is committed. A lag of 0 means committed, not only stored.
- **Committed offset** (instantaneous, per partition): the bookmark. Flat while nobody consumes; it never goes backwards for a healthy group.
- **Messages consumed** (cumulative): messages of chunks that completed. A chunk that was in flight at the kill is not in it.
- **Rows inserted** (cumulative): rows the database actually accepted (rowCount). **Duplicates** (cumulative): rows the database refused because the key already existed (attempted minus inserted).
- **Assigned partitions and rebalances** (instantaneous / cumulative): when a member owns partitions and how many times the group was re-formed.
- **Freshness** (a histogram): time from the API receiving an event to its first insert. Rows that waited for the outage show it directly.
- Counters of a process that was killed disappear with it, and the replacement starts again from 0. Rate and increase functions hide the reset; plain values do not.

## Pitfalls

- The number of reprocessed rows is not fixed by the crash point; it is the size of the chunk that was in flight, and that depends on how many messages had arrived in the partition when the batch was cut. It has to be read from the log, not assumed.
- A restarted consumer is not instantly productive: it waits for the dead member's session to expire. Starting it faster does not help.
- "Consumed minus unique rows" misses the chunk that was in flight, because the counter only moves when a chunk completes. The in-flight chunk has to be added back.
- The consumer's own counters are lost at the kill, so the final truth has to come from the database and from Kafka's offsets, not from the consumer's metrics.
- A duplicate-free table with ON CONFLICT does not mean nothing was processed twice. It means the second processing changed nothing. Both the "re-read" and the "stored twice" numbers are needed to tell the two apart.
- Producer retries create duplicates in the topic itself. The producer is idempotent (acks=all) in this experiment, so the topic is expected to hold each event once; the record count in the topic is checked against the 202 answers.
