# K6 Learnings: poison messages, retries and the dead-letter queue

A consumer reads records in order from a partition, and it moves its committed offset forward only after it has handled a record. That is what makes delivery at-least-once. The same rule has a dark side: if one record can never be handled, the offset can never move past it. A "poison message" is exactly such a record: it fails every time, however often it is tried. The way out is to decide, per failure, whether to wait (the problem is outside the record, for example the database is down), to retry a few times (the problem may be about this one row), or to give up on the record, park it somewhere safe and move on.

1. The consumer fetches a batch from a partition and splits it into chunks.
2. For each chunk it parses the records and writes the rows to Postgres in one transaction.
3. If everything worked, it commits the offset after the chunk and takes the next one.
4. If something throws, the offset is not committed. The client goes back to the last committed offset and hands the same records again.
5. If the cause is the same record every time, steps 2 to 4 repeat forever.

See also: [Explanation.md](Explanation.md) for the hypothesis and predictions, and `notebook/stage-2/s2-f4-poison-before-fix.md` for the first manual observation.

## Concepts

- **Poison message.** A record that makes the handler fail every time. Two families: it cannot be decoded (invalid JSON, an id that is not a UUID, a field longer than the allowed maximum), or it decodes but the database refuses it (for example a NUL character inside a jsonb value). The edge validation in the API does not help, because the record may not come through the API.
- **Blocked partition.** Order inside a partition is a promise, so the consumer cannot skip a record silently. The offset stays on the poison record, and everything behind it in that partition waits. Other partitions are separate streams and continue. With tenant or event keys, a few producers or tenants map to that partition, so their events stop being visible while everyone else is fine. From outside it looks like "one partition is slow".
- **Retry loop versus crash loop.** The consumer process here does not crash. The handler throws and the client re-delivers the batch at once, so the loop is hot: no pause between attempts, the CPU re-parses the same chunk, and the log grows. A crash loop would restart the process each time instead. Either way the offset does not move.
- **Error classes.** The consumer sorts a failure by what is wrong. A record that cannot be parsed is permanent: retrying cannot help. A row that Postgres refuses with a data or integrity error (SQLSTATE class 22 or 23) is permanent for that row. A database that is unreachable or overloaded is transient: it is the same for every record, so dead-lettering would be wrong, and the right action is to back off and pause. A schema or SQL error (class 42) is systemic: every row would fail, so it must not empty the topic into the DLQ.
- **Bounded retries with backoff.** Retrying a bad row a few times is cheap insurance against a one-off cause, but it must end. Backoff means waiting longer between attempts (the cap doubles: base, 2 x base, 4 x base, up to a maximum). With "equal jitter" the wait is a random value between half the cap and the cap, so many consumers do not retry in step. With base 100 ms and 3 retries the longest wait is 100 + 200 + 400 = 700 ms for one record.
- **Bisection to isolate a bad row.** When a chunk write fails with a row-level error, the consumer cannot tell which row is bad. It splits the chunk in halves and writes each half until the bad rows are alone, so all the good rows are still stored and only the bad row is retried.
- **Dead-letter queue (DLQ).** A separate topic that receives the original bytes of the record, its key and its original headers, plus headers that say where it came from (source partition and offset, error class, error code, number of attempts). The record is not lost and a person can inspect it; replaying is a policy decision (Stage 3). The consumer commits the offset only after the DLQ write is acknowledged, otherwise the record could be lost between the two steps.
- **Key to partition.** The producer hashes the record key (murmur2) and takes it modulo the number of partitions. The same key always lands on the same partition, which is how I can aim the poison at a busy partition.
- **Retention.** Kafka deletes old records after the retention time (here 24 h). A partition blocked for longer than that loses the unread events behind the poison for good. The blocked state therefore has a deadline.

## Signals and what they mean

- **Lag per partition** (log end offset minus committed offset): instantaneous. A line that climbs on one partition and stays near zero on the others is the signature of a blocked partition. A lag that rises on every partition is a different problem (a consumer that is too slow).
- **Committed offset per partition**: it rises when the consumer makes progress. A flat line while the log end offset moves is a stalled partition, and it shows exactly where it stopped.
- **Messages consumed**: counts records of completed chunks. In a retry loop the counter can grow while nothing new is stored, because the same records are counted again, so it must be read together with the rows inserted.
- **Failed chunk attempts by reason** (cumulative, take a rate): a steady high rate of `poison_blocked` is the retry loop. It should be near zero when the DLQ is on.
- **Retries and errors by class** (cumulative): the retries of a permanent database error are visible here; a parser error should have none.
- **DLQ records by class** (cumulative): one step per poison record. More than one means a good record was dead-lettered or one was published twice.
- **Rows in Postgres against 202 answers**: the audit. A difference means accepted events that are not visible; with a blocked partition the difference equals the records waiting behind the poison.
- **Consumer CPU**: a hot loop costs CPU for no progress.

## Pitfalls

- **The partition I think I hit may not be the one I hit.** The key decides it, but only if the topic has the same partition count. The DLQ record carries the real source partition, which is the check.
- **The consumer must be able to keep up at the chosen rate.** If the other partitions also lag, the experiment no longer isolates the poison partition.
- **Sampling resolution.** The stall time comes from samples about a second apart, so differences below that are noise.
- **Counters die with the process and restart at 0.** One consumer is used per run and its counters are read before it is stopped.
- **The poison record is not an acknowledged event.** It never went through the API, so it is not part of the 202 count; the audit must not subtract it.
- **A DLQ-off run leaves the partition stuck on purpose.** Every run therefore uses new topics and a new consumer group; reusing them would start the next run from a stuck state.
- **Dead-lettering a database outage would be a loss.** A good test of the policy is that the transient class never reaches the DLQ; K6 only covers the permanent classes.
