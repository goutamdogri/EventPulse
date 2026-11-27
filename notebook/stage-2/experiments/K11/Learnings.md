# K11 Learnings: retention, offsets that fall out of range, and the reset policy

Kafka does not keep records because someone still needs them. It keeps them for a time (or a size) and then deletes them, whether or not any consumer has read them. A consumer group only remembers a number per partition, the committed offset: "I have processed everything below this". If the consumer is away longer than the retention time, the records just above that number can be deleted before it returns. Kafka does not remember that they existed. The consumer then comes back with an offset that points into a hole, and the client has to decide where to continue.

1. A consumer reads a partition and commits its position (the offset) to Kafka after it has processed a chunk.
2. The consumer stops. The group keeps the committed offset.
3. Producers keep writing. The partition grows at the end.
4. The broker deletes old data at the front. The first offset still in the partition (the log start offset) moves up.
5. If the log start passes the committed offset, the committed offset is "out of range".
6. The consumer returns, asks for that offset, and gets an OFFSET_OUT_OF_RANGE answer. The client applies its reset policy: jump to the log start (`earliest`) or to the log end (`latest`).

See also: `notebook/stage-2/experiments/K3/Learnings.md` (consumer groups and partitions), `notebook/stage-2/experiments/K5/Learnings.md` (offsets and commits), `Explanation.md` in this folder for the hypothesis and predictions.

## Concepts

**Retention.** A topic setting (`retention.ms`) that says how old a record may become before it can be deleted. It is about the age of the record (its timestamp), not about whether it was read. The EventPulse default is 24 hours; here it is set to 3 minutes so the effect fits in an experiment.

**Segments.** A partition is stored as a list of files called segments. Kafka deletes whole segments, never single records, and only closed ones. The segment that is still being written (the active segment) is never deleted. A segment is closed ("rolled") when it reaches a size, or when it is older than `segment.ms` and the next record arrives. So if the writes stop, the last segment stays open for ever and its records survive retention. That is why this experiment keeps a small trickle of writes going. A segment can be deleted when the newest record in it is older than the retention time, so records can live longer than `retention.ms` by up to one segment.

**The retention check.** The broker looks for deletable segments only every `log.retention.check.interval.ms`. Here it is 300,000 ms (5 minutes), a broker setting that cannot be set per topic. So data older than retention can still be there for up to 5 more minutes. The point where the data really goes is when the log start offset moves, not when the clock passes the retention time.

**Log start offset and log end offset.** Per partition: the first offset that can still be read and the offset the next record will get. The number of readable records is end minus start.

**Committed offset and lag.** The committed offset is what the group stored. Lag is log end minus committed offset. It counts every record the group has not processed, including records that no longer exist. After the log start passes the committed offset, the lag is larger than the number of records that can still be read. That is a reason lag alone is a poor measure of "how much can I still recover".

**OFFSET_OUT_OF_RANGE and the reset policy.** When the committed offset is outside the range [log start, log end], the fetch fails with this error. The client does not stop: it applies `auto.offset.reset`. With `earliest` it continues at the log start, so it reads everything that is left. With `latest` it continues at the log end, so it skips everything that is left and only reads new records. In EventPulse's consumer-sink this is the setting `CONSUMER_FROM_BEGINNING` (`true` = earliest, `false` = latest). The same setting also decides where a brand new group starts, when there is no committed offset at all.

**The loss in two parts.** After a long outage, the records that were produced but never read split into (a) deleted records, lost with either policy, and (b) records still in the topic, which `earliest` recovers and `latest` skips. `latest` therefore loses more than retention alone.

**Acknowledged does not mean stored.** The API answers 202 when Kafka has acknowledged the event. Whether the event ends up in Postgres depends on a consumer reading it before it is deleted.

## Signals and what they mean

- Log start offset per partition (instantaneous, only ever goes up): steps up when the broker deletes a segment. A flat line means nothing was deleted yet.
- Log end offset per partition (cumulative, goes up with every record): the total written.
- Committed offset per partition (instantaneous): frozen while the consumer is away. It moves again only after the consumer processed a chunk and committed.
- Group lag (instantaneous): end minus committed. Large and still growing during the outage. It does not tell if the data is still there.
- Readable records (end minus start): what can really still be consumed.
- Group members: 0 while the consumer is away. The group and its offsets remain; Kafka keeps the offsets of an empty group for `offsets.retention.minutes` (7 days by default), much longer than a short outage.
- Records consumed and rows inserted (cumulative counters of the consumer): they restart from 0 when the consumer process is replaced, so a rate is the right way to read them.
- Acknowledged events (k6 202 answers, cumulative) against distinct rows in Postgres: the difference is the data that was lost between the API and the database.

## Pitfalls

- Retention is not exact. The segment has to be closed and the broker check has to run, so deletion can come up to about retention + segment time + check interval after the write. Waiting "retention plus a bit" can leave the data in place.
- With no writes after the outage load, the last segments stay active and are not deleted. The trickle stream is there for that reason; without it the experiment would show no loss.
- The lag and the committed offset do not move after a restart with `latest` until a chunk is processed and committed. A flat lag does not mean the consumer is stuck; I have to look at what it consumed.
- Counters of the consumer restart with the new process. Rows and records have to be counted in Postgres and in the topic offsets, not only in the process counters.
- A consumer that stops cleanly does not lose its place. The experiment is about time away, not about a crash (that was K5).
- The group must be the same for both consumers. A new group id would not have an out-of-range offset, only a start policy.
- Offsets are read with Kafka tools just before the restart. Reading them changes nothing, but the timing matters: a broker check that runs between the read and the restart changes the numbers slightly.
