# K7 Learnings: a queue between the API and the database

The consumer-sink reads records from Kafka and writes them to Postgres. The API and the sink do not talk to each other: the topic sits in between. That means the two sides have two separate rates. The API writes at the rate clients send. The sink writes at the rate Postgres lets it. The queue stores the difference.

1. A client sends an event; the API checks it and appends it to a Kafka partition, and answers 202 once the brokers have it.
2. The sink polls a batch from a partition, writes it to Postgres in one transaction, and only then commits the offset.
3. Records that arrived and are not yet committed by the sink are the lag.
4. If the sink is slower than the arrivals, lag grows. If it is faster, lag shrinks.

See also: [K3 Learnings](../K3/Learnings.md) for partitions and consumers, [K5 Learnings](../K5/Learnings.md) for what the offset commit means. The hypothesis and predictions are in [Explanation.md](Explanation.md).

## Concepts

**Decoupling.** Because the API only needs Kafka to answer, a slow database does not reach the client. In Stage 1 the API wrote to Postgres itself, so a slow database made every request slow. Here the cost moves from latency (seen by the client at once) to lag (seen only by someone looking at the consumer).

**Consumer lag.** Lag of a partition is the log end offset (the next offset that will be written) minus the group's committed offset. The total is the sum over the partitions. It counts records, not time. To turn it into time, divide by the rate the sink works at: lag 50,000 at 2,000 per second is 25 seconds of waiting.

**Capacity of the sink.** One chunk is one transaction: BEGIN, one multi-row INSERT, COMMIT. That is three round trips to Postgres. If every answer is delayed by L, one chunk takes about 3L plus the real insert time. The sink writes one chunk at a time per consumer here, so capacity is about rows per chunk divided by that time. The chunk is limited (1000 rows in this setup), so capacity has a ceiling when Postgres is slow.

**Chunk size adapts.** A chunk is whatever the consumer fetched, up to the limit. When the sink is slow, more records are waiting at the next poll, so chunks get bigger. This raises capacity by itself, up to the limit. So a modest slowdown can be absorbed and the lag settles at a plateau; a bigger slowdown pushes capacity under the arrival rate and the lag grows without bound.

**Growth and recovery.** Shortfall = arrival rate minus sink capacity while Postgres is slow. Lag grows by shortfall times duration. After Postgres recovers the sink works at its healthy capacity, and the lag falls at healthy capacity minus the arrival rate (if the arrivals continue). The closer the healthy capacity is to the arrival rate, the longer the recovery: with no headroom it never recovers.

**Latency injection with a proxy.** Toxiproxy sits between the sink and Postgres and can delay every answer in one direction. It slows existing connections too, so it can be turned on and off in the middle of a run. It delays every answer, so a query that needs several answers pays the delay several times.

**Retention and disk.** A Kafka log keeps records until the retention time or size limit, whether or not the consumer has read them. So the disk usage follows what the producers write, not what the consumer is behind. The risk of a long lag is different: if the consumer is behind by more than the retention, the broker deletes records the consumer has not read, and they are lost for that consumer. Disk usage matters for capacity planning (retention x write rate x replication factor), not for the lag itself.

**Replication factor.** Every record is stored on three brokers here, so the bytes on all brokers together are three times the size of one copy.

## Signals and what they mean

- **Consumer lag (gauge, instantaneous):** records written and not yet committed by the group. Healthy: small and flat. Unhealthy: a steady rise. The sink computes it from Kafka once a second; kafka_exporter gives the same number from outside with a coarser sampling.
- **Rows inserted per second (from a counter, take the rate):** what the sink actually stores. Compare with the arrival rate; the gap is how fast lag changes.
- **Database write time (histogram):** the time of one transaction as the sink sees it. It includes the round trips, so it shows the injected latency directly.
- **Rows per chunk (histogram sum / count):** how much work one transaction does. Near 1000 means the sink is at its limit.
- **Freshness (histogram):** time from the API receiving an event to the row being committed. It is the lag seen as time, and the number a user of the data would feel.
- **Topic bytes per broker (gauge):** size of the partition directories on disk. It only goes down when the broker deletes old segments.
- **Rebalances (counter):** how many times the group was reassigned. A jump in the middle of a run means a member left or was removed, which adds a pause to the lag unrelated to slowness.
- **k6 request time (histogram):** what the client sees. It is cumulative as a histogram, so the quantile is taken over a rate window.

## Pitfalls

- The latency toxic must really be on the path. If the sink connects to Postgres directly, nothing is slowed and the run looks like a calm one. The write time panel is the check.
- A rate window (for example 30 s) smooths a lag change; read the lag gauge itself for the shape and the rate panels for the speed.
- Lag only becomes zero when the load has ended or the sink has headroom. If the load stays on, "recovered" means back under a small threshold, not exactly zero.
- The window labels of the k6 trends are decided by time since the start, not by the toxic itself, so they can be off by about a second at the edges.
- The first seconds of a run are not steady: the consumer joins the group and the first chunks are small. The growth slope skips the first 10 seconds of the slow window for the same reason of settling.
- `du` on the brokers walks segment files while they grow, so the byte count has a small error. It is sampled every 2 seconds and is a trend, not an accounting number.
- Postgres in this setup has no other load, so a real slow database (disk or lock contention) may also slow the readers and writers differently than a pure added latency does. A pure latency toxic is the simplest case.
