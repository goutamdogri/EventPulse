# K9 Learnings: record keys, partitions and hot partitions

Kafka decides which partition a record goes to from its key. The producer hashes the key and takes the result modulo the number of partitions, so the same key always lands in the same partition. A partition is an ordered log and only one consumer of a group reads it at a time. Putting those two facts together gives the trade-off this experiment is about: the key can give me order or it can give me an even spread, and I only get both when the keys happen to be spread evenly. In EventPulse the ingest-api puts `tenant_id` or `event_id` on each record, and the consumer-sink group `sink-postgres` reads the topic and writes rows to Postgres.

How a record travels:

1. The API builds the record and chooses the key (`tenant_id` or `event_id`, set by `KAFKA_KEY_MODE`).
2. The producer's partitioner hashes the key, modulo the partition count, and appends to that partition.
3. The group coordinator has given each partition to exactly one consumer of the group.
4. That consumer reads the partition in offset order, writes the chunk to Postgres, then commits the offset.

See also: `../K3/Learnings.md` for partitions as the unit of parallelism and `../K8/Learnings.md` for group stability. The hypothesis and the predictions are in `Explanation.md`.

## Concepts

**Record key and partitioner.** The key is a few bytes the producer attaches to a record. The hash of the key picks the partition, so equal keys share a partition. A different key never means "a different partition for sure": two keys can hash to the same partition (a collision). With 6 partitions and 8 tenants, collisions are normal and which tenant meets which partition depends only on the hash.

**Ordering guarantee.** Kafka orders records only inside one partition. If all events of a tenant have the same key, they sit in one partition and are read in the order they were written. If the events of a tenant are spread over several partitions, no single consumer sees them in one order, and the partitions are read at different speeds and by different consumers. Order across partitions does not exist.

**Hot key and hot partition.** Keys are rarely equally busy. If one key sends 80 % of the traffic, the partition it hashes to gets about 80 % of the records, and the other five partitions share the rest. Adding partitions or consumers does not help, because that one key still goes to one partition and one consumer.

**Per-partition throughput ceiling.** A partition is read by one consumer thread at a time, so its speed is limited by how fast that consumer can process one chunk and commit it. In the lab I set a ceiling on purpose with an artificial delay per chunk: a chunk of N rows that takes `delay + insert time` gives about `N / (delay + insert time)` events per second for one partition. When the rate into the partition is above this, the difference piles up as lag.

**Lag as a queue.** Lag is the log end offset minus the committed offset, so the number of records written but not yet processed. It grows at `input rate - processing rate` while the input is above the ceiling, and it drains at `ceiling - input rate` when the input drops. A growing lag only on the hot partition, with the others near zero, is the typical picture of a hot key. The API does not slow down: it only waits for Kafka to accept the record, so the damage is on the consumer side and shows as delay between receive and store (freshness).

**Skew versus total capacity.** Total capacity of a group is the sum of the ceilings of the partitions that have work, not the number of consumers times a speed. With one hot partition the group behaves like one consumer, however many exist. Spreading by `event_id` turns the topic into a balanced one and total capacity comes back, at the price of the per-tenant order.

**Ordering and what it is needed for.** Per-tenant order matters when later events depend on earlier ones for the same tenant (for example an update after a create). A store that applies events by timestamp or by idempotent upsert can accept out-of-order delivery, one that applies them in arrival order cannot. Stage 6 handles hot partitions explicitly because of this: the options are to key on something finer than the tenant (tenant plus a bucket), to isolate big tenants on their own partitions, or to accept weaker ordering.

## Signals and what they mean

- **Log end offset per partition (producer side).** Counts every record ever written to the partition. Cumulative, so I look at its rate. The rates of the six partitions show the skew directly.
- **Committed offset per partition (consumer side).** How far the group has finished. Cumulative; its rate is the consumption speed of that partition. A flat top at a fixed height while the written rate is higher is the per-partition ceiling.
- **Lag per partition.** Instantaneous gauge, the difference of the two above. One line climbing and five at zero means a hot partition. Two pollers measure it (the sink itself and the kafka exporter) with different intervals, so they can disagree for a few seconds.
- **Bytes per partition.** The size of the partition log on the brokers (the three replicas are the same). It tells how much data the skew moved, not how fast. With a fixed payload it follows the record count.
- **Event freshness.** Time from the API receiving an event to the sink storing it. Instantaneous per event, a histogram over time. It grows when records wait in a lagging partition. It is not split per partition.
- **Overtaken events.** For one tenant, the events sorted by when the API received them, compared with the time their rows were inserted. An event stored before an older one of the same tenant was reordered on the way. It counts reordering between inserts; events inside one insert statement are never counted.

## Pitfalls

- **The hash decides collisions.** Another tenant may hash to the hot partition and add to it. The measured share of the busiest partition is the truth, not 80 %.
- **The artificial delay is the ceiling, not the real hardware.** The ceiling I find belongs to this setup (chunk size, delay, insert time). The experiment is about the mechanism, not about this exact number.
- **A shared CPU can hide the key effect.** Two consumers share one CPU in this lab. If the sink CPUs are busy, a ceiling in event mode comes from the CPU and the run does not answer the question about the key.
- **Rebalances contaminate lag.** A consumer losing partitions in the middle of a run stops its partitions for a while, which also looks like lag. The owned-partition and rebalance panels tell.
- **The tenant comes from the API key.** The body cannot choose the tenant, so a skewed load needs several keys.
- **Stale k6 series.** k6 series are not marked stale after a run; the last value can stay in Grafana for minutes.
- **Probing disturbs a run.** Capturing lag and bytes is done right after the load, not during it, so the measurement does not slow the consumers.
