# K1 Learnings: what limits the throughput of an ingest API that writes to Kafka

In Stage 1 a request waited for Postgres to commit before the API answered. In Stage 2 the API appends the event to a
Kafka topic and answers when the brokers acknowledge it. The throughput of such a service is the slowest of a chain:
the load generator, the API process, the network hop, the brokers' CPU, and the brokers' disk. This note is about how
to tell which link of the chain is the slowest.

The path of one request:

1. k6 sends `POST /v1/events`.
2. Node parses the HTTP request, checks the API key, validates the body and builds the record (key, value, headers).
3. The record goes into the API's own micro-batcher queue and waits there for its batch to be sent.
4. The batcher hands the batch to the Kafka client (librdkafka), which sends it to the leader of the target partition.
5. The leader appends it to its log, the two followers copy it, and the leader acknowledges once enough replicas have it.
6. The batcher resolves every request of that batch and Node answers `202`.

See also: `notebook/stage-2/learning/Kafka/listeners.md` (why the brokers have three listeners), and
`Explanation.md` in this folder for the hypothesis and predictions.

## Concepts

**Knee and capacity.** In an open-model test the offered rate is fixed by the generator. Below the capacity of the
slowest link every request is handled and latency is flat. At the capacity the queue in front of that link starts to
grow and latency bends upward. That bend is the knee. The throughput measured after the knee is the capacity of the
bottleneck.

**Bottleneck and utilisation.** A resource is the bottleneck when it is the first one to reach about 100% busy as the
rate rises. For a single server the utilisation is rate times service time (rho = lambda x s). A Node main thread that
needs 125 microseconds per request is full at 1 / 0.000125 = 8,000 requests/s. Resources that are far below 100% at
the knee are not the limit, however big their numbers look.

**Little's law.** The number of requests in the system equals the arrival rate times the time each one spends inside
(L = lambda x W). It lets me check a measurement: requests in flight should be about rate times mean latency.

**Event loop.** Node runs all JavaScript on one thread. One request costs CPU on that thread (parsing, validation,
JSON, metrics). Waiting for the network does not cost CPU, but every callback that finishes waiting does. So one
Node process saturates when that thread is 100% busy, even if the machine has idle cores.

**Hyper-threading (SMT) and pinning.** A physical core presents two logical CPUs that share execution units. The
server is pinned to one such pair (0 and 6): the Node main thread runs on one of them, and Kafka client background
threads and libuv helper threads can use the sibling. A busy sibling slows the main thread somewhat, so "2 CPUs" is
less than twice the speed of 1.

**Producer batching.** A Kafka producer does not send one record per network request. It collects records into a
batch and sends the batch. Two settings decide when a batch leaves: how long to wait for more records (linger) and
how large it may grow. Bigger batches mean fewer requests and fewer syscalls, so a higher throughput, but the first
record in a batch waits longer. The API also has its own batcher in front of the client: it flushes after a maximum
wait (5 ms by default), a maximum count (500), or a maximum number of bytes, and allows a limited number of batches
in flight at once (1 by default). At a given rate the batch size is about rate times (wait + send time).

**Acknowledgements and replication.** With `acks=all` the leader answers only after all in-sync replicas have the
record. With replication factor 3 and `min.insync.replicas=2` a write is refused if fewer than 2 replicas are in sync.
This costs one extra round trip to the followers and means each record is written three times in the cluster. So the
cluster's disk and network work is about three times the bytes the producer sends. It is why the raw ceiling is a
measure of the brokers, not of one broker.

**Partitions, keys and leaders.** A topic is split into partitions. Each partition has one leader broker that takes all
writes for it. The record key chooses the partition (the same key always goes to the same partition). If every record
has the same key, all writes go to one leader and the other brokers sit idle, no matter how many partitions exist.
Keying by tenant is good for ordering per tenant and bad for spreading load when there is one tenant. Keying by event id
spreads the load evenly but loses per-tenant ordering.

**Page cache and the log.** A broker appends records to a file. The operating system keeps recent writes in memory
(the page cache) and flushes them to disk later. Kafka relies on this: acknowledged data is in the cache of several
brokers, not necessarily fsynced. Disk write bandwidth therefore shows up in bursts, and a fast NVMe is usually not
the limit for small events.

**Raw ceiling versus end-to-end.** The producer performance tool sends records as fast as it can, with no HTTP, no
validation, no per-request answer. It measures what the brokers can absorb. The gap between that number and the API's
capacity is work that exists only in the API (HTTP, validation, JSON, metrics, batching, the answer path). That gap is
"my code's overhead" in the stage document's words.

**Generator limits.** k6 is also a program that needs CPU. In an open model each planned request needs a free virtual
user (VU). If k6 has no free VU or no CPU, it drops iterations: the offered rate was not really offered. So at high
rates the generator can look like the server's limit.

## Signals and what they mean

- **Latency percentiles (client and server).** p50 is the typical request, p99 the slow tail. The knee shows first in
  p99. Client latency includes everything; server latency starts when the handler starts, so a wait before the handler
  (accept queue, a busy event loop) shows only on the client side.
- **Throughput accepted versus offered.** Accepted is the number of 202 answers per second; it is capped by the
  bottleneck. Offered is what the generator tried to send.
- **Dropped iterations (k6).** Requests planned but not started. A signal about the generator (or about every VU being
  stuck waiting); it must be zero for a clean run.
- **Process CPU (user, system).** Cumulative CPU seconds of the Node process; take the rate. A value near 1.0 means
  the main thread is full. System time is kernel work (sockets, syscalls).
- **Per-CPU busy share.** One minus the idle share of a specific logical CPU, from the host. It shows the pinned
  groups separately: server, brokers, generator, monitoring. Instantaneous (a rate of a cumulative counter).
- **Event loop lag.** How late timers fire. Near zero when healthy; large when the thread is saturated.
- **Batch size and flush reason.** Messages per Kafka send, and why each was sent (time or size). Flushing by time
  means batches are small and the window decides the latency; flushing by size means the cap is reached.
- **Batcher queue wait and queue depth.** Time and number of events waiting for a send slot. Growth means the
  batcher (or Kafka behind it) is slower than arrivals; reaching the queue limit makes the API refuse with 503.
- **Kafka send time and broker round trip.** How long an acknowledged send takes, and the client's own measure of
  request round trip per broker. These are the Kafka-side share of latency. Cumulative histograms: take a rate before
  taking a quantile.
- **Disk write rate.** Bytes per second written to the device; cumulative counter, take a rate.
- **Shedding (503, rejected events).** The API refusing work to protect itself. A step with shedding has reached the
  API's own limit.

## Pitfalls

- The brokers and the generator must not share cores, or "generator headroom" and the broker ceiling are both wrong.
  Pinning is part of the setup, not a detail.
- The raw ceiling tool must not run at the same time as k6 or the API, and it must write to a topic with the same
  shape (partitions, replication, min ISR), or the numbers are not comparable.
- One tenant key sends every record to one partition. Check how the records are spread before reading a throughput
  number.
- A Kafka log grows during a test. Recreating the topic for every run keeps disk and segment state the same, but it
  also means a run never includes the cost of an old, large log.
- Cumulative counters (CPU seconds, bytes written, histogram counts) mean nothing as raw values; only differences or
  rates do.
- Heavy tools (per-thread CPU listings, `docker stats`, iostat) cost CPU themselves. They belong in separate probe
  runs, never in a measured run.
- k6 series stay in Prometheus after a run ends; the last value can look alive for minutes.
- On one machine, all numbers are relative. The brokers, the API and the generator share one CPU package, memory
  bandwidth, caches and thermal budget, so absolute numbers do not transfer to a real cluster.
