# K2 Learnings: the producer settings triangle

K2 is about what a Kafka producer does between "my code has a message" and "the broker says it is safe". Three things
pull against each other: throughput (messages per second per CPU), latency (how long one message waits) and durability
(what is guaranteed when the answer comes back). The producer's settings are the dials. In EventPulse there are two
batching layers in series, and the cycle of one message is:

1. An HTTP request arrives and the API validates it and puts the record into the micro-batcher queue.
2. The batcher sends the queue as one batch when it is full (message count or byte cap) or when the oldest message has waited `BATCH_MAX_WAIT_MS`. Only `BATCH_MAX_IN_FLIGHT` batches may be on their way at once.
3. The batch goes to the Kafka client (librdkafka), which may hold it a little longer (`linger.ms`), compresses it, and sends one produce request per partition leader.
4. The leader appends it to its log. Depending on `acks` it answers at once, after its own write, or after the in-sync followers have copied it.
5. The client resolves the send, the API answers 202.

See also: `notebook/stage-2/experiments/K1/Learnings.md` (the batcher, the knee), `Explanation.md` in this folder for the hypothesis and predictions.

## Concepts

**Batching.** One produce request has a fixed cost (a syscall, a round trip, a promise, some bookkeeping) and a cost per
byte. Putting more messages into one request spreads the fixed part over more messages, so CPU per message falls. The
price is that the first message of the batch waits for the others. With messages arriving at rate R and a wait window W,
a batch holds about R x W messages (arrivals are random, so this is an average). Example: 4,000 msg/s x 5 ms = 20 messages.

**Wait versus size cap.** A batch is sent at whichever limit is hit first. The size cap (`BATCH_MAX_MESSAGES`) only
matters if it is lower than R x W; at a rate where 5 ms collects 20 messages, a cap of 500 and a cap of 5000 give the
same batches. A byte cap also exists (`BATCH_MAX_BYTES`, hard limit 1,000,000 because of the broker's `message.max.bytes`),
so "5000 messages" of 500 B can never fit in one batch anyway.

**In-flight limit.** With one batch in flight, the next batch cannot go out until the previous send is acknowledged. While
it waits, the queue keeps growing, so batches get bigger on their own when the broker is slow. This couples batch size to
ack time: a slower `acks` setting produces bigger batches even with the same wait.

**Two batching layers.** The micro-batcher (the API's own) feeds librdkafka, which has its own batching (`linger.ms`,
`batch.size`). In this experiment `linger.ms` is 0 for every run, so librdkafka sends what it is given right away and the
API's batcher decides the batch size.

**acks.** `acks=0`: the client does not wait for any answer; a send "succeeds" when the bytes are handed to the socket
(a lost message is not noticed). `acks=1`: the partition leader answers after writing to its own log; if it dies before
the followers copy the record, the record is lost. `acks=all` (-1): the leader answers when every in-sync replica has the
record (the topic has `min.insync.replicas=2`, so at least 2 copies exist). Each step adds a wait, but also removes a way
to lose data. This is the durability dial.

**Idempotent producer.** The client gets a producer id and numbers every batch per partition. The broker drops a batch it
has already stored, so a retry after a lost answer does not create a duplicate. It requires `acks=all` (the API refuses
the combination idempotence + acks 0/1) and allows at most 5 requests in flight per connection. It costs a little
bookkeeping, not a wait.

**Compression.** The client compresses a whole batch (not single messages) and the broker stores it as it arrived, so the
saving is on the network, the broker disk and the replication traffic. Costs CPU in the client (and decompression in the
consumer later). The ratio depends on the data and on the batch size: more messages in a batch means more repeated
JSON keys to match. The codecs differ: `gzip` (deflate, strong, slowest), `snappy` and `lz4` (fast, they only find
repeated byte sequences, no entropy coding), `zstd` (strong and fast). Random base64 text cannot be matched against
anything, only entropy-coded: 6 bits of information in every 8-bit character, so the best case is 75% of the size for
that part. A filler like `xxxx...` compresses about 50:1 and says nothing about real data.

**Throughput at a fixed offered rate.** K2 offers one rate below the K1 knee, so every healthy config accepts the same
4,000 requests per second. A "throughput gain" then does not show in the accepted rate. It shows as CPU spent per request
(its inverse is the capacity of the one Node thread) and as bytes per request.

## Signals and what they mean

- **Average batch size** (messages per send, from a histogram sum and count): follows rate x wait, capped by the size limit. Cumulative counters, so take a difference over a window.
- **Flush reason** (counter by reason): time means the wait expired, size/bytes means a cap was hit. Shows which dial is actually binding.
- **Queue wait** (histogram): time from enqueue to the batch being sent; the direct cost of the wait setting.
- **Send time** (histogram): the ack latency of one batch; grows with `acks` strength.
- **Wire bytes** (cumulative counter from the client's own statistics, after compression, updated every few seconds): divide by messages for bytes per message; this is the compression ratio plus framing.
- **Log bytes on the broker**: the stored size of the topic; compressed batches are stored compressed. With RF 3 on 3 brokers every broker holds a full copy.
- **Process CPU user/system**: cumulative CPU seconds of the whole process, including the librdkafka threads that do compression; divided by requests it gives CPU per request.
- **Broker core busy share**: the only broker CPU view here (no broker exporter); it includes whatever else shares those CPUs.
- **Dropped iterations, generator CPU, 503 count**: validity signals. If k6 dropped requests, the run did not offer its full rate.

## Pitfalls

- One offered rate means a size cap that is above R x W is invisible. Equal results for two caps are a finding, not a bug.
- Changing one setting changes others indirectly (slower acks gives bigger batches through the in-flight limit). Read the batch size panel before blaming a setting for a latency change.
- `acks=0` answers before the broker has the record, so its latency is not comparable as "the same service"; the audit (records in the topic against 202 answers) is the check that nothing was dropped.
- Compression results depend on the payload. The filler must be random or the numbers do not mean anything for real events; the fixed seed keeps it the same in every run.
- Wire bytes include protocol framing and update in steps of the statistics interval; short windows are noisy.
- Every run starts with a fresh server and an empty topic on purpose: the first seconds of a Node process (JIT warm-up) and a growing log would otherwise leak between configs. The warm-up requests are not counted.
- Restarting for every run means each config meets the same conditions, but a drift over the 50 minutes (thermal, other processes) can still bias one config; the second repeat runs in reverse order to expose it.
