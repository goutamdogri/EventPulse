# K12 Learnings: size limits, and why each layer has its own

An event on its way from a client to a stored record passes several places that each have a size limit: the HTTP server that reads the request, the application code that queues it, the Kafka client library that packs it, the broker that appends it, and later the follower brokers and the consumer that read it back. Each limit is set in a different place, by a different setting, and each reports a failure in its own way: some answer the HTTP client with a clear 4xx, some end in an error object deep in a library, and some do not report anything. The size of the event is the same everywhere, but the limit that is hit first, and what the caller sees, depends on which layers are configured how.

1. The client sends the request with a body. The HTTP server compares the announced body size with its limit and refuses early or reads the body.
2. The application validates the event and turns it into a Kafka message (key, value, headers). The batcher checks the size of the message against its byte cap and groups messages into a batch.
3. The Kafka client library checks one message against its own `message.max.bytes`, packs messages per partition into record batches, and sends produce requests.
4. The broker checks the size of each record batch against `message.max.bytes` (or the topic's `max.message.bytes`) and appends it or answers with an error.
5. Followers copy the batch (`replica.fetch.max.bytes`), consumers read it back (fetch size limits).

See also: `notebook/stage-2/experiments/K6/Learnings.md` (a bad message and what the consumer does with it), `notebook/stage-2/experiments/K10/Learnings.md` (the batcher queue and its bounds), `Explanation.md` in this folder for the hypothesis and predictions.

## Concepts

**Edge validation.** Checking a request before the application does any real work with it. The limit on the request body is the first one. In the Express stack the body-parser reads the `Content-Length` header and refuses with 413 (Payload Too Large) when it is above `BODY_LIMIT`, before it has read the body. This is cheap for the server: no parsing, no Kafka. Whatever is refused here never costs a queue slot, a producer call or a broker request.

**HTTP status as a message to the caller.** 413 says: this request is too big, the fault is yours, do not send the same request again. 500 says: something broke on my side, maybe retry. 503 says: I am temporarily not able, retry later (with `Retry-After`). A size problem answered with 500 or 503 is a lie that makes a well-behaved client retry for ever.

**Message, record batch and request.** A message is one record: key, value and headers. The producer packs the messages for one partition into a record batch; the broker's size limit applies to that batch, not to one message. A request to a broker can carry batches for several partitions. So a limit of 1 MB can be exceeded by one big message or by many small ones that were packed together. The client library cuts its batches so that they stay under its own limit, but it does not know a lower limit that only the topic has.

**The limits, by layer.**
- `BODY_LIMIT` of the API: the HTTP body, in bytes (`64kb` = 65,536 bytes). Edge.
- `BATCH_MAX_BYTES` of the batcher: the size of one message as the batcher counts it (key + value + header names and values), and the size of a batch. A message above it is refused before it is queued. The configuration refuses a value above 1,000,000.
- `message.max.bytes` of the client (librdkafka default 1,000,000): the largest message the client agrees to send. The client adds its own overhead per message, so a message a little below 1,000,000 can still be too large.
- `message.max.bytes` of the broker (default 1,048,588): the largest record batch the broker appends. Slightly above 1 MiB (1,048,576) because it counts the batch header.
- `max.message.bytes` of a topic: overrides the broker's value for one topic. It is the way to lower the limit for one topic without touching the cluster.
- `replica.fetch.max.bytes`, the consumer's fetch sizes: limits on copying and reading. A batch above them is still handled in modern Kafka (the first batch is always returned), but the setting still decides how much is fetched at once.
- `socket.request.max.bytes` of the broker: the largest request of any kind (100 MB by default).
The limits must be nested: edge below batcher cap below client limit below broker limit. If a lower layer is looser than a higher one, the higher one never does its job; if a higher layer is looser, the error appears in a layer that is far from the caller and harder to read.

**Compression.** The broker's limit is measured on the batch as it is on disk, after compression, so a compressed batch can carry more raw bytes than the limit. The batcher counts raw bytes (before compression), so it is a stricter test than the broker's.

**What the producer waits for (acks).** With `acks=all` the producer call finishes only when the broker has answered, so an error from the broker comes back to the caller. With `acks=0` the producer does not wait for an answer: for a record batch the broker refuses, it can only close the connection, and the client has nothing to report per message. That is how a size failure can be silent.

**All or nothing at the API.** The API calls the producer with a whole batch and waits for the whole call. If the call fails, every request that was in that batch gets the failure, including the small, valid events that were only next to the bad one. Some of those events may still have been appended by the broker (other partitions). The caller is told "failed" about events that may exist in the topic.

## Signals and what they mean

- HTTP status at the client and at the API (cumulative counters, read as rates): 202, 413, 500, 503, and status 0 for "no answer at all". Status 0 appears when the server closes the connection while the client is still sending a big body.
- `route` label of the API request counter: a refusal by the body-parser has no matching route, so it is counted under `unmatched`.
- Batcher rejects by reason (cumulative): `too_large` counts the events refused by the byte cap, before any queueing.
- Kafka send errors by type (cumulative): failed batches, named by the client's error type. The names tell which layer refused (a client-side "message size too large" and the broker's refusal both exist).
- Messages acknowledged and messages failed (cumulative): the first counts events the broker confirmed, the second counts every event of a failed batch, which can be more than the bad ones.
- Log end offset of the topic (cumulative, only goes up): the number of records the topic has ever received. A refused event must not change it. Together with the acknowledged count it is the audit: acknowledged = records added, otherwise something was lost or stored twice.
- Batch size in bytes (histogram): shows how close the batches get to the cap.

## Pitfalls

- The size of the body, the size of the value and the size the batcher counts are three different numbers. The value has the tenant added, and the message has a key and headers. A level "at the limit" must say which of the three it measures.
- A limit exactly at a power of two is a trap: `64kb` is 65,536 bytes, and Kafka's default is 1,048,588, not 1,048,576.
- The real broker limit cannot be reached through the API because the API refuses larger events itself. To see the broker's refusal the limit has to be lowered on the topic; this stands in for the real limit and is not the same number.
- Big requests cost the API even when they are refused: the connection, the memory for what was read before the refusal, and the time the event loop is busy.
- An error that closes the connection (a refusal while the client is still sending) can reach the client as a reset and not as a 413. Not seeing a 413 does not mean the server did not refuse.
- Counters of the API restart with the process. The API is restarted for every cell here so a cell starts at 0, but a dashboard line for a counter has a gap or a reset at every restart.
- Measuring the topic with a Kafka tool changes nothing, but the offsets are read twice (before and after a cell), so a late acknowledgement could fall outside the window. A short wait before the second read covers it.
