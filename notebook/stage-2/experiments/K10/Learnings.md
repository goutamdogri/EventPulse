# K10 Learnings: queues, backpressure and load shedding

An API that writes to Kafka has a buffer between "a request arrived" and "Kafka confirmed it". In the ingest-api that buffer is the micro-batcher queue: many HTTP requests drop their message into it, and one sender takes up to 500 at a time and sends them as a batch. Every request waits on a promise that is resolved only when its batch is acknowledged. As long as Kafka answers quickly the queue is nearly empty. When Kafka stops answering, the queue is the only thing between the clients and the failure, and what it does with that pressure decides whether the API survives.

How pressure travels when Kafka is gone:

1. Requests keep arriving at the same rate (clients do not know about Kafka).
2. The sender is stuck on a batch that cannot be acknowledged, so nothing leaves the queue.
3. Each new request is either put in the queue (memory grows, the client waits) or refused at once (memory stays flat, the client gets an answer).
4. When Kafka comes back, the API either continues by itself or needs a restart.

See also: `../K4/Learnings.md` for broker failures and `../K7/Learnings.md` for a slow downstream. The hypothesis and the predictions are in `Explanation.md`.

## Concepts

**Unbounded queue.** A queue with no limit. Accepting is always "successful", so the producer side never learns that the consumer side is stuck. Every waiting item costs memory: the message, a promise, the closure, and the HTTP request and response objects that the waiting handler still references. The cost per item is a few kilobytes, so 2,000 requests per second adds up to several megabytes per second. A queue without a limit does not remove the overload, it only moves it from "the client sees an error" to "the server runs out of memory".

**Bounded queue.** A queue with a maximum length (and here also a maximum size in bytes). When it is full, new items are refused immediately. The refusal is the signal that tells the caller to back off. The limit is a decision about how much waiting is acceptable: 10,000 messages at 2,000 req/s is about 5 s of work.

**Load shedding.** Deliberately refusing some work so the rest can be done well, or so the process stays alive. The 503 status with a `Retry-After` header is the HTTP form of it: the server says "not now", and a well-behaved client waits. A fast 503 costs the server almost nothing, a request that hangs costs a connection, a queue slot and memory.

**Backpressure.** Pushing the "I am full" signal back to whoever produces the work, instead of absorbing it. It has to exist at every boundary, because a boundary without a limit is where the pressure piles up: the socket accept queue, the HTTP handler, the batcher queue, the librdkafka producer queue, the broker. A bound at one boundary only protects the layer behind it.

**Timeouts as a bound on waiting.** A queue bounded by size still lets an item wait a long time if the consumer is slow. A timeout bounds the wait in time. Here a message waits at most the send timeout (5 s) for its batch to start, and a send itself is cut at 5 s. The unbounded mode removes this timeout on purpose, so there is no hidden cap.

**Circuit breaker.** A small state machine in front of a dependency: closed (normal), open (the dependency is known to be failing, refuse at once), half-open (after a cooldown let one probe through to see if it works again). It makes a known outage cost nothing per request. Here it opens after 3 failed sends or when the background health check fails, and it empties the queue when it opens.

**Open and closed load models.** A closed model (a fixed number of users who wait for each answer) slows down by itself when the server slows down. An open model (arrivals at a fixed rate) does not. Real internet traffic is closer to open, so an open model in k6 is the honest test of what happens during an outage. The price is that stuck requests pile up in the generator too: each one holds a VU until the client timeout.

**Process memory, heap and the heap cap.** RSS is the memory the OS has given the process. The V8 heap is the part that holds JavaScript objects. `--max-old-space-size` caps the old-space heap; when live objects need more, the process aborts with "heap out of memory". Before that, the garbage collector runs more and more often because it finds little to free, which shows as CPU and event-loop delay.

**Reconnection.** The Kafka client reconnects to brokers by itself with a backoff (0.5 s growing to 10 s here). Whether the whole API recovers without a restart depends on its own state too: a queue that is full of dead requests, a breaker that stays open, or a process that already crashed.

## Signals and what they mean

- **Queue depth and bytes** (instantaneous): how much is waiting. A line that rises without bound is a leak in the making; a line that stops at a flat top is a limit working.
- **Rejections by reason** (cumulative counter, look at the rate): `overloaded` means the queue was full, `breaker_open` means the breaker refused, `queue_timeout` means the message waited too long.
- **503 by reason** (cumulative): what the API itself answered. Compare with what the client counted: a request that the client abandoned before the answer shows up on the server only.
- **Circuit breaker state** (instantaneous): 0 closed, 1 open, 2 half-open.
- **Producer connected and health check** (instantaneous): the API's own view of Kafka. They can disagree with each other for a few seconds.
- **RSS and heap used** (instantaneous): memory. A line that ends abruptly is a process that died.
- **Requests in flight and open connections** (instantaneous): requests accepted but not answered.
- **Client side (k6)**: status 202, 503, and "other" (no answer: timeout or refused connection). The latency of a timed-out request equals the client timeout.
- **k6 dropped iterations and VUs** (validity): the generator itself must not be the limit.

## Pitfalls

- With the send timeout removed, an unbounded API has no limit of its own, so the heap cap set by `run.sh` is what ends the run. The number I get for "time until death" depends on that cap and on the CPU pinning, it is not a property of Node in general.
- A client that gives up (timeout) closes its socket, but the server may still hold the request in the queue. Server memory is therefore not bounded by what the client is still waiting for.
- A trial run is a plumbing check and its numbers are never used.
- k6 series are not marked stale after a run, so the last values can stay on the graphs for minutes.
- All three brokers dead also means the KRaft controller quorum is gone. Recovery time includes the election after the restart, not only the API's reconnect.
- k6 itself needs memory for the stuck requests (one VU each). If the generator runs out of VUs it drops iterations and the offered load is no longer constant.
- Reading `/metrics` from an API that is nearly out of memory is slow and may fail; the RSS samples read from `/proc` are the reliable source for memory.
