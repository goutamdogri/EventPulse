# B9 Learnings: retry storms and metastable failure

A retry is a second chance for a request that failed. It is useful when the failure was a one-off (a dropped packet, a restarted pod). It is harmful when the failure comes from overload, because the retry adds load to the thing that is already overloaded. A metastable failure is a state where the system stays broken even after the thing that started the trouble (the trigger) is gone, because something inside the system now keeps the overload alive. Retries are the classic example. The cycle, in plain steps:

1. The system runs healthy at some load below its capacity.
2. A trigger makes it slower for a short time (here a slow database). Capacity drops below the load.
3. Requests queue up and clients start to time out.
4. Clients retry, so the offered load goes up to a multiple of the original.
5. The trigger goes away and capacity is back, but the offered load is still above it and the queue is full of old requests.
6. The system spends its capacity on requests nobody is waiting for any more, so it keeps failing and the clients keep retrying. It stays in that state until the load is cut from outside.

See also: `notebook/stage-1/experiments/B3/Learnings.md` (slow database, pool as a queue), `notebook/stage-1/learning/nodeAndNest/4. http-status-semantics.md`. Hypothesis and predictions: `Explanation.md`.

## Concepts

- **Capacity and the pool.** The pool has N connections and each request holds one for h0 seconds, so capacity is about N / h0. A slower database raises h0 and lowers capacity. Example: 5 connections and 5 ms give 1000 req/s; 55 ms give 91 req/s.
- **Offered load vs goodput.** Offered load is everything clients send, retries included. Goodput is the part that gets a useful answer. In a healthy system they are equal. In a metastable state offered load is high and goodput is near zero.
- **Open model.** The generator starts new events at a fixed rate whatever the server does (k6 `constant-arrival-rate`). A real population of users does not slow down just because the server is slow, so this is the right model to see a collapse. A closed model (a fixed number of users who wait for an answer) would slow itself down and hide the problem.
- **Amplification.** If every failed request is retried up to R times, one event can cost up to R + 1 requests. With R = 3 that is a factor of 4 at the worst moment. It is not a property of the server, it is a property of the clients.
- **Timeouts that do not match.** The client gives up after 1 s, the server's pool waits up to 2 s. A request that waits 1.5 s is still served, but its client left half a second ago. The work is done and the answer is thrown away. This is wasted work, and it is what turns an overload into something self-sustaining: the server is busy, but goodput is zero.
- **Queue with a deadline.** A FIFO queue that drops items only after 2 s stays full under overload, and every item that reaches the front has waited close to 2 s. If the client timeout is shorter than that, every served item is stale.
- **Idempotency.** A retry may arrive after the first try already committed. The server uses `INSERT ... ON CONFLICT DO NOTHING` on `event_id`, so a retry with the same id does not create a second row. Without it, retries would also duplicate data.
- **Backoff.** Waiting longer before every next retry (for example 100, 200, 400 ms). It spreads retries out in time. It does not by itself reduce how many retries are sent if every event may still retry the same number of times.
- **Jitter.** A random part added to the wait, so that many clients that failed together do not retry together. Without it the retries come back as synchronised waves.
- **What comes later in the project.** Rate limiting, retry budgets and load shedding act on the other side of the problem: they cut the number of requests that reach the overloaded part. They appear in later stages.

## Signals and what they mean

- **Attempts per second** (instantaneous rate of a counter): everything k6 sends. Compare it with the new events per second. A ratio above 1 means retries are being sent.
- **Goodput (accepted events per second):** healthy equals the original rate. A value near zero while the database is already fast again means the stuck state.
- **Share of attempts answered 202:** healthy is about 100 %. It is the simplest single signal of recovery.
- **Pool waiting and pool wait time:** how long a request waits for a connection. A wait close to the server limit that does not go down after the trigger ended means the queue is full of old requests.
- **Client timeouts (status 0) vs 503:** status 0 means the client left first. 503 means the server gave up first.
- **Postgres commits per second vs accepted events per second:** commits above accepted events means work was done for clients that were gone. Commits are a cumulative counter, so only the rate is meaningful.
- **Dropped iterations (k6):** k6 could not start every planned event, usually because retrying iterations hold virtual users. Any value above 0 makes a run invalid.
- **Cumulative counters** (requests, commits, errors): take the difference over a window. **Instantaneous gauges** (pool waiting, in flight, VUs): read the value.

## Pitfalls

- The generator is part of the system here. A retrying iteration holds its virtual user for several seconds, so a high original rate needs thousands of VUs. If VUs run out, the offered load is silently capped and the storm looks smaller than it is.
- The result depends on the relation between the client timeout and the server's queue timeout. Another ratio can give another answer, so the numbers are valid for these settings only.
- Recovery time needs a definition fixed before the run (here: at least 99 % of attempts accepted and attempts/s within 10 % of the original for 10 s in a row). The word "recovered" is easy to bend afterwards.
- k6 series are not marked stale after a run, so the last value can linger in Grafana for minutes.
- Heavy tools (strace, perf) are not used in measured runs; they would slow the server and change the result.
- The pool is 5 here, not 25, so absolute numbers are not comparable with B3. Only the shape of the behaviour is.
