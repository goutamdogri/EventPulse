# B10 Missing timeouts: concepts

A timeout (a deadline) is the longest time a program is willing to wait for something outside itself: a pool connection, a query, a network answer. If nothing sets a deadline, the program waits for as long as the other side takes, and "forever" is a valid duration. This note collects what I need to know to understand what happens when a slow dependency meets a service with no deadlines.

The chain for one request in ingest-api:

1. The request arrives and the server accepts it. From now on it uses memory (the body, the parsed object, the response object).
2. The handler asks the pg pool for a connection. If all 25 are busy, the request waits in the pool's queue.
3. With a connection, the handler sends the INSERT to Postgres and waits for the answer.
4. The answer comes back and the server sends 202. If something fails, the filter turns the error into a 503.

Each of the steps 2 and 3 can wait. Each wait needs its own deadline.

See also: `notebook/stage-1/learning/nodeAndNest/1. node-pg-pool.md`, `notebook/stage-1/learning/nodeAndNest/4. http-status-semantics.md`, `notebook/stage-1/learning/Postgres/7. postgres-wait-events.md`, and `Explanation.md` here for the hypothesis and predictions.

## Concepts

### Pool wait deadline (`connectionTimeoutMillis`)
How long a request may wait for a free connection, including opening a new one. The default in `pg` is 0, which means wait forever. It decides how long the queue in front of the pool can hold a request. With a deadline of 2 s, a request is refused after 2 s with an error the server maps to 503. Without it the queue is only limited by memory.

### Statement deadline (`statement_timeout`)
A Postgres setting: how long one statement may run. The timer runs inside Postgres, and it counts the time the statement spends executing or waiting for locks inside the database. It does not count network time. So a slow network (added latency) cannot trigger it; a lock wait or a slow query can. When it fires, Postgres cancels the statement and the connection goes back to the pool. Default 0 (no limit).

### Client deadline (k6 `timeout`)
The client gives up after some time. This frees the client, but it does not tell the server to stop. The server cannot see that the client left until it tries to write the response. So a request whose client is gone can still wait in the pool queue, still run its INSERT, and still be counted as work. Only a deadline inside the server stops that.

### Queue and memory
A queue is a list of waiting requests, and each entry holds memory (here mostly the request body, a Buffer, plus objects for the handler). If requests arrive faster than they leave, the queue grows by (arrival rate minus service rate) per second. Memory grows with it. Node keeps going until the heap or the machine runs out, so the failure is slow and silent.

### Capacity of the pool and Little's Law
The pool can serve at most `pool size / time one request holds a connection`. Little's Law says the average number of requests in the system equals arrival rate times the average time each spends there (L = lambda x W). With a deadline, W is capped, so L is capped. Without it, W grows and so does L.

### Fail fast and goodput
Failing fast means refusing a request quickly with a clear error (503, usually with `Retry-After`). Goodput is the rate of requests that finish successfully in time for the client to use the answer. Throughput that serves clients who already left is not goodput. A server that processes its queue in order can end up doing only wasted work.

### Table lock stall
An `ACCESS EXCLUSIVE` lock on a table blocks every other statement on that table until the lock holder ends its transaction. Waiting backends are alive but not working: in `pg_stat_activity` they show a wait event of type `Lock`. This is a clean way to make statements slow on the server without changing the network.

## Signals and what they mean

- **Requests waiting for a pool connection** (instantaneous gauge): queue length in Node. Healthy: near 0. A steadily rising value means arrivals exceed what the pool serves.
- **Connections total / idle**: total at the pool max and idle at 0 means every connection is busy or stuck.
- **HTTP requests in flight** (instantaneous): accepted and not yet answered. It includes requests whose client has left.
- **Pool wait time** (histogram, cumulative buckets): only recorded for requests that did get a connection. Requests refused by the deadline are not in it.
- **Database errors by type** (cumulative counter, take a rate): `pool_timeout` and `statement_timeout` only appear if a deadline exists.
- **Status 0 in k6**: no HTTP answer arrived before the client deadline. It is not an HTTP status. Healthy: none.
- **Process memory**: resident set size and heap move slowly; heap does not shrink immediately after the queue drains because garbage collection runs later. Compare peaks.
- **Dropped iterations in k6**: the generator could not start requests on time (it ran out of virtual users). A hung server can cause this, because each hung request keeps a virtual user busy for the whole client deadline.

## Pitfalls

- A latency added on the network does not trigger `statement_timeout`. To test the statement deadline the statement itself has to be slow on the server.
- Hung requests keep k6 virtual users busy: virtual users needed is about rate times client timeout. If k6 runs out, the offered rate is no longer what I set. The rates in this experiment are chosen so that this does not happen.
- The client timeout and the server deadlines are different things. A request counted as hung in k6 may still be processed by the server later.
- Heavy captures (`pg_stat_activity`, `pidstat`, `vmstat`) would disturb a measured run, so they are taken only in the probe run.
- k6 series stay visible in Prometheus for a while after a run ends, so the last value can look as if the run were still going.
- Memory comparisons between runs only work if the server is restarted for each run, otherwise earlier heap growth stays in the baseline.
