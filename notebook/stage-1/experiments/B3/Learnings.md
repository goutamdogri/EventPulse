# B3 Learnings: a slow dependency inside a request

When a request has to wait for the database, the database becomes part of the API's own response time. The API keeps a pool of database connections. Each request borrows one, sends its query, waits for the answer and gives the connection back. If the answer takes longer, the connection is borrowed for longer, so fewer requests per second fit through the same pool. This is the cycle:

1. A request arrives and asks the pool for a connection.
2. If one is free, the request takes it. If not, it waits in a queue.
3. The request sends the query and waits for the answer. The connection is busy the whole time.
4. The answer arrives, the connection goes back to the pool, the next waiting request takes it.

See also: `notebook/stage-1/learning/nodeAndNest/1. node-pg-pool.md`, `notebook/stage-1/learning/Postgres/2. postgres-write-path.md`, and `Explanation.md` in this folder for the hypothesis and predictions.

## Concepts

**Hold time.** How long one request keeps a pool connection: the query time plus the time the answer needs to travel back. In B2 it was dominated by the commit wait, not by the INSERT itself. Anything added between the API and the database (network, a proxy, a slow disk) adds directly to it.

**Pool capacity.** With N connections and a hold time H, at most N / H requests per second can finish, because each connection serves 1 / H requests per second. Example: 25 connections and 25 ms give 1,000 requests per second. The pool is a gate, it does not matter how fast the CPU is.

**Little's Law.** The average number of requests inside a system equals the arrival rate times the average time each stays: L = lambda x W. It holds for any stable queue. It lets me check two measurements against each other: the in-flight gauge should match rate x mean latency. Example: 500 req/s with 25 ms each means about 12 requests in the system.

**Open and closed load, queue bound.** k6's constant-arrival-rate keeps sending even when the API is slow, so the queue grows until something stops it. Here two things stop it: the pool's connection timeout (a request that waits too long gets a 503) and the number of k6 virtual users. The longest wait is the smaller of the two bounds.

**Added latency versus a slow database.** A latency toxic in a TCP proxy delays every chunk of data by a fixed time. It does not make the database do more work. So Postgres is healthy and mostly idle, while the API suffers. That is the difference from a database that is slow because it is overloaded.

**Round trips.** A latency is paid once per round trip. A single autocommit INSERT is one round trip. Opening a new connection takes several (authentication handshake), so connections opened while the latency is on pay it several times.

**Deadlines.** Without a pool wait timeout and a statement timeout, a slow dependency makes requests wait forever instead of failing. The 2 s connection timeout is what turns a queue into 503 answers.

## Signals and what they mean

- **Requests in flight** (instantaneous): requests the server has accepted and not answered. Healthy: small. Growing without bound: a queue.
- **Pool in use, idle, waiting** (instantaneous): in use equals the pool size when the pool is the limit; waiting greater than zero means requests are queued for a connection.
- **Pool wait time** (a distribution of a cumulative counter): time spent waiting for a connection. Near zero when healthy, near the timeout when saturated.
- **Query duration** (distribution): time from sending the query to the answer, which is the hold time here.
- **Accepted versus offered requests per second** (rates): the gap is what the system could not take. Accepted at saturation is the capacity.
- **Postgres connections by state** (instantaneous): active means running a query, idle means connected and waiting for the next command. A slow API with an idle database is a sign the problem is outside Postgres.
- **Dropped iterations in k6** (cumulative): k6 wanted to send a request but had no free virtual user. This means the generator, not the API, set the limit.

## Pitfalls

- The proxy shares CPU cores with the monitoring stack. If those cores are busy the proxy, not the pool, becomes the limit. The control probe without the proxy checks this.
- Latency added before the warm-up would be paid by every connection handshake. The latency is added after the warm-up so mostly established connections are measured.
- The toxic applies in one direction only. Putting it in both directions would double the per-query latency.
- k6 pushes metrics once a second, so a recovery time shorter than about one second cannot be seen.
- Heavy tools (iostat, pidstat, `pg_stat_activity` grouping) cost CPU, so they run only in probe runs.
- k6 series in Prometheus are not marked stale after a run, so the last value can linger in a panel.
