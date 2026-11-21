# B4 Learnings: row locks and the hot row

When two transactions want to change the same row, Postgres lets only one of them do it at a time. The second one has to wait until the first one finishes (commits or rolls back). That wait is a row lock. Locks are what keeps data correct under concurrency, but they also turn parallel work into a queue. This note is about how that queue forms and what it looks like from outside.

How one write goes through a row lock:

1. The transaction runs `UPDATE` on a row. Postgres marks the row as changed by this transaction.
2. A second transaction wants the same row. It sees the row is owned by an unfinished transaction and waits.
3. The first transaction commits. The commit is only done when its log record is on disk.
4. Postgres wakes the waiter. It now owns the row and the cycle repeats.

See also: `Explanation.md` in this folder for the hypothesis and predictions.

## Concepts

**Row lock.** Taken by `UPDATE`, `DELETE` and `SELECT ... FOR UPDATE`. Reading does not take it (MVCC lets readers see the old version). It is held until the end of the transaction, not the end of the statement. So the time that matters is from the `UPDATE` until the `COMMIT` is finished, not the time the `UPDATE` itself takes.

**Transaction.** A group of statements that succeed or fail together. A single statement without `BEGIN` is its own tiny transaction. A transaction with `BEGIN`, `INSERT`, `UPDATE`, `COMMIT` needs more round trips between the API and the database, and the lock is held across the ones after the `UPDATE`.

**Hot row.** One row that many transactions want to change. A per-tenant counter is one: everything a tenant sends touches it. The throughput on a hot row is at most `1 / lock hold time`. Example: if the lock is held 3 ms, one row can serve about 333 transactions per second, no matter how many connections wait.

**Serialization.** Work that could run in parallel is forced to run one after another because it shares one resource. Adding workers does not help because the shared resource is the limit. This is the same kind of limit as a single-threaded function in Amdahl's law: the serial part sets the ceiling.

**Commit flush and `synchronous_commit`.** With `synchronous_commit=on`, `COMMIT` returns only after the write-ahead log (WAL) is flushed to disk (`fsync`). A flush takes about milliseconds on a laptop SSD. Postgres can flush several transactions' commits together (group commit), but transactions that wait for the same row lock cannot be grouped: each one only starts after the one before it finished. So the flush time lands inside the lock hold time.

**Why spreading helps.** If the load is split over N tenants, there are N independent hot rows, and N queues work in parallel. Capacity is about N times the per-row limit, until another limit (CPU, pool, disk) takes over.

**Pool connections held by waiters.** A request that waits for a row lock still holds its pool connection. With a pool of 25 and one hot row, about 24 connections can end up doing nothing but waiting. New requests wait for a free connection (pool wait) and time out after `PG_CONN_TIMEOUT_MS`. The queue moves from the database into the API.

**Dead tuples and HOT updates.** An `UPDATE` in Postgres writes a new version of the row; the old version stays until cleaned up. A row updated thousands of times a second creates thousands of old versions. Postgres can prune them cheaply when the new version fits on the same page (a HOT update). It is a cost of synchronous counters.

**Aggregation downstream.** The alternative to a synchronous counter is to not count in the write path: write only the event, and count later from the events (a periodic query or a stream processor). Then no row is shared by the writers. This is the usual alternative.

## Signals and what they mean

- **Wait events** (`wait_event_type` and `wait_event` of a session): what a session is waiting for right now. `Lock / transactionid` means waiting for another transaction to finish (this is the row-lock wait). `IO / WALSync` means waiting for the commit flush. `Client / ClientRead` means the database is waiting for the API to send the next statement (idle round trip). Instantaneous: sampled, so it is a count of sessions at that moment.
- **Session state:** `active` is running or waiting inside a statement. `idle in transaction` means the transaction is open but no statement is running: the lock is held while the API does something else. Instantaneous.
- **Lock population (`pg_locks`):** how many locks of each type and mode exist, granted or not. A waiting session shows up as a lock that is not granted. The exporter counts per mode only, so it does not separate granted from waiting. Instantaneous.
- **Counter update latency:** time of the `UPDATE` as the API sees it. It includes the time spent waiting for the lock, so it is the direct measure of the queue on the row. A histogram: I read quantiles. The top bucket hides anything above it.
- **Pool wait and pool waiting:** time requests wait for a free connection, and how many are waiting now. They show that the lock queue has spread into the API.
- **Commits and updated rows per second (`pg_stat_database`):** cumulative counters, so I take a rate. Many commits with low CPU means the database is busy waiting, not computing.
- **CPU per group:** the claim of this experiment is about CPU that stays low while latency is high. CPU is about work done; a lock wait does no work.
- **Little's Law:** number in the system = arrival rate x time in the system. High latency at a given rate means many requests in flight, which matches many waiting sessions.

## Pitfalls

- The lock is held until commit, not until the end of the `UPDATE`. The order of statements in the transaction changes how long it is held: putting the `UPDATE` last keeps the hold short.
- A tenant that gets almost all the traffic is one hot row even when there are 1000 tenants. Here the load is spread evenly, so this experiment is the best case for many tenants.
- The pool is small (25). A pool timeout is an error from the API, not from Postgres. I must not read it as a database failure.
- Taking snapshots during a run adds a connection and some work. I keep them to one query set per `over` run, so latency is not disturbed much (not verified).
- The warm-up uses one tenant only and is deleted afterwards. A counter that is not reset between runs would break the audit (rows in `events` against the counter sum).
- k6 series are not marked stale after a run, so the last value can linger in Grafana. I read panels only inside the run windows from `runs.tsv`.
- Counters for the exporters are per scrape (1 s). Short spikes can be missed.
