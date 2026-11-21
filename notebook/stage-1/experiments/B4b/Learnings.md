# B4b Learnings: commit flush, group commit and pool slots

In B4 a hot row capped one tenant at about 330 transactions per second, and 10 tenants gave about half of what 10 independent rows should give. This note is about the two ideas I need to read the follow-up: what a commit waits for, and how a small connection pool distributes work over queues.

The cycle of one transaction on a hot row:

1. The API takes a connection from the pool and sends `BEGIN`, `INSERT`, `UPDATE`.
2. The `UPDATE` waits for the row lock (if someone else holds it) and then takes it.
3. The API sends `COMMIT`. Postgres writes the commit record to the write-ahead log (WAL).
4. With `synchronous_commit=on`, Postgres waits until the WAL is flushed to disk, then answers.
5. The lock is released and the next waiter on that row continues.

See also: `Explanation.md` in this folder for the hypothesis and predictions, and `notebook/stage-1/experiments/B4/Learnings.md` for row locks.

## Concepts

**Write-ahead log (WAL) and the flush.** Before a change is final, Postgres writes it to a sequential log. A commit is only durable when that log is on disk, so the commit waits for a flush (`fsync`). The flush time depends on the disk, not on the CPU. On the laptop I do not know it; B4 suggested about 3 ms for one transaction, but that was inferred, not measured.

**`synchronous_commit`.** With `on` (the default) a commit waits for the flush. With `off` the commit returns right after the record is in memory, and the log writer flushes a little later. Cost: if Postgres crashes, the last few transactions that were already acknowledged can be lost (the database stays consistent). It can be set per role or per session, and it only applies to new sessions in the role case.

**Group commit.** When many sessions commit at the same time, one flush can cover all of them. This is why many independent transactions do not each pay a full flush. Transactions that wait for the same row lock cannot be grouped: the second one only starts after the first one finished. So with one hot row each commit pays its own flush, and with 10 hot rows up to 10 can share one.

**WAL write lock (`WALWrite`).** Writing and flushing the WAL is done by one session at a time, guarded by a lightweight lock. Others wait for it (`LWLock/WALWrite`) and then often find their record already flushed. If that queue gets long, each commit takes longer, and so does every row lock held across the commit.

**Connection pool as a limit on parallel rows.** The pool has a fixed number of connections (25). A request that waits for a row lock keeps its connection. If most connections sit in the queue of a few tenants, the other tenants get fewer connections, and the number of rows that make progress at the same time can drop below the number of tenants. This is what I call pool drift. With a pool much larger than the number of tenants every row will always have somebody ready.

**Max connections.** Postgres has a hard limit (100 here) on sessions. Pool size, the metrics exporter and my own checks all count against it.

**Little's Law.** Sessions in the system = rate x time in the system. Used to check if a wait time and a rate fit together: for example the rate times the mean UPDATE time gives how many sessions are inside the UPDATE at any time.

## Signals and what they mean

- **Wait events `LWLock/WALWrite` and `IO/WALSync`:** sessions waiting to write or for the disk to confirm the flush. Many of them means the commit path is the bottleneck. Instantaneous, a sample.
- **Wait event `Lock/transactionid` and `LWLock/LockManager`:** waiting for another transaction's row lock, and for the internal structure that manages those locks.
- **Session state `idle in transaction`:** the transaction is open and the lock is held, but Postgres is waiting for the next statement from the API. Time in this state is part of the lock hold time.
- **Mean counter UPDATE time:** how long a request waits at the row lock, including the queue. Computed from the sum and count of the histogram, so it is an average and hides the tail.
- **Commits per second:** cumulative counter, so I take a rate. It is the real throughput of the database.
- **Pool waiting and pool wait time:** how many requests wait for a connection, and for how long. A full pool with a falling rate is a sign of waiters holding connections.

## Pitfalls

- `synchronous_commit=off` changes what a commit costs everywhere, not only on the hot row. Any speed-up has to be read as "the flush was part of it", not as "the lock is gone".
- A role setting does not change sessions that are already open. The server must be restarted after it is set, otherwise the pool still has old connections.
- A bigger pool makes the database do more at the same time. If the Postgres CPU becomes the limit, a flat result can come from the CPU and not from the lock.
- One wait-event sample is one instant. I need averages over the run to say what most sessions were doing.
- The role setting must be removed after the run, otherwise it silently affects other experiments.
