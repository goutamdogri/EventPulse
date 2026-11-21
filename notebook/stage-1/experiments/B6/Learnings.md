# B6 Learnings: what a commit promises

This is the concept page for the durability experiment. A database commit is a promise: "this write will still be there after a crash". Making that promise true costs time, because the data has to reach storage that survives a power cut. Postgres lets me choose how long a commit waits before it makes the promise (`synchronous_commit`). The same question comes back everywhere: how long does the writer wait for the "it is safe" answer, and what is lost if it does not wait?

The path of one insert in Postgres (write-ahead logging, WAL):

1. A backend process (one per connection) changes the table page in shared memory (the page is now "dirty", not yet on disk).
2. It first writes a WAL record describing the change into the WAL buffers (also shared memory).
3. At COMMIT it writes a commit record. With `synchronous_commit=on` the backend now waits until the WAL up to that record is flushed to disk (`fsync`/`fdatasync`), then returns.
4. Later (checkpoints, background writer) the dirty table pages are written to the data files. This can wait because the WAL already has the change.
5. After a crash, Postgres replays the WAL from the last checkpoint (crash recovery), which brings back every change whose commit record reached the disk.

See also: `notebook/stage-1/learning/Postgres/2. postgres-write-path.md` and `6. postgres-checkpoints.md`. The hypothesis and predictions are in [Explanation.md](Explanation.md).

## Concepts

**WAL (write-ahead log).** Postgres writes the description of every change to a sequential log before it counts the change as done. Sequential writes are cheap, and the log is enough to rebuild the table pages after a crash. The rule is "log first, data later". That rule is why the commit can return before the table page is on disk.

**WAL buffers and the page cache.** A WAL record first lives in Postgres' own memory (WAL buffers). It becomes visible to the operating system when Postgres calls `write()`: now it is in the kernel's page cache. It becomes safe against a power cut only after `fsync`/`fdatasync` has pushed it to the disk. So there are three places: Postgres memory, kernel memory, disk. A process crash (SIGKILL of Postgres) loses the first. A machine crash or power cut loses the first and the second. Only the third survives everything.

**fsync and why it is slow.** `fsync` asks the disk to put data on stable media and waits for the answer. It takes milliseconds on a disk even when the data is tiny, because it has to wait for the device. In B1 the flush time was most of the 5 ms that one INSERT kept its pool connection busy.

**Group commit.** When several backends commit at about the same time, one `fsync` can cover all their commit records. So the number of flushes per second is lower than the number of commits per second. How many commits share a flush depends on how many are committing at the same moment, and with a small pool that is limited.

**synchronous_commit.** The setting says how long COMMIT waits. `on`: until the WAL is flushed to local disk. `off`: it does not wait at all. The commit record is only in the WAL buffers; the WAL writer process (a background process that wakes every `wal_writer_delay`, 200 ms by default) writes and flushes it a little later. The documentation says the delay until the data is safe is at most three times `wal_writer_delay`. Important: with `off` the database is never corrupted after a crash, because the WAL is still written in order and the recovery only stops at the last record that reached disk. The result is a database as it was a moment ago, with the newest commits missing. This is different from `fsync=off`, which can corrupt the database and which I do not touch.

**What "lost" means for the service.** The API answers 202 when the INSERT's COMMIT returns. With `on` that means the data is safe. With `off` it means "accepted, probably safe in a fraction of a second". A client that got 202 will not retry. So the rows that were acknowledged but not safe are rows that nobody knows are missing. That is the difference between a fast write and a durable one, and in Stage 2 the same choice appears as Kafka `acks`.

**Limits of the test.** I can kill the Postgres container hard. That is a process crash: the kernel and its page cache survive, so everything Postgres had already handed to the kernel survives. What is lost is only what still sat in the WAL buffers. A power cut would lose more. So the number I measure is a lower bound of the real loss.

**Queueing view of the pool.** One request holds a pool connection for the whole INSERT. The pool can complete at most `pool size / hold time` requests per second (Little's Law: connections in use = rate x hold time). If the commit wait is the biggest part of the hold time, removing it raises that ceiling a lot. But another limit (the single Node thread, or Postgres CPU) will take over. The bottleneck moves; it does not disappear.

## Signals and what they mean

- **Accepted requests per second (202/s)** counted by k6: the rate of promises made. Instantaneous. Compare modes at equal offered load.
- **Latency p50 and p99** at the client: p99 is the unlucky requests and reacts first when utilisation (rate / capacity) goes towards 1.
- **Query time in the server**: includes the COMMIT wait. It is the quantity the setting changes directly.
- **Pool in use and waiting**: how close the pool is to its limit. Waiting above 0 means requests queue before they reach the database.
- **Disk flush requests per second**: how often the host asked a disk to flush. It is a counter, so I use its rate. It also counts other processes (checkpoints, the Prometheus container), so it never goes to zero.
- **Commits per second** at Postgres: should match the accepted rate for a one-INSERT-per-request service.
- **WAL bytes per second**: how much log is written. Changing `synchronous_commit` changes when it is flushed, not how much is written.
- **pg_up and the 503/other rates**: during a crash probe they show when the database was gone.
- **Row audit (202s counted by k6 vs rows in the table)**: the durability signal itself. Each is a count taken once at the end, so it is not a time series.
- **Cumulative vs instantaneous:** `node_disk_*`, `pg_stat_*`, WAL counters and k6 counters are cumulative (take a rate or a difference); pool in use, in flight and `pg_up` are instantaneous.

## Pitfalls

- Treating `off` as a free win. The stage document lists it as a pitfall: it is a trade, and this experiment is only a measurement, not a setting to keep.
- Comparing modes at different offered loads. The same rates are used for both modes, and the order is alternated inside each repeat so drift hits both equally.
- Crash numbers depend on timing. The loss is roughly "commits in the last 0 to 200 ms (and in rare cases more)", so one probe is not enough; I repeat it and look at the spread.
- A process kill is not a power cut. Do not read the number as "all that could be lost".
- The kill also stops the exporters that read the container, so crash probes run after the measured runs and panels for WAL and Postgres CPU stay empty afterwards.
- If the database is restarted while requests are still arriving, the new database can answer 202 again. The server is stopped right after the kill, so the acknowledged count stays "promises made before the crash".
- Rows can exceed the 202 count by a few: a request that committed just before the kill but whose answer never reached k6. A negative "lost" has to be read with that in mind.
- A run with dropped iterations or a k6 CPU group above about 70% cannot be used as a capacity number.
- `pg_stat_wal`, `docker kill` and the kernel are shared: other containers are on the same machine, so the flush counter includes their flushes.
