# B11 Learnings: why a long run is different from a short run

A short load test starts on an empty table and ends before anything has grown. A real service does not live like that: the table gets bigger every second, the indexes get bigger with it, and at some size the data no longer fits in the memory that Postgres uses as a cache. A soak test is a long run at a steady, comfortable load to see what changes only because time passed. The chain I think about in this experiment:

1. Every accepted event adds a row to the table and one entry to each index.
2. The table and indexes grow, so the part of them that is "hot" (touched often) stops fitting in the cache.
3. A page that is not in the cache has to be read from the OS page cache or from the disk before it can be changed.
4. Dirty pages (changed, not yet on disk) pile up and are written by checkpoints; each checkpoint also makes the next change of every page expensive in WAL (full-page images).
5. Each insert gets a little slower, the pool connection is held a little longer, and the capacity of the pool (connections divided by hold time) goes down.

See also: `notebook/stage-0/learning/load-testing-science/5. The knee.md`, `4. Little's law.md`, `2. closed vs open load model.md`, `notebook/stage-1/learning/Postgres/2. postgres-write-path.md`, `4. postgres-index-cost.md`, `6. postgres-checkpoints.md`. The hypothesis and predictions are in [Explanation.md](Explanation.md).

## Concepts

**Soak test.** A test that holds a fixed load for a long time. Its question is not "how much can it take" but "does it stay the same". It runs at a load that is clearly below the knee (here about 70% of it), because at the knee any change would look like a collapse and hide slow effects. Example: a service that is fine for 60 seconds and leaks one connection per minute only shows the leak after 25 minutes.

**The knee and headroom.** The knee is the rate where latency starts to bend upward because the busiest resource (here the pool of 25 connections) is nearly full. If I run at 70% of it, there is 30% headroom. A fixed rate cannot show a lower capacity by itself: the accepted rate stays at the offered rate until the capacity falls below it. To see a capacity change I have to measure the capacity itself, once before and once after.

**Open model.** The generator sends at a fixed rate whatever the server does (constant-arrival-rate). When the server gets slower the requests wait longer and the latency grows; the sent rate does not quietly fall. This is why a slower service shows up as latency first.

**Pool as the capacity.** With a pool of N connections that each are held for h seconds per request, the most the service can finish is N / h requests per second (Little's law). The hold time is mostly the INSERT plus the COMMIT wait for the WAL flush. If h grows by 10% the capacity falls by about 9%.

**Shared buffers and the OS page cache.** Postgres keeps pages it uses in its own cache (`shared_buffers`, 128 MB here) and below that the operating system keeps file pages in RAM. A page missing in shared buffers may still be in the OS cache (cheap, microseconds) or has to come from the disk (expensive). The container here has a 2 GB limit, and the OS page cache of the container counts against it. So there are two thresholds: where the hot data passes 128 MB (misses in shared buffers) and where it passes what the OS can keep (real disk reads).

**Append-only heap and random index.** The table is only appended to: new rows go to the last pages, which are always hot. The primary key is a random uuid, so each new entry goes to a random place in the index. A random index has a hot set that is the whole index, so it is the first thing to fall out of cache. The second index (tenant, time) grows at 100 places only (one per tenant) and stays hot. Example: 10 million rows need about 50,000 index pages of 8 KB for the uuid index, about 400 MB: more than 128 MB of shared buffers.

**Checkpoints.** A checkpoint writes all dirty pages to disk and marks a point from where recovery would start. It is started by a timer (`checkpoint_timeout`, 5 minutes) or earlier when the WAL since the last one gets close to `max_wal_size` (1 GB; the real trigger is lower, about `max_wal_size` divided by 2.9, that is about 350 MB, because Postgres counts the spread-out checkpoint too). Postgres spreads the writing over most of the interval (`checkpoint_completion_target`), so a checkpoint is a long, heavy stretch of disk writes, not a single spike. At 2 MB/s of WAL the size trigger comes before the timer.

**Full-page images (FPI).** The first time a page is changed after a checkpoint, Postgres writes the whole 8 KB page into the WAL (to survive a torn page after a crash), not just the small change. With a random index each insert touches a different page, so when the index has more pages than there are inserts between two checkpoints, almost every insert causes an FPI. WAL per row then grows from hundreds of bytes toward thousands, which makes checkpoints come sooner and the disk busier.

**Autovacuum on an insert-only table.** Since Postgres 13 autovacuum also runs after many inserts (not only after updates or deletes), to freeze rows and set visibility bits. It reads the table and writes WAL and competes for the disk. It runs more rarely as the table grows because the trigger is a fraction of the table size.

**Table and index size.** The table (heap) holds rows; each index holds a copy of the indexed columns plus a pointer. Size grows linearly with rows at a fixed rate. Numbers from B7: indexes about 90 to 100 bytes per event, heap about 250 to 550 bytes per event at this payload.

## Signals and what they mean

- **Rows, table and index size:** instantaneous gauges; their slope is the insert rate times bytes per row. A bend in the slope means the rate changed.
- **Checkpoints timed and requested:** cumulative counters, take a difference over time. Timed means the timer fired; requested means the WAL size forced it. Healthy for a steady load: mostly timed. Many requested ones mean `max_wal_size` is too small for the write rate.
- **Buffers written by checkpointer, background writer, backends:** cumulative counters of 8 KB pages. Writes done by backends are a bad sign: the insert itself waited to free a buffer.
- **Buffer cache hit ratio:** hits divided by hits plus reads. It counts shared-buffers only, so a read here can still be an OS cache hit. A fall shows where the hot set outgrew `shared_buffers`; the disk read bytes show the real misses.
- **WAL bytes and full-page images:** cumulative; divide by inserted rows to get a per-row cost. A rise per row over time is the sign of random-index FPIs.
- **INSERT time on the connection:** the server's own measure of the hold time; the best single signal for the pool capacity.
- **Disk busy and read/write bytes:** busy is time with a request in flight; 100% means the disk is the limit.
- **Free disk space:** a soak writes for an hour or more; it is a resource that can run out before the test ends.

## Pitfalls

- A trend that is really a drift of the machine (thermal, other processes, a fuller disk). Defense: the control run on an empty table at the same rate after the soak, and the capacity probe before and after.
- Quoting the average of the whole run. A slow decay and a periodic checkpoint bump are both hidden by one number; read p99 over time and compare the first and last minutes.
- Reading the checkpoint spikes from a scrape that is too coarse. The scrape interval limits how short a window can be.
- Counting rows with `count(*)` during the soak: it scans the table and disturbs the measurement. The sampler uses the statistics estimate and the exact count is taken only after k6 has finished.
- Probes that are above the capacity are not latency measurements: their p99 is queueing time. They only tell the accepted rate.
- Running out of disk during the test, which would end the run and could hurt other containers on the same disk.
- Leftover rows: the table stays big after the experiment, which changes the start of the next experiment unless it truncates first.
