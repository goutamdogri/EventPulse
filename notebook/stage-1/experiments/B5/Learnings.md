# B5 Learnings: what an index costs on every insert

An index is a second copy of some columns, kept sorted, so that Postgres can find rows without reading the whole table. Reading gets faster, but every INSERT now has to put one entry into every index of the table, in the right sorted place, and every one of those changes must also be written to the WAL (the write-ahead log, the file Postgres writes first so it can recover after a crash). "Write amplification" means that one logical event of about 500 bytes causes many more bytes to be written than its own size.

What one INSERT into `events` does, in plain steps:

1. Find or extend a heap page and put the row (the tuple) in it. Write a WAL record for it.
2. For the primary key index: walk the b-tree from the root to the leaf page where the new key belongs, insert the entry, write a WAL record. If the leaf is full, split it into two pages (more WAL).
3. Do the same for every secondary index.
4. At COMMIT, write a commit record and flush the WAL to disk (this wait is the `WALSync` wait event).
5. Later, in the background, the dirty pages are written to the data files by the checkpointer.

See also: [Postgres index cost](../../learning/Postgres/4.%20postgres-index-cost.md), [write path](../../learning/Postgres/2.%20postgres-write-path.md), [checkpoints](../../learning/Postgres/6.%20postgres-checkpoints.md), [wait events](../../learning/Postgres/7.%20postgres-wait-events.md). Hypothesis and predictions: [Explanation.md](Explanation.md).

## Concepts

**B-tree index.** A sorted tree of pages (8 KB each). A leaf page holds entries of the form (key, pointer to the heap row). An insert must find the leaf page for its key; if the page is full it splits. Cost per insert: a few page reads (the upper levels are almost always in cache), one change in the leaf, one WAL record. An entry for a uuid key is about 28 bytes (8 header + 16 key + 4 line pointer), so about 290 entries fit in a page before fill-factor.

**Fill factor and page fill.** A b-tree leaf is filled up to 90 % by default when the keys arrive in order (the right-most page is split so that the left page stays full). When keys arrive in random order, pages split in the middle and settle at about 69 % full (ln 2). Lower fill means more pages for the same rows: a bigger index and more cache needed.

**Random vs time-ordered primary key.** UUIDv4 is 122 random bits, so every new key goes to a random leaf page of the whole index. UUIDv7 starts with the time in milliseconds, so new keys always go to the right-most pages. With v7, the hot part of the index is one or a few pages (always in cache, one full-page image per page after a checkpoint). With v4, the hot part is the whole index: if it does not fit in memory, inserts cause reads; and after a checkpoint every different page that gets touched needs a full-page image. Two v7 ids made in the same millisecond are in random order, so v7 is only ordered at millisecond level.

**Secondary index shapes.** Selectivity matters: an index on `type` with 10 distinct values stores the same key many times; since Postgres 13 a b-tree can deduplicate equal keys into one entry with a list of row pointers, so such an index is much smaller than its row count suggests. A composite index `(tenant_id, type, occurred_at)` has a longer key and sorts by tenant first, so inserts spread over as many hot spots as there are tenants (each tenant's newest time is its own hot spot).

**GIN index on jsonb.** GIN (generalized inverted index) is built for "which rows contain this key or value": it maps each key or value found inside the document to the list of rows that have it. One event with a payload of two fields adds several entries (the keys and the values), so one insert touches several places of the index. To make inserts cheaper, GIN has a "fast update" mode: new entries first go to a small pending list, which is merged into the main index later in a batch (when the list reaches `gin_pending_list_limit`, or on vacuum). The cost is not removed, only delayed: the merge can make a single insert (or a background step) slow.

**WAL and full-page images.** Every change to a page is described in a WAL record. The first time a page is changed after a checkpoint, Postgres logs the whole 8 KB page instead of a small delta (a full-page image, "FPI"), to survive a torn write (a crash in the middle of writing the page). So the WAL volume depends on how many different pages are touched since the last checkpoint, not only on how many rows were inserted. Many inserts into the same page share one image; random inserts into many pages do not.

**Checkpoints.** A checkpoint writes all dirty pages to the data files and marks a point from which recovery can start. It starts because of time (`checkpoint_timeout`) or because the WAL since the last one reached a size limit derived from `max_wal_size`. After it, all pages are "clean again", so the next change to each page logs a new full-page image. A short run right after a manual checkpoint therefore sees the highest FPI rate.

**CPU quota vs cpuset.** The Postgres container is allowed to run on four cores (its cpuset) but only has a quota of 2 CPUs of time. When it uses the quota, it is paused until the next period (throttling). So "capacity" of this setup is the point where the container uses 2 CPUs of work, not 4 cores' worth.

**Open-model load and capacity.** With a constant arrival rate far above what the system can do, the accepted rate settles at the capacity and the rest becomes queueing (and dropped iterations in k6). That makes it a measurement of capacity; at a rate below capacity the accepted rate only equals the offered rate and says nothing about cost, which is why the cost per row is read from WAL bytes and CPU per row there.

## Signals and what they mean

- **WAL position (LSN) and WAL bytes.** Cumulative counters: how many bytes of WAL were generated since a start. Take the difference between the end and the start of a run and divide by the rows added to get bytes per row. `pg_stat_wal` also counts WAL records, full-page images, and the times the WAL buffers were full. These stats are flushed with a small delay, which is why the WAL position (exact) is used for bytes.
- **Full-page images per row.** Cumulative. Higher means more different pages were touched for the first time since the checkpoint.
- **Index and table size.** Instantaneous gauges (`pg_relation_size`, the sum in the exporter). Size per row of the primary key shows page fill: about 31 bytes per row is a tight tree, about 41 is a randomly filled one.
- **Checkpoints timed and requested.** Cumulative counters. "Timed" is caused by the timer, "requested" by WAL size or a manual CHECKPOINT. Buffers written by checkpoints shows how much was flushed.
- **Wait events.** What each session is waiting for right now. `WALWrite`, `WALSync` and `WALInsert` mean the WAL is the bottleneck; `DataFileRead` means a page was not in the cache; `Client` means the session is idle waiting for the application.
- **Buffer cache hit ratio.** Cumulative hits and reads of 8 KB blocks; a read here may still be served from the OS page cache, so a lower ratio is not always a disk read.
- **Container CPU and throttled time.** CPU seconds used and seconds paused by the quota. Both cumulative: take the rate.
- **Dropped iterations (k6).** Arrivals that k6 could not start because every virtual user was busy. A signal of an overloaded server in an `over` run and of an overloaded generator in an `under` run.

## Pitfalls

- Autovacuum can start on a table that just received many inserts and add its own WAL and CPU in the middle of a run. The run has to be flagged if it did.
- After `TRUNCATE` the table is empty and fits in cache: this hides the cost of a random key. That is why a separate probe with a large pre-loaded table exists, and why the fresh-table numbers cannot be used to say "v4 is free".
- The first seconds after a checkpoint have more full-page images than later ones; a short run is dominated by that. Runs are all started right after the same checkpoint so the comparison is fair, but the absolute WAL per row is a short-run number.
- The GIN pending list makes the cost lumpy: latency spikes (p99) and WAL bursts can appear when the list is merged, and a rate computed over a few seconds can look strange.
- The `type` index on a generator that sends one single type is almost free (one distinct value, deduplicated). The generator sends 10 types so that the index does something, but the real cost depends on the real distribution of types, which I do not know.
- WAL bytes per second divided by accepted rps is only valid while a run is going; between runs the ratio is meaningless.
- Measuring changes things: the WAL exporter keeps one extra Postgres session and reads one view per second, and the mid-run capture runs one query. Both are cheap but not zero; probes and heavier queries are never run inside a measured run.
- The Node server has 2 cores. If it saturates before Postgres does, the low index levels will look flat and the experiment measures Node, not the indexes. The CPU panels per group tell which one it is.
- Row audit: if the rows added to `events` differ from the 202 responses counted by k6, the run is not trustworthy.
