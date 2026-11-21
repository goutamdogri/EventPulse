# B7 Learnings

When an event gets bigger, nothing in the system gets cheaper per request, and almost everything gets more expensive per byte. This note collects the concepts I need to read B7: where a payload's bytes are copied, how Postgres stores a big jsonb value (TOAST), what WAL has to write for it, and how I tell which resource is the limit. The cycle of one `POST /v1/events`:

1. k6 builds the JSON text and sends it over a socket.
2. Node reads the bytes into a buffer, the body parser turns them into a JS object (JSON parse), and the validator checks it.
3. The pg driver turns the payload object into JSON text again and sends it to Postgres in an INSERT.
4. The Postgres backend parses that text into the binary jsonb format and builds the row.
5. If the row is too big, Postgres compresses it or moves big values into the TOAST table, in chunks.
6. Every change is described in a WAL record, and COMMIT waits for the WAL to be flushed.
7. Later, a checkpoint writes the changed table pages to disk.
8. Node answers 202.

See also: [postgres-write-path](../../learning/Postgres/2.%20postgres-write-path.md), [postgres-index-cost](../../learning/Postgres/4.%20postgres-index-cost.md), [postgres-checkpoints](../../learning/Postgres/6.%20postgres-checkpoints.md), [node-pg-pool](../../learning/nodeAndNest/1.%20node-pg-pool.md), and [Explanation.md](Explanation.md) for the hypothesis and predictions.

## Concepts

**Fixed cost vs per-byte cost.** A request has a cost that does not depend on its size (accepting the connection, parsing the HTTP headers, authentication, taking a pool connection, the COMMIT flush) and a cost that grows with every byte (copying, JSON parse, WAL, disk). With small events the fixed part hides the per-byte part. The size where the two are equal is the "knee". Example: if the fixed part is 0.17 ms and each KB costs 0.004 ms, the two are equal at about 40 KB.

**JSON parse and serialize.** Node parses the body once and the pg driver serializes the payload once more, so a payload is turned into text twice and into an object once. Both are CPU work on the single JavaScript thread. A string is cheap to parse compared with an object with many fields (it is scanned and copied), so a payload of one big string is the friendly case, and a payload with many small fields would cost more per byte.

**Bytes copied.** The same bytes are copied several times: the kernel socket buffer, the Node buffer, the JS string, the serialized string, the socket to Postgres, the Postgres buffers. On one machine this is memory bandwidth and CPU system time, not a real network. A real network would add latency and a bandwidth limit (1 Gbit/s is about 125 MB/s), which is smaller than what the WAL needs at 100 KB events.

**Row size and the 8 kB page.** Postgres stores rows in 8 kB pages. A row has to fit in a page, and Postgres wants at least 4 rows per page, which gives the TOAST threshold of about 2 kB (`toast_tuple_target`, 2032 bytes by default). Rows above it are shrunk by compressing and then by moving big columns out of line.

**TOAST (The Oversized-Attribute Storage Technique).** Big values (jsonb, text, bytea) of a row are first compressed (the default method is pglz; lz4 is the alternative), and if the row is still too big the value is cut into chunks of about 2 kB and stored in a separate table, the TOAST table of the main table, which has its own index. The main row keeps only an 18 byte pointer. A write therefore costs one insert in the main table plus one insert per chunk plus one index entry per chunk. A 100 KB value is about 52 chunks. Reading a TOASTed column costs extra reads, but this experiment only writes.

**Compression and data that cannot be compressed.** pglz is a simple back-reference compressor (it finds repeated byte sequences, with no entropy coding). Postgres keeps the compressed copy only if it saves at least 25%. A value made of one repeated character shrinks to almost nothing, a value of random hex digits barely shrinks, because every 4 bits of information are stored in a byte but pglz cannot pack them. This is why the test payload is random hex: the filler has to be incompressible so the bytes sent are the bytes stored. Real payloads (names, ids, repeated keys) compress better than my test data, so my result is the pessimistic case for storage.

**WAL (write-ahead log) volume.** Every change goes into the WAL before it counts. The size of the WAL for one event is the row (or the TOAST chunks) plus index records plus a commit record plus some full-page images. A full-page image (FPI) is a full 8 kB copy of a page written the first time the page changes after a checkpoint, to protect against torn pages. WAL bytes per event is the number that links payload size to disk cost: WAL per second = events per second x WAL per event. The WAL is written once to the WAL files and a second time as table pages at the next checkpoint.

**What limits throughput: queueing law.** Whichever resource saturates first sets the capacity: Node's single thread (capacity = 1 / CPU time per request), the pool (capacity = pool size / hold time per request, Little's Law), Postgres CPU, or the disk (capacity = disk speed / bytes per event). For small events it is the pool plus the commit flush; for big events I expect another resource to take over, and that switch is what the experiment looks for.

## Signals and what they mean

- **Accepted rps (202 per second):** the events the server promised to store. In an open-model run above capacity, the plateau is the capacity.
- **Dropped iterations (k6):** arrivals k6 could not start because every virtual user was busy waiting. Expected above capacity; a warning below it.
- **WAL position (LSN) difference:** cumulative, a difference between two moments gives the bytes written in the run. Dividing by the rows inserted gives WAL per event.
- **Full-page images and WAL records:** cumulative counters; FPI count rises after every checkpoint.
- **Heap, TOAST and index size:** instantaneous sizes of the relation files. Because the table is emptied before each run, the size after the run is the growth. The TOAST size is zero while the rows stay below the threshold.
- **Pool in use / waiting, pool wait time:** how many of the 25 connections are busy and how long callers queue. High use with low Node CPU says the pool or Postgres is the limit.
- **Event loop lag and CPU of the server's pinned CPUs:** if one of the two pinned CPUs is full and the event loop lags, Node is the limit.
- **Disk busy and disk write rate:** if the disk is busy near 100% while Node and Postgres CPUs have room, storage is the limit.
- **Requested checkpoints:** a checkpoint forced because the WAL reached `max_wal_size`. Many of them with big events mean frequent bursts of page writes and more full-page images.

## Pitfalls

- **Compressible filler hides the cost.** A payload of repeated characters would be compressed by pglz and the WAL would be tiny. The filler must be random.
- **The body limit.** The server rejects bodies above `BODY_LIMIT` (64 kb default) with 413. A 100 KB run at the default would measure the error path, not the insert.
- **The body size is about the target.** The type and the user id change the JSON length by a few bytes.
- **The generator can become the limit.** Building and sending 100 KB strings costs k6 CPU; if k6's pinned CPUs are busy above about 70% the result is the generator's limit.
- **Disk fills fast.** 100 KB events at about 1,700 per second write several GB per run; the table is truncated between runs and at the end.
- **Sizes and WAL are cumulative or per-run.** WAL and the TOAST size must be read as a difference between two snapshots or on an empty table, and divided by the events inserted in the same window; autovacuum or a late checkpoint can add a little.
- **Rows still in flight.** Requests finishing after k6 stops add rows; the script waits for Postgres to go quiet before it counts.
- **Localhost network.** The experiment cannot show the cost of a real network for big bodies.
