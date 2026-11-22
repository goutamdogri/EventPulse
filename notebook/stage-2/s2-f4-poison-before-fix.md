# S2-F4 part 1: a poison message blocks its partition (before the fix)

*2026-11-22. Consumer at the S2-F3 commit (`894d7fd`): a record that can't be parsed makes `eachBatch` throw. The client then seeks back and hands the same batch over again, forever.*

## Setup

- Real ingest-api with `KAFKA_KEY_MODE=event`, so the load spreads over all 6 partitions of `events.raw`. One real consumer-sink, group `sink-postgres`, default settings.
- Steady load: 200 req/s for 60 s (`11,874 × 202`).
- At t=10 s, one record that is not valid JSON was written straight into `events.raw`, bypassing the API:
  `scripts/produce-poison.sh --kind invalid-json --key poison-observe-1`. The key `poison-observe-1` hashes to **partition 5**.

## What happened

`kafka-consumer-groups.sh --describe --group sink-postgres`:

```
=== t=10s, before poison ===
PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG
0          1385            1385            0
1          1428            1429            1
2          1415            1416            1
3          1424            1425            1
4          1437            1437            0
5          2745            2745            0

=== t=30s, 20s after poison ===
0          2269            2273            4
1          2281            2282            1
2          2357            2360            3
3          2356            2360            4
4          2371            2373            2
5          2893            3653            760      <- stuck at offset 2893 (the poison record)

=== t=55s, 45s after poison ===
0          2910            2910            0
1          2891            2891            0
2          3006            3006            0
3          3018            3018            0
4          3043            3043            0
5          2893            4309            1416     <- still 2893; lag grows with the arrival rate

=== load stopped + 10s ===
5          2893            4309            1416     <- never recovers on its own
```

Consumer `/debug/stats` and log:

```
t=30s   consumed 272,814   inserted 7,311    batches 6,150   batch_errors 737
end     consumed 787,810   inserted 10,459   batches 8,991   batch_errors 1,274
last_error: 1 unparseable record(s); first at partition 5 offset 2893: invalid_json (Unexpected token 'b' ...)
log: 1,274 x "Consumer encountered error while processing message ... The same message may be reprocessed."
```

## Observations

1. **Only the poison's partition stops.** Partition 5's committed offset froze at 2893, exactly the poison record. Partitions 0-4 kept draining to lag 0 the whole time.
2. **Lag on that partition grows linearly with arrival** (760 after 20 s, 1,416 after 45 s, about 1/6 of 200 req/s) and stays there after the load stops. Nothing on that partition after the poison reaches Postgres: those events got a 202 but are invisible.
3. **It's a hot retry loop, not a crash loop.** The client seeks back and redelivers the same batch immediately, about 28 times per second. `consumed` reached 787,810 for 11,874 real events. The handler re-reads the chunk each time, so the counter keeps inflating. The CPU spent re-parsing the same messages is pure waste, and the error log grows by one long line per retry.
4. **The chunks before the poison in the same batch were committed** (per-chunk commits from S2-F3), so the partition is blocked exactly at the bad record, not at the start of its batch.
5. Nothing alerts on this today. From the outside it looks like "one partition is slow". Lag per partition (S2-F6) is the signal.

## Why this is serious even though only one partition stops

- Every event keyed to that partition (with `tenant_id` keys: every event of the tenants that hash there) stops being stored. Visibility for them is no longer "eventual". They are lost from the user's point of view until a human acts.
- If the consumer is down longer than retention (24 h), the stuck events after the poison are deleted unread (K11).
- One bad producer, or one record written by a buggy tool, is enough. The edge validation in the API doesn't help, because the record didn't come through the API.

## What the fix must do (S2-F4 part 2)

- Classify failures. A record that fails **on its own** (can't be parsed, or the DB rejects that row) goes to `events.dlq` with its original bytes and the error, then the offset moves past it.
- A database that is **down** must never send records to the DLQ. It must be retried with backoff and pause the partition, without a hot loop.
- Retries are bounded in count and time; the partition always moves forward.

Note: the real `sink-postgres` group is left stuck at partition 5 offset 2893. Restarting the consumer with the fix should drain it (the "partition drains after poison" check, on real data).

## After the fix (same day)

Restarted the consumer with S2-F4 (classify, retry with backoff, isolate, dead-letter) on the same stuck group:

```
before restart:  partition 5   committed 2893   end 4309   lag 1416
after ~1 s:      partition 5   committed 4309   end 4309   lag 0
consumer:        consumed 1416, inserted 1415, dlq 1 (PERMANENT_PARSE), batch_errors 0, pauses 0
```

`scripts/dlq-peek.sh` shows the record in `events.dlq` with its original key (`poison-observe-1`), its original bytes
(`{"event_id": "...", "type": broken json`), its original headers, and `dlq.original_partition:5`,
`dlq.original_offset:2893`, `dlq.error_class:PERMANENT_PARSE`, `dlq.error_code:invalid_json`, `dlq.attempts:1`.
