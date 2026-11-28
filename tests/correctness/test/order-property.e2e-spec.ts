// S3-F8 / G5: late and out-of-order events are stored correctly. Property tests (fast-check) against a reference
// model: a Map keyed by (tenant_id, event_id), FIRST DELIVERED WINS.
//
// 1. The real BatchWriter against the test database, >= 1,000 runs (PROPERTY_RUNS=10000 for a nightly run): a set of
//    events with random lateness (0 s .. 24 h), exact AND conflicting duplicates mixed in, shuffled, split into
//    random chunks. Stored set == reference; per-tenant counts and min/max(occurred_at) == reference; counted
//    duplicates == generated duplicates; conflicts reported == conflicting copies.
// 2. The same idea through the whole pipeline (API -> Kafka -> sink), fewer runs (PIPELINE_PROPERTY_RUNS, 20):
//    shuffled, late, exact resends mixed in, sent concurrently in random groups.
//
// Each run uses its own tenants (no truncation needed); everything is deleted at the end.
import { randomUUID } from 'node:crypto';
import fc from 'fast-check';
import { eventContentHash } from '@eventpulse/idempotency';
import { BatchWriter } from '../../../services/consumer-sink/src/sink/batch-writer.js';
import type { EventRow } from '../../../services/consumer-sink/src/sink/parse.js';
import { StoreProbe, createTopic, deleteTopic, spawnSink, waitFor, waitReady } from '../src/harness.js';
import type { SinkProcess } from '../src/harness.js';
import { postEvent, spawnApi, waitApiReady } from '../src/api-process.js';
import type { ApiProcess } from '../src/api-process.js';

const RUNS = Number(process.env.PROPERTY_RUNS ?? 1000);
const PIPELINE_RUNS = Number(process.env.PIPELINE_PROPERTY_RUNS ?? 20);
const H = 3_600_000;

interface Ev {
  tenant_id: string;
  event_id: string;
  type: string;
  occurred_at: string;
  received_at: string;
  payload: Record<string, unknown>;
}

const contentKey = (e: Ev) => `${e.tenant_id}|${e.event_id}`;
const fingerprint = (e: { type: string; occurred_at: string; received_at: string; payload: unknown }) =>
  JSON.stringify([e.type, e.occurred_at, e.received_at, e.payload]);

function toRow(e: Ev, offset: number): EventRow {
  return {
    event_id: e.event_id,
    tenant_id: e.tenant_id,
    type: e.type,
    occurred_at: e.occurred_at,
    schema_version: 1,
    payload: JSON.stringify(e.payload),
    received_at: e.received_at,
    content_hash: eventContentHash({ tenant_id: e.tenant_id, event_id: e.event_id, type: e.type, occurred_at: e.occurred_at, schema_version: 1, payload: e.payload }),
    partition: 0,
    offset: String(offset),
  };
}

/** One generated case: the delivered sequence (shuffled, with duplicates) and how it is chunked. */
const caseArb = (now: number) =>
  fc
    .record({
      tenants: fc.integer({ min: 1, max: 3 }),
      base: fc.array(
        fc.record({
          tenant: fc.nat({ max: 2 }),
          lateMs: fc.integer({ min: 0, max: 24 * H }), // lateness 0 s .. 24 h
          receivedLagMs: fc.integer({ min: 0, max: 5_000 }),
          n: fc.integer({ min: -1_000_000, max: 1_000_000 }),
          s: fc.string({ maxLength: 12 }),
        }),
        { minLength: 1, maxLength: 30 },
      ),
      dups: fc.array(fc.record({ of: fc.nat(), conflicting: fc.boolean() }), { maxLength: 20 }),
      order: fc.array(fc.double({ min: 0, max: 1, noNaN: true }), { minLength: 50, maxLength: 50 }),
      cuts: fc.array(fc.integer({ min: 1, max: 12 }), { minLength: 1, maxLength: 20 }),
    })
    .map((c) => {
      const run = randomUUID().slice(0, 8);
      const tenantIds = Array.from({ length: c.tenants }, (_, i) => `prop_${run}_${i}`);
      const base: Ev[] = c.base.map((b) => ({
        tenant_id: tenantIds[b.tenant % c.tenants],
        event_id: randomUUID(),
        type: 'prop.event',
        occurred_at: new Date(now - b.lateMs).toISOString(),
        received_at: new Date(now + b.receivedLagMs).toISOString(),
        payload: { n: b.n, s: b.s },
      }));
      const copies: Ev[] = c.dups.map((d) => {
        const src = base[d.of % base.length];
        // An exact copy (redelivery, replay, client resend) or a conflicting one (a reused event_id: producer bug).
        return d.conflicting ? { ...src, payload: { ...src.payload, conflict: d.of } } : { ...src };
      });
      const all = [...base, ...copies];
      const delivered = all.map((e, i) => ({ e, k: c.order[i % c.order.length] + i * 1e-9 })).sort((a, b) => a.k - b.k).map((x) => x.e);
      const chunks: Ev[][] = [];
      for (let i = 0, j = 0; i < delivered.length; j++) {
        const size = c.cuts[j % c.cuts.length];
        chunks.push(delivered.slice(i, i + size));
        i += size;
      }
      return { tenantIds, delivered, chunks, generatedDuplicates: copies.length };
    });

/**
 * Reference model: first delivered copy of a key wins. Conflicts: a copy whose content differs from the stored
 * winner, reported once per (key, content) per chunk (what the sink's conflict check counts per write).
 */
function reference(chunks: Ev[][]) {
  const m = new Map<string, Ev>();
  for (const e of chunks.flat()) if (!m.has(contentKey(e))) m.set(contentKey(e), e);
  let conflicts = 0;
  for (const ch of chunks) {
    const seen = new Set<string>();
    for (const e of ch) {
      const k = `${contentKey(e)}|${JSON.stringify(e.payload)}`;
      if (fingerprint(e) !== fingerprint(m.get(contentKey(e))!) && !seen.has(k)) {
        seen.add(k);
        conflicts++;
      }
    }
  }
  return { rows: m, conflicts };
}

describe('S3-F8 property: order and lateness do not change what is stored', () => {
  const store = new StoreProbe();
  const tenants: string[] = [];
  afterAll(async () => {
    for (let i = 0; i < tenants.length; i += 500) await store.pool.query('delete from events where tenant_id = any($1)', [tenants.slice(i, i + 500)]);
    await store.end();
  });

  it(`[G5.property] BatchWriter, ${RUNS} runs: stored set == first-wins reference; counts, min/max(occurred_at), duplicates and conflicts match`, async () => {
    const writer = new BatchWriter(store.pool as any);
    const now = Date.now();
    let checkedRows = 0;
    await fc.assert(
      fc.asyncProperty(caseArb(now), async (c) => {
        tenants.push(...c.tenantIds);
        let attempted = 0;
        let inserted = 0;
        let conflicts = 0;
        let offset = 0;
        for (const ch of c.chunks) {
          const w = await writer.writeChunk(ch.map((e) => toRow(e, offset++)));
          attempted += w.attempted;
          inserted += w.inserted;
          conflicts += w.conflicts?.length ?? 0;
        }
        const ref = reference(c.chunks);
        const stored = await store.pool.query(
          `select tenant_id, event_id::text, type, occurred_at, received_at, payload from events where tenant_id = any($1)`,
          [c.tenantIds],
        );
        // Same set, same content (first delivered wins), occurred_at/received_at to the millisecond.
        expect(stored.rows.length).toBe(ref.rows.size);
        for (const r of stored.rows) {
          const want = ref.rows.get(`${r.tenant_id}|${r.event_id}`);
          expect(want, `unexpected row ${r.tenant_id}/${r.event_id}`).toBeDefined();
          expect(fingerprint({ type: r.type, occurred_at: r.occurred_at.toISOString(), received_at: r.received_at.toISOString(), payload: r.payload })).toBe(fingerprint(want!));
        }
        // Per tenant: count and event-time range.
        const agg = await store.pool.query(
          `select tenant_id, count(*)::int n, min(occurred_at) lo, max(occurred_at) hi from events where tenant_id = any($1) group by 1`,
          [c.tenantIds],
        );
        for (const a of agg.rows) {
          const mine = [...ref.rows.values()].filter((e) => e.tenant_id === a.tenant_id).map((e) => Date.parse(e.occurred_at));
          expect(a.n).toBe(mine.length);
          expect(a.lo.getTime()).toBe(Math.min(...mine));
          expect(a.hi.getTime()).toBe(Math.max(...mine));
        }
        // Every generated duplicate was absorbed and counted; every conflicting copy reported (never overwritten).
        expect(attempted - inserted).toBe(c.generatedDuplicates);
        expect(conflicts).toBe(ref.conflicts);
        checkedRows += stored.rows.length;
      }),
      { numRuns: RUNS, endOnFailure: true },
    );
    expect(checkedRows).toBeGreaterThan(RUNS); // the property really looked at rows
  }, 600_000);
});

describe('S3-F8 property through the pipeline (API -> Kafka -> sink)', () => {
  const store = new StoreProbe();
  const tenant = `propipe_${randomUUID().slice(0, 8)}`;
  const apiKey = `k_${randomUUID()}`;
  let topic: string;
  let api: ApiProcess;
  let sink: SinkProcess;

  beforeAll(async () => {
    topic = await createTopic(3);
    api = spawnApi({ KAFKA_TOPIC: topic, API_KEYS: `${apiKey}:${tenant}`, IDEM_KEY_PREFIX: `pp${randomUUID().slice(0, 6)}` });
    sink = spawnSink({ CONSUMER_TOPIC: topic, CONSUMER_GROUP_ID: `propipe-${randomUUID().slice(0, 8)}`, SINK_MAX_ROWS: '50' });
    await Promise.all([waitApiReady(api), waitReady(sink)]);
  });
  afterAll(async () => {
    await Promise.all([api?.stop(), sink?.stop()]);
    await deleteTopic(topic);
    await store.cleanup(tenant);
    await store.end();
  });

  it(`[G5.property-pipeline] ${PIPELINE_RUNS} runs: shuffled, late (0 s .. 24 h), resends mixed in, concurrent sends: stored == sent, received_at = server time`, async () => {
    await fc.assert(
      fc.asyncProperty(
        fc.record({
          events: fc.array(fc.record({ lateMs: fc.integer({ min: 0, max: 24 * H }), n: fc.integer(), offsetMin: fc.constantFrom(0, 60, -300, 330) }), { minLength: 1, maxLength: 40 }),
          resends: fc.array(fc.nat(), { maxLength: 15 }),
          order: fc.array(fc.double({ min: 0, max: 1, noNaN: true }), { minLength: 60, maxLength: 60 }),
          group: fc.integer({ min: 1, max: 10 }),
        }),
        async (c) => {
          const now = Date.now();
          const sent = c.events.map((x) => {
            const t = new Date(now - x.lateMs);
            // The same instant written in another zone offset: stored as UTC (millisecond precision).
            const local = new Date(t.getTime() + x.offsetMin * 60_000).toISOString().replace('Z', '');
            const sign = x.offsetMin >= 0 ? '+' : '-';
            const off = `${sign}${String(Math.floor(Math.abs(x.offsetMin) / 60)).padStart(2, '0')}:${String(Math.abs(x.offsetMin) % 60).padStart(2, '0')}`;
            return { body: { event_id: randomUUID(), type: 'prop.pipe', occurred_at: `${local}${off}`, schema_version: 1, payload: { n: x.n } }, utc: t.toISOString() };
          });
          const delivery = [...sent, ...c.resends.map((r) => sent[r % sent.length])]
            .map((e, i) => ({ e, k: c.order[i % c.order.length] + i * 1e-9 }))
            .sort((a, b) => a.k - b.k)
            .map((x) => x.e);
          const t0 = Date.now();
          for (let i = 0; i < delivery.length; i += c.group) {
            // A resend racing its original gets 409 (still in progress): the client retries, as the contract says.
            const send = async (body: object) => {
              for (let attempt = 0; attempt < 50; attempt++) {
                const r = await postEvent(api, apiKey, body);
                if (r.status !== 409) return r.status;
                await new Promise((res) => setTimeout(res, 20));
              }
              return 409;
            };
            const rs = await Promise.all(delivery.slice(i, i + c.group).map((e) => send(e.body)));
            for (const r of rs) expect(r).toBe(202);
          }
          const t1 = Date.now();
          const ids = sent.map((e) => e.body.event_id);
          await waitFor(async () => Number((await store.pool.query('select count(*) from events where tenant_id = $1 and event_id = any($2)', [tenant, ids])).rows[0].count) === ids.length, 30_000, 'pipeline stored the run');
          const rows = (await store.pool.query('select event_id::text, occurred_at, received_at, payload from events where tenant_id = $1 and event_id = any($2)', [tenant, ids])).rows;
          const byId = new Map(rows.map((r) => [r.event_id, r]));
          for (const e of sent) {
            const r = byId.get(e.body.event_id)!;
            expect(r.occurred_at.toISOString()).toBe(e.utc);
            expect(r.payload).toEqual(e.body.payload);
            expect(r.received_at.getTime()).toBeGreaterThanOrEqual(t0 - 5); // server time of the request, not the client's
            expect(r.received_at.getTime()).toBeLessThanOrEqual(t1 + 5);
          }
        },
      ),
      { numRuns: PIPELINE_RUNS, endOnFailure: true },
    );
  }, 600_000);
});
