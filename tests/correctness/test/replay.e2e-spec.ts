// S3-F2 / G3: replaying any range of the log leaves stored data unchanged.
//
// A real consumer-sink process stores N events; then the group's offsets are moved back (to the start, and to the
// middle of every partition) and the same log is consumed again. The stored data's fingerprint (every column a
// duplicate could change, stored_at included) must not move, and every replayed record must be COUNTED as a
// duplicate (S3-F3: never silently swallowed).
import { randomUUID } from 'node:crypto';
import {
  StoreProbe,
  createTopic,
  deleteTopic,
  fullyCommitted,
  groupOffsets,
  produceEvents,
  resetGroupOffsets,
  spawnSink,
  waitFor,
} from '../src/harness.js';
import type { SinkProcess } from '../src/harness.js';

const N = 3000;

describe('S3-F2 replay safety (real process, real log)', () => {
  const store = new StoreProbe();
  const tenant = `replay_${randomUUID().slice(0, 8)}`;
  const group = `replay-${randomUUID()}`;
  let topic: string;
  const procs: SinkProcess[] = [];

  /** One full pass: start the sink, wait until every partition is committed to the log end, stop it cleanly. */
  async function pass(label: string) {
    const p = spawnSink({ CONSUMER_TOPIC: topic, CONSUMER_GROUP_ID: group, SINK_MAX_ROWS: '500' });
    procs.push(p);
    try {
      await waitFor(async () => (await store.countTenant(tenant)) === N && (await fullyCommitted(group, topic)), 90_000, `${label}: all offsets committed`);
      const st = await p.stats();
      return { consumed: st.consumed as number, inserted: st.inserted as number, duplicates: st.duplicates as number, conflicts: st.conflicting_duplicates as number };
    } finally {
      const exit = await p.stop();
      expect(exit, p.log.join('').slice(-2000)).toEqual({ code: 0, signal: null });
    }
  }

  beforeAll(async () => {
    topic = await createTopic(6);
  });

  afterAll(async () => {
    await Promise.all(procs.map((p) => p.stop()));
    await deleteTopic(topic);
    await store.cleanup(tenant, group);
    await store.end();
  });

  it('[G3.triple-replay] the same log consumed 3 times: storage unchanged, duplicates = 2N', async () => {
    await produceEvents(topic, N, { tenant });
    const first = await pass('pass 1');
    expect(first).toMatchObject({ consumed: N, inserted: N, duplicates: 0 });
    const original = await store.fingerprint(tenant);
    expect(original).toMatchObject({ rows: N, distinct: N });
    const ends = Object.fromEntries((await groupOffsets(group, topic)).map((p) => [p.partition, p.end - 1]));
    expect(await store.sinkOffsets(group, topic)).toEqual(Object.fromEntries(Object.entries(ends).filter(([, o]) => o >= 0)));

    let duplicates = 0;
    for (const label of ['pass 2', 'pass 3']) {
      await resetGroupOffsets(group, topic, 'earliest');
      const r = await pass(label);
      expect(r).toMatchObject({ consumed: N, inserted: 0, duplicates: N, conflicts: 0 });
      duplicates += r.duplicates;
      expect(await store.fingerprint(tenant)).toEqual(original);
    }
    expect(duplicates).toBe(2 * N);
    console.info(`[G3.triple-replay] ${N} events x 3 passes: rows ${N}, duplicates counted ${duplicates}, fingerprint unchanged`);
  });

  it('[G3.range-replay] replaying from the middle of every partition: storage unchanged, duplicates = replayed records', async () => {
    const original = await store.fingerprint(tenant);
    const offsets = await groupOffsets(group, topic);
    const mid = Object.fromEntries(offsets.map((p) => [p.partition, Math.floor(p.end / 2)]));
    const replayed = offsets.reduce((a, p) => a + (p.end - mid[p.partition]), 0);
    await resetGroupOffsets(group, topic, mid);
    const r = await pass('range pass');
    expect(r).toMatchObject({ consumed: replayed, inserted: 0, duplicates: replayed });
    expect(await store.fingerprint(tenant)).toEqual(original);
    // sink_offsets never moves back: replaying lower offsets keeps the highest stored one.
    expect(await store.sinkOffsets(group, topic)).toEqual(Object.fromEntries(offsets.filter((p) => p.end > 0).map((p) => [p.partition, p.end - 1])));
  });
});
