// G4 ordering (S3-F9): events with the same key (the tenant, KAFKA_KEY_MODE=tenant) are in one partition, in the
// order the API acknowledged them. No global order across keys. (The exception, a retried event stored after later
// ones, is proven by [G5.retry-late] in event-time.e2e-spec.ts.)
import { randomUUID } from 'node:crypto';
import { EventSerde, RegistryClient, SUBJECT_EVENTS_RAW, latestSchema } from '@eventpulse/schema';
import { createTopic, deleteTopic, readTopic } from '../src/harness.js';
import { postEvent, spawnApi, waitApiReady } from '../src/api-process.js';

describe('G4 ordering', () => {
  it('[G4.per-key-order] two tenants interleaved, sequence numbers in the payload, read from the log: per key, one partition and offsets in sequence order', async () => {
    const topic = await createTopic(6);
    const id = randomUUID().slice(0, 8);
    const tenants = [`order_a_${id}`, `order_b_${id}`];
    const keys = tenants.map(() => `k_${randomUUID()}`);
    const api = spawnApi({ KAFKA_TOPIC: topic, API_KEYS: tenants.map((t, i) => `${keys[i]}:${t}`).join(','), IDEM_KEY_PREFIX: `or${id}` });
    try {
      await waitApiReady(api);
      const N = 200;
      // Each tenant's events are sent one after the other (the next once the previous got its 202); the two
      // tenants run concurrently, so their records interleave in time.
      await Promise.all(
        tenants.map(async (_t, ti) => {
          for (let seq = 0; seq < N; seq++) {
            const r = await postEvent(api, keys[ti], { event_id: randomUUID(), type: 'order', occurred_at: new Date().toISOString(), schema_version: 1, payload: { seq } });
            expect(r.status).toBe(202);
          }
        }),
      );
      const recs = await readTopic(topic, 30_000, (rs) => rs.length >= 2 * N);
      expect(recs).toHaveLength(2 * N);
      const serde = new EventSerde(new RegistryClient({ url: process.env.SCHEMA_REGISTRY_URL ?? 'http://localhost:8081', timeoutMs: 5000 }), {
        subject: SUBJECT_EVENTS_RAW,
        schema: latestSchema(SUBJECT_EVENTS_RAW).parsed,
      });
      await serde.prefetch(recs.map((r) => r.value));
      for (const t of tenants) {
        const mine = recs.filter((r) => r.key === t).map((r) => ({ partition: r.partition, offset: Number(r.offset), seq: Number(serde.decode(r.value!).payload.seq) }));
        expect(mine).toHaveLength(N);
        expect(new Set(mine.map((m) => m.partition)).size).toBe(1); // one key, one partition
        const byOffset = [...mine].sort((a, b) => a.offset - b.offset).map((m) => m.seq);
        expect(byOffset).toEqual(Array.from({ length: N }, (_, i) => i)); // log order == acknowledgement order
      }
    } finally {
      await api.stop();
      await deleteTopic(topic);
    }
  });
});
