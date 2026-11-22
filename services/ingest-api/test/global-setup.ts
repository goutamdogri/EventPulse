// Runs once per e2e run: creates a throw-away topic so the tests never write into events.raw
// (whatever lands in events.raw is later stored in the dev database by the real sink-postgres group).
import { randomUUID } from 'node:crypto';
import { createKafka, loadKafkaClientConfig, waitTopicReady } from '@eventpulse/kafka-config';

export default async function setup() {
  const topic = `ingest-e2e-${randomUUID()}`;
  const admin = createKafka({ ...loadKafkaClientConfig(process.env, 'ingest-api-e2e-setup'), logLevel: 'error' }).admin();
  await admin.connect();
  await admin.createTopics({
    topics: [{ topic, numPartitions: 6, replicationFactor: 3, configEntries: [{ name: 'min.insync.replicas', value: '2' }] }],
  });
  await waitTopicReady(admin, topic, 6); // producing into a brand-new topic lost acked records (notebook/fix/step-1.md 1.4)
  process.env.KAFKA_TOPIC = topic; // inherited by the test workers and by spawned API processes
  return async () => {
    await admin.deleteTopics({ topics: [topic] }).catch(() => undefined);
    await admin.disconnect();
  };
}
