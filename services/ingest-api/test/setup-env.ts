import { config } from 'dotenv';
import { resolve } from 'node:path';

config({ path: resolve(import.meta.dirname, '../.env.test'), quiet: true });

// Stage 2: e2e tests need the Kafka cluster (infra/compose/docker-compose.kafka.yml), not Postgres.
// KAFKA_BROKERS defaults to the host listeners localhost:9092,9094,9096 when unset.

const TEST_API_KEY = 'key_a:tenant_a';
const configuredKeys = (process.env.API_KEYS ?? '').split(',').filter(Boolean);
const hasTestKey = configuredKeys.some(
  (pair) => pair.split(':')[0]?.trim() === 'key_a',
);

if (!hasTestKey) {
  process.env.API_KEYS = [...configuredKeys, TEST_API_KEY].join(',');
}
