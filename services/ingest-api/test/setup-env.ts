import { config } from 'dotenv';
import { resolve } from 'node:path';

config({ path: resolve(import.meta.dirname, '../.env.test'), quiet: true });

// Stage 2: e2e tests need the Kafka cluster (infra/compose/docker-compose.kafka.yml), not Postgres.
// KAFKA_BROKERS defaults to the host listeners localhost:9092,9094,9096 when unset.

// Two tenants: the S3-F1 tests check that idempotency keys are scoped by tenant.
const TEST_API_KEYS = ['key_a:tenant_a', 'key_b:tenant_b'];
const configuredKeys = (process.env.API_KEYS ?? '').split(',').filter(Boolean);
for (const pair of TEST_API_KEYS) {
  if (!configuredKeys.some((c) => c.split(':')[0]?.trim() === pair.split(':')[0])) configuredKeys.push(pair);
}
process.env.API_KEYS = configuredKeys.join(',');

// Stage 3: idempotency keys go to Redis database 15, never 0 (the dev database of a running API).
process.env.REDIS_URL ??= 'redis://localhost:6379/15';
if (!/\/1[0-5]$/.test(process.env.REDIS_URL)) {
  throw new Error(`Refusing REDIS_URL=${process.env.REDIS_URL} for e2e tests: use a test database (redis://localhost:6379/15)`);
}

// Stage 3 (S3-F7): the dev registry (schemas registered with `make schemas`). Subjects are read-only for the tests.
process.env.SCHEMA_REGISTRY_URL ??= 'http://localhost:8081';
