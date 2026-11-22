import { config } from 'dotenv';
import { resolve } from 'node:path';

// e2e tests need Postgres (DATABASE_URL, migrations applied through 0004) and the Kafka cluster
// (infra/compose/docker-compose.kafka.yml; KAFKA_BROKERS defaults to localhost:9092,9094,9096).
config({ path: resolve(import.meta.dirname, '../.env.test'), quiet: true });

if (!process.env.DATABASE_URL) {
  throw new Error('DATABASE_URL is required for e2e tests. Copy .env.test.example to .env.test and set it.');
}
process.env.LOG_ENABLED ??= 'false';
process.env.KAFKA_LOG_LEVEL ??= 'error';
