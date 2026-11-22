import { config, parse } from 'dotenv';
import { existsSync, readFileSync } from 'node:fs';
import { resolve } from 'node:path';

// e2e tests need Postgres (a SEPARATE test database, migrations applied through 0004: `pnpm migrate:test`)
// and the Kafka cluster (infra/compose/docker-compose.kafka.yml; KAFKA_BROKERS defaults to localhost:9092,9094,9096).
config({ path: resolve(import.meta.dirname, '../.env.test'), quiet: true });

const url = process.env.DATABASE_URL;
if (!url) {
  throw new Error('DATABASE_URL is required for e2e tests. Copy .env.test.example to .env.test and set it.');
}

// Guard: the tests write hundreds of thousands of rows. They must never run against the dev/benchmark database.
const dbName = new URL(url).pathname.replace(/^\//, '');
const devEnv = resolve(import.meta.dirname, '../.env');
const devUrl = existsSync(devEnv) ? parse(readFileSync(devEnv)).DATABASE_URL : undefined;
if (!/test/i.test(dbName) || (devUrl && new URL(devUrl).pathname === new URL(url).pathname && new URL(devUrl).host === new URL(url).host)) {
  throw new Error(
    `Refusing to run e2e tests against database "${dbName}": use a separate test database whose name contains "test" ` +
      '(e.g. eventpulse_test; create it and run `pnpm migrate:test`).',
  );
}

process.env.LOG_ENABLED ??= 'false';
process.env.KAFKA_LOG_LEVEL ??= 'error';
