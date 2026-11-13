import { config } from 'dotenv';
import { resolve } from 'node:path';

config({ path: resolve(import.meta.dirname, '../.env.test'), quiet: true });

if (!process.env.DATABASE_URL) {
  throw new Error(
    'DATABASE_URL is required for e2e tests. Copy .env.test.example to .env.test and set DATABASE_URL.',
  );
}

const TEST_API_KEY = 'key_a:tenant_a';
const configuredKeys = (process.env.API_KEYS ?? '').split(',').filter(Boolean);
const hasTestKey = configuredKeys.some(
  (pair) => pair.split(':')[0]?.trim() === 'key_a',
);

if (!hasTestKey) {
  process.env.API_KEYS = [...configuredKeys, TEST_API_KEY].join(',');
}
