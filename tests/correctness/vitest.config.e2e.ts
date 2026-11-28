import { defineConfig } from 'vitest/config';

// Needs the Kafka cluster, Postgres with the test database migrated (`pnpm --filter @eventpulse/consumer-sink
// migrate:test`) and a built consumer-sink (pretest:e2e builds it). Reuses consumer-sink's test env guard, so it
// refuses to run against any database whose name lacks "test".
export default defineConfig({
  test: {
    globals: true,
    include: ['test/**/*.e2e-spec.ts'],
    setupFiles: ['../../services/consumer-sink/test/setup-env.ts'],
    fileParallelism: false, // the files share Kafka and Postgres
    testTimeout: 300_000,
    hookTimeout: 120_000,
  },
});
