import { defineConfig } from 'vitest/config';

export default defineConfig({
  resolve: { tsconfigPaths: true },
  test: {
    globals: true,
    root: './',
    include: ['**/*.e2e-spec.ts'],
    setupFiles: ['./test/setup-env.ts'],
    fileParallelism: false, // the e2e files share Kafka and Postgres
    testTimeout: 60_000,
    hookTimeout: 60_000,
  },
});
