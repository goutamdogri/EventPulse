import { defineConfig } from 'vitest/config';

// Unit tests of the suite itself (report rendering, scenario coverage). No infrastructure needed.
export default defineConfig({
  test: { globals: true, include: ['src/**/*.spec.ts', 'test/**/*.spec.ts'], exclude: ['**/*.e2e-spec.ts'] },
});
