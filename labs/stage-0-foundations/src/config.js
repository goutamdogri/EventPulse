// Environment-based configuration.
import os from "node:os";

export const config = {
  PORT: Number(process.env.PORT || 8000),
  WORKERS: Number(process.env.WORKERS || Math.max(1, os.availableParallelism() - 1)),
  MAX_QUEUE: Number(process.env.MAX_QUEUE ?? 10_000),
  MAX_BODY_BYTES: Number(process.env.MAX_BODY_BYTES || 1_048_576),
  LISTEN_BACKLOG: process.env.LISTEN_BACKLOG ? Number(process.env.LISTEN_BACKLOG) : undefined,
};

if (config.LISTEN_BACKLOG !== undefined && !(Number.isInteger(config.LISTEN_BACKLOG) && config.LISTEN_BACKLOG >= 1)) {
  throw new Error(`LISTEN_BACKLOG must be an integer >= 1, got "${process.env.LISTEN_BACKLOG}"`);
}
