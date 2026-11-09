// Route table: path -> { METHOD: handler }. Each route is a factory that receives the shared ctx.
import { health } from "./health.js";
import { cpu } from "./cpu.js";
import { cpuWorker } from "./cpu-worker.js";
import { io } from "./io.js";
import { echo } from "./echo.js";
import { stall } from "./stall.js";
import { debugStats } from "./debug-stats.js";
import { metricsRoute } from "./metrics.js";

export const createRoutes = (ctx) => ({
  "/health": health(ctx),
  "/cpu": cpu(ctx),
  "/cpu-worker": cpuWorker(ctx),
  "/io": io(ctx),
  "/echo": echo(ctx),
  "/stall": stall(ctx),
  "/debug/stats": debugStats(ctx),
  "/metrics": metricsRoute(ctx),
});
