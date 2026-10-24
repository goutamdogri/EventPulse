import { sendJson } from "../http-utils.js";

export const debugStats = ({ pool, loopDelay, state }) => ({
  GET: (req, res, search) => {
    const toMs = (ns) => ns / 1e6;
    sendJson(res, 200, {
      loopDelayMs: {
        p50: toMs(loopDelay.percentile(50)),
        p99: toMs(loopDelay.percentile(99)),
        max: toMs(loopDelay.max),
      },
      memory: process.memoryUsage(),
      activeResources: process.getActiveResourcesInfo().length,
      inFlight: state.inFlight,
      workers: pool.stats(),
      uptimeSeconds: process.uptime(),
    });
    // Add ?reset=1 to clear the event-loop delay histogram between runs.
    if (new URLSearchParams(search).get("reset") === "1") loopDelay.reset();
  },
});
