// Blocks the event loop synchronously: demonstrates coordinated omission.
import { sendJson, intParam, badParam } from "../http-utils.js";

export const stall = ({ metrics }) => ({
  GET: (req, res, search) => {
    const ms = intParam(search, "ms", 2000, 0, 30_000);
    if (ms === null) return badParam(res, "ms", 0, 30_000);
    metrics.stallsTotal.inc();
    const end = Date.now() + ms;
    while (Date.now() < end) {
      /* spin */
    }
    sendJson(res, 200, { stalled: ms });
  },
});
