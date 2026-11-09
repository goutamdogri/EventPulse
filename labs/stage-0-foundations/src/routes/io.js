// Simulated slow downstream: waiting, no CPU.
import { sendJson, intParam, badParam } from "../http-utils.js";

export const io = () => ({
  GET: async (req, res, search) => {
    const ms = intParam(search, "ms", 50, 0, 10_000);
    if (ms === null) return badParam(res, "ms", 0, 10_000);
    await new Promise((r) => setTimeout(r, ms));
    sendJson(res, 200, { ok: true });
  },
});
