// Same work as /cpu, in a worker thread.
import { sendJson, intParam, badParam } from "../http-utils.js";

export const cpuWorker = ({ pool }) => ({
  GET: async (req, res, search) => {
    const n = intParam(search, "n", 30, 0, 45);
    if (n === null) return badParam(res, "n", 0, 45);
    try {
      const result = await pool.submit(n);
      sendJson(res, 200, { n, result });
    } catch (e) {
      if (e.code === "QUEUE_FULL") {
        return sendJson(
          res,
          503,
          { error: "worker queue full" },
          { "retry-after": "1" },
        );
      }
      throw e;
    }
  },
});
