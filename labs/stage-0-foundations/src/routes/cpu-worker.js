// Same work as /cpu, in a worker thread.
import { sendJson, intParam, badParam } from "../http-utils.js";

export const cpuWorker = ({ pool }) => ({
  GET: async (req, res, search) => {
    const n = intParam(search, "n", 30, 0, 45);
    const pad = intParam(search, "pad", 0, 0, 100);

    if (n === null) return badParam(res, "n", 0, 45);
    if (pad === null) return badParam(res, "pad", 0, 100);

    // Mocking request body
    const binarySize = pad * 1024;
    const paddedBinaryBody = new Uint8Array(binarySize);

    try {
      const result = await pool.submit(n, paddedBinaryBody);
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
