// Parse JSON and return it: measures JSON cost and payload scaling.
import { sendJson, readBody } from "../http-utils.js";

export const echo = ({ config, metrics }) => ({
  POST: async (req, res) => {
    let raw;
    try {
      raw = await readBody(req, config.MAX_BODY_BYTES);
    } catch (e) {
      return sendJson(res, e.status || 400, { error: e.message });
    }
    metrics.echoBodyBytes.observe(raw.length);
    let parsed;
    try {
      const t0 = process.hrtime.bigint();
      parsed = JSON.parse(raw.toString("utf8"));
      metrics.echoParse.observe(Number(process.hrtime.bigint() - t0) / 1e9);
    } catch {
      return sendJson(res, 400, { error: "invalid JSON" });
    }
    sendJson(res, 200, parsed, {}, {
      onStringify: (seconds) => metrics.echoStringify.observe(seconds),
    });
  },
});
