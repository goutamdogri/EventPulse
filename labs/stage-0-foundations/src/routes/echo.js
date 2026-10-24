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
      parsed = JSON.parse(raw.toString("utf8"));
    } catch {
      return sendJson(res, 400, { error: "invalid JSON" });
    }
    sendJson(res, 200, parsed);
  },
});
