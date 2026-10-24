import { sendJson } from "../http-utils.js";

export const health = () => ({
  GET: (req, res) => sendJson(res, 200, { ok: true }),
});
