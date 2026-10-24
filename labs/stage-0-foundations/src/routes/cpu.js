// Synchronous Fibonacci: blocks the event loop on purpose.
import { sendJson, intParam, badParam } from "../http-utils.js";
import { fib } from "../fib.js";

export const cpu = () => ({
  GET: (req, res, search) => {
    const n = intParam(search, "n", 30, 0, 45);
    if (n === null) return badParam(res, "n", 0, 45);
    sendJson(res, 200, { n, result: fib(n) });
  },
});
