// Worker thread entry: receives { n }, replies with { result }.
import { parentPort } from "node:worker_threads";
import { fib } from "./fib.js";

parentPort.on("message", ({ n }) => {
  parentPort.postMessage({ result: fib(n) });
});
