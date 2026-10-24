// Small HTTP helpers for node:http handlers.

export const sendJson = (res, status, obj, extraHeaders = {}) => {
  const body = JSON.stringify(obj);
  res.writeHead(status, {
    "content-type": "application/json",
    "content-length": Buffer.byteLength(body),
    ...extraHeaders,
  });
  res.end(body);
};

// Returns default if absent, the number if valid, or null if invalid.
export const intParam = (search, name, def, min, max) => {
  const raw = new URLSearchParams(search).get(name);
  if (raw === null) return def;
  const v = Number(raw);
  return Number.isInteger(v) && v >= min && v <= max ? v : null;
};

export const badParam = (res, name, min, max) =>
  sendJson(res, 400, {
    error: `${name} must be an integer in [${min}, ${max}]`,
  });

// Reads the body with a size cap. Over the cap: stop buffering, keep draining,
// and reject with status 413 once the body ends (so a 413 can still be sent).
export const readBody = (req, limit) =>
  new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    let tooBig = false;
    req.on("data", (c) => {
      size += c.length;
      if (size > limit) {
        tooBig = true;
        chunks.length = 0;
      } else if (!tooBig) {
        chunks.push(c);
      }
    });
    req.on("end", () =>
      tooBig
        ? reject(Object.assign(new Error("payload too large"), { status: 413 }))
        : resolve(Buffer.concat(chunks)),
    );
    req.on("error", reject);
  });
