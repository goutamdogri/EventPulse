// Prometheus scrape endpoint.
export const metricsRoute = ({ metrics }) => ({
  GET: async (req, res) => {
    try {
      const body = await metrics.register.metrics();
      res.writeHead(200, { "content-type": metrics.register.contentType });
      res.end(body);
    } catch {
      res.writeHead(500);
      res.end("Failed to collect metrics");
    }
  },
});
