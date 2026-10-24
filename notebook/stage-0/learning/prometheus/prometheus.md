# Prometheus

In Prometheus server, a scrape job defines what Prometheus collects metrics from and how often it collects them.

1. `scrape_interval`:
   Prometheus attempts to collect metrics from each target in the specified interval.
   A shorter interval gives more detailed monitoring but increases network traffic, processing work, and the number of stored samples.

2. `scrape_timeout`:
   This is the maximum time Prometheus waits for a target to respond to a scrape.
   If a scrape takes longer than the timeout, Prometheus marks that scrape as failed. The target might still be reachable, but its metrics weren't collected successfully within the deadline.
   Important: The timeout must not exceed the scrape interval. For example, a 10-second timeout with a 15-second interval is valid.

3. `static_configs` and `targets`:
   This tells Prometheus where to find the metrics endpoint.
   - host.docker.internal identifies the host machine from the container's perspective in your setup.
   - 9100 is the Node Exporter's HTTP port.
   - Node Exporter exposes metrics at /metrics, which Prometheus requests automatically.
   - The endpoint is effectively: http://host.docker.internal:9100/metrics

4. `metrics_path`:
   - If configured, this specifies the HTTP path for metrics: `metrics_path: /metrics`
   - Prometheus uses /metrics by default, so we generally don't need to specify it unless your application exposes metrics at another path.

5. `scrape_interval` inside a job:
   To override the global interval for a specific job.

## Why does Prometheus scrape itself?

- The reason is self-monitoring. Prometheus is also a running application, and it can experience problems that you need to detect.
- For example, it could run out of memory, have slow queries, fail to evaluate alerting rules, or encounter storage problems.
- Prometheus exposes its own internal metrics at: http://localhost:9090/metrics
- When Prometheus scrapes itself, it collects those internal metrics and stores them in its own time-series database.

### What does it monitor?

Examples include:

| Metric | What it tells you |
| --- | --- |
| prometheus_tsdb_head_samples_appended_total | How many samples have been appended to the in-memory TSDB head |
| prometheus_tsdb_head_series | Number of active time series in the TSDB head |
| prometheus_engine_query_duration_seconds | Query execution duration |
| prometheus_target_scrape_pool_sync_total | Activity related to synchronizing scrape target pools |
| up{job="prometheus"} | Whether the last scrape of the Prometheus target succeeded |

These metrics help you understand Prometheus's own health and workload.

**One distinction:** scraping Prometheus itself does not mean Prometheus is collecting every metric from its host machine. That is Node Exporter's job. Prometheus's own /metrics endpoint exposes metrics about Prometheus's internal operation.

## In brief

- **Node Exporter:** Collects host/system metrics — CPU, RAM, disk, network, and filesystem usage.
- **Prometheus itself:** Exposes monitoring-system metrics — scrape health, query performance, time-series count, memory usage, and TSDB activity.
- **Prometheus client in Node.js:** Exposes application metrics — request rate, latency, HTTP errors, active requests, and business-specific metrics.
