# Stage 0 — Foundations

Baseline Node.js service instrumented with `@prometheus-io/client`, plus a
full observability stack (Prometheus, Grafana, Loki, node_exporter, Grafana Alloy).

The Node service is **not** containerized. It runs on the host (e.g. with
`taskset`) and Prometheus scrapes it through the Docker host gateway.

## Run the app (host)

```bash
taskset -c 0-1 node baseline-server.js   # listens on :8000, exposes /metrics
```

To ship app logs to Loki via Alloy (recommended):

```bash
mkdir -p labs/stage-0-foundations/logs
taskset -c 0-1 node labs/stage-0-foundations/baseline-server.js 2>&1 | tee -a labs/stage-0-foundations/logs/app.log
```

Alloy tails `logs/*.log` and ships them to Loki with job `eventpulse-app`.

## Run the monitoring stack (Docker)

```bash
cd labs/stage-0-foundations
docker compose up -d --build
```

| Service        | URL                     |
| -------------- | ----------------------- |
| App            | http://localhost:8000   |
| Prometheus     | http://localhost:9090   |
| Grafana        | http://localhost:3000   |
| Loki           | http://localhost:3100   |
| Node Exporter  | http://localhost:9100/metrics |
| Grafana Alloy  | http://localhost:12345  |

Grafana default credentials: `admin` / `admin`.

## Load tests with live metrics to Grafana

k6 v2.3.0 runs with Prometheus remote write to `http://localhost:9090/api/v1/write`.

```bash
taskset -c 2,8,3,9,4,10 \
  K6_PROMETHEUS_RW_SERVER_URL=http://localhost:9090/api/v1/write \
  k6 run \
    -e RATE=1000 -e DURATION=60s -e ROUTE=/health \
    -o experimental-prometheus-rw \
    --tag testid=manual-test \
    loadtests/k6/script.js
```

Or use the provided script (fixes server check, enables remote write, tags test runs):

```bash
cd loadtests/k6 && ./run-stage-0.sh
```

On a new Grafana data volume, import the JSON files from `grafana/dashboards` through Grafana's dashboard import page. After importing, open Grafana → Dashboards → **k6 Prometheus** and set `testid = All` (or the specific run). Also view **Node Exporter Full** for host metrics and **NodeJS Application Dashboard** for app metrics. Imported dashboards and later UI edits are stored in `grafana-data`.

## How it fits together

- App (host) exposes `/metrics` on `:8000` → Prometheus scrapes `host.docker.internal:8000`.
- `node-exporter` (container, host PID/mounts) exposes host metrics → Prometheus scrapes `node-exporter:9100`.
- Prometheus is configured with `--web.enable-remote-write-receiver`; k6 pushes samples via `experimental-prometheus-rw` to `/api/v1/write`.
- Loki stores logs; Alloy discovers Docker container logs and tails host app log files (`logs/*.log`) → pushes to Loki.
- Grafana provisions the Prometheus and Loki datasources. Its dashboard provider intentionally points at an empty folder, with UI updates enabled and deletion disabled; this keeps existing dashboards while preventing the source JSON from replacing edits saved in Grafana. Dashboard state is stored in the `grafana-data` named volume. On a new data volume, import the JSON files from `grafana/dashboards` through the Grafana UI; those imported dashboards and later edits are saved in the database.

## Layout

```
grafana/
  provisioning/
    datasources/  (prometheus.yml, loki.yml)
    dashboards/   (dashboards.yml)
  dashboards/     (nodejs-application-dashboard.json, node-exporter-full.json, k6-prometheus.json)
  tools/          (sync-dashboards.py)
alloy/config.alloy
loki/loki-config.yml
prometheus-config.yml
docker-compose.yml
README.md
```

### Recommended CPU pinning for isolation

To avoid generator/load-test interference and to see single-core saturation clearly:

- **k6 (load generator)**: `taskset -c 2-5 ./loadtests/k6/run-stage-0.sh` (or pick separate cores)
- **Node app (target)**: `taskset -c 0-1 node labs/stage-0-foundations/baseline-server.js 2>&1 | tee -a labs/stage-0-foundations/logs/app.log`

Use `maxVUs: 1000` (configured) and `preAllocatedVUs: 200`. With this, overload shows up as dropped iterations rather than a huge connection storm.

### Listen queue textfile (for Listen queue length panel)

If using the EventPulse Load Lab dashboard, the "Listen queue length" panel requires node_exporter textfile collector metrics `ep_listen_recvq` and `ep_listen_sendq` for port 8000.

Create/update the textfile every second (e.g. with cron, or a small background loop):

```bash
while true; do
  ./labs/stage-0-foundations/scripts/collect_listen_queues.sh
  sleep 1
done &
```

The textfile is mounted into node_exporter at `/var/lib/node_exporter/textfile`.
