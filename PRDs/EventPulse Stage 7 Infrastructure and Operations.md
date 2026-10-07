# EventPulse Stage 7: Infrastructure and Operations

*Duration: about 4 weeks. Goal: run the whole platform like production: containerized, orchestrated, autoscaled, reproducible in the cloud, observable end to end, governed by SLOs, and proven under system-level chaos. You finish by writing a runbook and a postmortem about a failure you caused.*

## 1. Objective and Motivating Pain

Stage 6 left components that scale individually but run from compose files and scripts. Scaling is manual, deployments are risky, observability is ad hoc, and failure testing has been one component at a time. Production-grade systems are defined by how they behave when things go wrong and how quickly you notice and recover.

By the end of this stage you must be able to:

1. Deploy the full system with one command locally, and create and destroy a cloud environment with Terraform, with cost controls enforced.
2. Roll out a new version under load with zero failed requests and no lost acknowledged events, and roll back quickly when it is bad.
3. Autoscale stateless services on CPU and consumers on Kafka lag, and explain exactly when autoscaling helps and when it hurts.
4. Define SLOs, alert on error budget burn, and show that every alert fired during chaos tests while noisy alerts were removed.
5. Run chaos experiments with hypotheses and abort conditions, measure time to detect and time to recover, and write a blameless postmortem.

**Predictions to write down first:** list the three failures you expect to hurt most and what your dashboards and alerts will show for each. After the experiments, compare.

## 2. Prerequisites and Setup

| Item | Requirement |
| --- | --- |
| Stages 0-6 | Complete: services, delivery guarantees, limiter, sharding, runbook habits |
| Kubernetes | Local cluster (k3d or kind) for development; k3s on cloud VMs or a managed cluster for burst tests. Check current managed-cluster control plane pricing before choosing; self-managed k3s on spot VMs is usually cheaper |
| Packaging | Docker, Helm |
| IaC | Terraform with remote state (a cloud bucket with locking) |
| CI/CD | GitHub Actions (or equivalent), a container registry |
| Autoscaling | Metrics server for HPA, KEDA for Kafka-lag scaling |
| Stateful systems | Kafka via an operator such as Strimzi; Postgres, Redis, and ClickHouse as StatefulSets or operators, or kept on dedicated VMs initially. Running stateful systems on Kubernetes is its own discipline; choose consciously and record the decision in an ADR |
| Observability | kube-prometheus-stack (Prometheus, Alertmanager, Grafana), OpenTelemetry Collector with a tracing backend (Tempo or Jaeger), structured logs (Loki optional) |
| Chaos tools | `kubectl delete`, Chaos Mesh or Litmus (optional), toxiproxy, `tc` via a privileged debug pod |
| Cost controls | Billing alerts at $10, $25, $50, a hard cap, spot instances, resource tags on everything, and a teardown that you run after every session |

## 3. Learning Plan

### Kubernetes essentials

- **Workloads:** Deployments for stateless services, StatefulSets for stateful ones, Services and Ingress for traffic, ConfigMaps and Secrets for configuration.
- **Resources:** requests drive scheduling; limits drive throttling (CPU) and OOM kills (memory). Know QoS classes. CPU limits can throttle latency-sensitive services even when average CPU looks low.
- **Probes:** *liveness* decides restart, *readiness* decides whether traffic is sent, *startup* protects slow starters. A liveness probe that checks dependencies turns a downstream outage into a restart storm; keep liveness shallow.
- **Rollouts:** `maxSurge` and `maxUnavailable`, PodDisruptionBudgets, topology spread and anti-affinity.
- **Graceful termination:** on shutdown a pod receives SIGTERM; endpoint removal and signal delivery are asynchronous, so a short `preStop` delay plus readiness going false prevents requests landing on a dying pod. Set `terminationGracePeriodSeconds` to cover draining.

### Autoscaling

- **HPA:** scales on CPU, memory, or custom metrics. CPU is a poor signal for IO-bound services.
- **KEDA:** scales on external signals such as Kafka consumer lag. Maximum useful replicas for a consumer group equals the partition count.
- **Scaling side effects:** each scale event causes a consumer group rebalance. Scaling consumers faster can overload the database behind them.
- **Node scaling:** cluster autoscaler or Karpenter, understood conceptually; local clusters will not exercise it.

### Helm, Terraform, CI/CD

- **Helm:** templates plus values per environment, releases, rollback.
- **Terraform:** providers, modules, state, `plan` vs `apply`, drift, locking, tagging, outputs, `destroy`.
- **Pipeline:** lint, unit and integration tests, image build, vulnerability scan, schema compatibility check (Stage 3), push, deploy, smoke test, automated rollback. Optional: GitOps with a controller that syncs the cluster from Git, and canary releases.

### Load balancing

- **Nginx or Envoy** in front of the API: upstream keepalive, timeouts, connection limits, health checks, buffering, HTTP/2. Be careful with automatic retries: only retry requests that are safe, and your idempotency keys make ingest retries safe.

### Observability and reliability engineering

- **Metrics:** RED (rate, errors, duration) for services and USE (utilization, saturation, errors) for resources. Use histograms for latency. **Control label cardinality**: a `tenant_id` label on a high-volume metric can explode Prometheus memory.
- **Tracing:** OpenTelemetry with context propagated through Kafka headers so one event can be followed from ingest to ClickHouse. Understand head vs tail sampling.
- **Logs:** structured JSON with trace and request ids; no per-request logging on the hot path at full volume.
- **SLI, SLO, error budget:** an SLI is a measured ratio, an SLO a target, the budget is the allowed failure. A 99.9% target over 30 days allows about 43 minutes of full unavailability.
- **Burn-rate alerting:** page when budget is being consumed too fast over multiple windows, instead of alerting on raw CPU. Typical pairs for a 99.9% SLO are 14.4x over 1 hour with a 5 minute confirmation, 6x over 6 hours with 30 minutes, and a ticket at 1x over 3 days.
- **Runbooks and postmortems:** alerts link to actionable steps; postmortems are blameless and end in tracked actions.

### Chaos engineering principles

- State the steady state, write a hypothesis, inject a fault with a limited blast radius, define abort conditions, observe, and fix. Run first in a non-shared environment.

### Reading

Google's *Site Reliability Engineering* book (SLOs, monitoring, postmortems) and the SRE Workbook chapter on alerting on SLOs. Kubernetes documentation on pod lifecycle, probes, and disruptions. KEDA's Kafka scaler documentation. Terraform's state and workflow documentation. Nygard's *Release It!* for stability patterns, if you have time. In `notebook/ops-notes.md`, explain why a liveness probe should not check the database.

## 4. Requirements

| ID | Requirement |
| --- | --- |
| S7-F1 | Every service is containerized with a small non-root image, separate liveness and readiness endpoints, configuration through environment, and graceful shutdown |
| S7-F2 | Helm charts with per-environment values; a single command deploys the whole system to local k3d |
| S7-F3 | Terraform creates and destroys the cloud test environment; budget alarms and resource tags are part of the code; a leak check confirms nothing remains after destroy |
| S7-F4 | HPA for the API; KEDA on Kafka lag for the sink, processor, rule engine, and notifier, with maximum replicas bounded by partitions |
| S7-F5 | Rolling deployment under load with zero failed requests and no lost acknowledged events |
| S7-F6 | Nginx or Envoy in front with tuned keepalive, timeouts, and health checks |
| S7-F7 | CI/CD with tests, image scan, schema check, deployment to a dev environment, smoke test, and rollback |
| S7-F8 | Prometheus metrics, Grafana dashboards, OpenTelemetry traces across Kafka, and structured logs for every service |
| S7-F9 | SLOs with multi-window burn-rate alerts; each paging alert has a runbook |
| S7-F10 | A chaos experiment suite with hypotheses, abort conditions, and recorded results |
| S7-F11 | At least one blameless postmortem for a failure you caused |

### Proposed SLOs (tune with data)

| SLI | Target |
| --- | --- |
| Ingest availability (valid requests answered 202) | 99.9% over 30 days |
| Ingest latency | 99% of requests under 50 ms |
| Freshness (ingest to queryable aggregate) | 99% within 5 s |
| Notification delivery (firing to webhook attempt) | 99% within 60 s |
| Invariant (not an SLO, tested continuously) | zero acknowledged events lost |

## 5. Design

```
Internet/k6 -> Nginx/Envoy -> API (Deployment, HPA, N pods)
                                  |
         Kafka (Strimzi, 3 brokers) <-- events.raw
           |            |            |             |
        sink-ch     processor    rule-engine    sink-pg
        (KEDA)      (KEDA)       (KEDA)         (KEDA)
           |                         |
      ClickHouse (StatefulSet)    notifier (KEDA) -> webhooks
      Postgres + PgBouncer, Redis (shards)

Namespaces: ep-app | ep-data | ep-observability
Observability: Prometheus + Alertmanager + Grafana, OTel Collector -> Tempo/Jaeger, logs
Terraform modules: network | cluster | nodes(spot) | storage | budget | tags
Environments: local (k3d) | cloud-test (burst, destroyed after) | demo (tiny, always on)
```

### Deployment manifest essentials (API)

```yaml
spec:
  replicas: 3
  strategy:
    rollingUpdate: { maxSurge: 1, maxUnavailable: 0 }
  template:
    spec:
      terminationGracePeriodSeconds: 40
      containers:
        - name: api
          resources:
            requests: { cpu: "500m", memory: "256Mi" }
            limits:   { memory: "512Mi" }     # decide CPU limits deliberately
          readinessProbe: { httpGet: { path: /readyz, port: 3000 }, periodSeconds: 2 }
          livenessProbe:  { httpGet: { path: /healthz, port: 3000 }, periodSeconds: 10, failureThreshold: 3 }
          startupProbe:   { httpGet: { path: /healthz, port: 3000 }, failureThreshold: 30, periodSeconds: 2 }
          lifecycle:
            preStop: { exec: { command: ["sleep", "8"] } }
```

`/healthz` must not call dependencies. `/readyz` reports whether this instance should receive traffic and flips to not-ready as soon as shutdown begins.

### Graceful shutdown sequence (API and consumers)

```
SIGTERM -> readiness = false -> wait for endpoint removal (preStop delay)
  -> stop accepting new connections (server.close)
  -> flush the micro-batcher and wait for broker acks
  -> producer.disconnect()
Consumer: stop fetching -> finish current batch -> write to DB -> commit offsets
  -> leave the group cleanly -> exit
```

Set the Node heap limit consistently with the container memory limit (for example with `--max-old-space-size`) so you see GC pressure instead of unexplained OOM kills.

### KEDA scaler (consumer example)

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata: { name: sink-clickhouse }
spec:
  scaleTargetRef: { name: sink-clickhouse }
  minReplicaCount: 1
  maxReplicaCount: 6            # never above the partition count
  cooldownPeriod: 300
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: kafka:9092
        consumerGroup: sink-clickhouse
        topic: events.raw
        lagThreshold: "5000"
```

Verify field names and options against the KEDA version you install.

### Burn-rate alert example (conceptual)

```yaml
- alert: IngestErrorBudgetFastBurn
  expr: |
    (sum(rate(http_server_requests_total{service="ingest",code=~"5.."}[1h]))
       / sum(rate(http_server_requests_total{service="ingest"}[1h])) > 14.4 * 0.001)
    and
    (sum(rate(http_server_requests_total{service="ingest",code=~"5.."}[5m]))
       / sum(rate(http_server_requests_total{service="ingest"}[5m])) > 14.4 * 0.001)
  labels: { severity: page }
  annotations: { runbook: "docs/runbooks/ingest-errors.md" }
```

Use `promtool test rules` with synthetic series so the alert logic is unit tested before it ever meets an incident.

### Repo additions

```
deploy/helm/ (charts per service, values-local.yaml, values-cloud.yaml)
deploy/terraform/ (modules, envs, budget, tags)
deploy/observability/ (dashboards JSON, alert rules, otel config)
.github/workflows/ (ci.yml, deploy.yml)
chaos/ (experiments as code, abort checks)
docs/ (slo.md, runbooks/, postmortems/, chaos-report.md, adr/)
```

## 6. Step-by-Step Build Plan

1. **Productionize each service:** images, health endpoints, graceful shutdown, config via environment.
2. **Observability first,** before any orchestration work, so every later experiment is instrumented: metrics, dashboards, traces, structured logs.
3. **Helm charts and local k3d deployment** with one command.
4. **SLOs and alerts,** including `promtool` rule tests.
5. **Nginx or Envoy** with tuned settings.
6. **HPA and KEDA** and the autoscaling tests.
7. **CI/CD pipeline** with rollback.
8. **Terraform cloud environment** with budget alarms, tags, and a destroy-and-leak-check script.
9. **Rolling deploy under load test** until it is clean.
10. **Chaos game days** from the catalog, one experiment at a time, with hypotheses and abort conditions.
11. **Incident postmortem** for a failure that surprised you.
12. **Runbooks and the chaos report.**

## 7. Test Plan

| Layer | Tests | Pass condition |
| --- | --- | --- |
| Image | Size, non-root user, vulnerability scan | Within budget; no critical findings |
| Manifest | `helm lint`, template rendering, schema validation of manifests | Clean |
| Smoke | Post-deploy event goes through ingest, Kafka, ClickHouse, query API | Appears within the freshness SLO |
| Rolling deploy | Continuous k6 load during a rollout, then the pipeline audit | Zero failed requests, no lost acknowledged events |
| Graceful shutdown | SIGTERM during load | In-flight requests complete; batcher flushed; consumers leave cleanly |
| Autoscaling | Inject lag; stop injection | Scale-up within a target time, scale-down without flapping |
| Alert rules | `promtool test rules` on synthetic series | Alerts fire and resolve as designed |
| Trace completeness | One tagged event | A single trace spans ingest, Kafka, sink, and processor |
| Terraform | `plan` in CI; full apply and destroy | No drift; tag-based leak check finds nothing after destroy |
| Rollback | Deploy a deliberately bad version | Detected by SLO alert or probe; rollback completes within a target |

## 8. Break-It Experiment Catalog (Chaos)

For every experiment record: steady-state hypothesis, injection, blast radius, abort condition, the alert you expect, time to detect, time to recover, whether human action was needed, and what you changed afterwards.

### I1 Kill pods under load

- **Hypothesis:** killing any single stateless pod causes no failed acknowledged requests beyond a brief window and no data loss; killing a consumer pod causes a rebalance and recovery without manual action.
- **How:** delete pods of each component under load: API, sink, processor, rule engine, notifier, Kafka broker, Redis primary, Postgres primary.
- **Observe:** error rate, latency, lag, recovery time, audit result, alerts fired.
- **Explains:** which components are truly resilient and which have hidden single points of failure.

### I2 Kill and drain nodes

- **Hypothesis:** PodDisruptionBudgets and anti-affinity keep the service available during a drain; a hard node loss (including a spot interruption simulation) causes a longer recovery.
- **How:** drain a node, then hard-stop one.
- **Observe:** rescheduling time, partitions leadership changes, stateful pod recovery, SLO impact.
- **Explains:** replica placement and disruption policy.

### I3 Network latency and loss

- **Hypothesis:** added latency or loss to Kafka, Redis, or Postgres raises p99 proportionally; missing timeouts convert slowness into outages; circuit breakers and bounded queues limit damage.
- **How:** `tc` or toxiproxy or Chaos Mesh on each dependency link, at 50 ms, 200 ms, and 1% loss.
- **Observe:** latency, error rate, in-flight requests, retry volume, breaker state.
- **Explains:** timeout budgets and gray failures.

### I4 Network partition

- **Hypothesis:** partitioning API from Kafka yields fast 503s with bounded memory; partitioning consumers from Postgres grows lag but loses nothing.
- **How:** block traffic between selected pods or namespaces.
- **Observe:** errors, lag, recovery, duplicates after healing.
- **Explains:** your explicit failure policies, now tested at system level.

### I5 Disk fill

- **Hypothesis:** a disk filling on a Kafka broker, ClickHouse, or Postgres degrades or stops that component; an alert on disk usage trend should fire long before exhaustion.
- **How:** fill a volume in a controlled way with an abort condition.
- **Observe:** component behavior, alert lead time, recovery steps, data integrity.
- **Explains:** retention, capacity alerts, and runbooks.

### I6 Slow instead of dead (gray failure)

- **Hypothesis:** a slow ClickHouse or Redis is worse than a dead one because health checks pass while latency rises everywhere.
- **How:** throttle CPU or IO of one dependency.
- **Observe:** which SLO alerts catch it, how long detection takes, whether dashboards point to the cause.
- **Explains:** why symptom-based alerts and good dashboards matter.

### I7 Bad deploys

- **Hypothesis:** a crash-looping image is stopped by readiness probes and rolling update settings; a subtle regression (slower p99, wrong config, memory leak) is caught only by SLO alerts or canary analysis.
- **How:** ship each type of bad version through the pipeline under load.
- **Observe:** blast radius, detection time, rollback time.
- **Explains:** the value of progressive delivery and fast rollback.

### I8 Resource limits

- **Hypothesis:** a low CPU limit causes throttling and p99 spikes while average CPU looks fine; a memory limit below the Node heap setting produces OOM kills.
- **How:** vary limits; watch throttling metrics and OOM events.
- **Observe:** latency, restarts, `container_cpu_cfs_throttled` style metrics.
- **Explains:** requests and limits as a performance decision.

### I9 Probe misconfiguration

- **Hypothesis:** an aggressive liveness probe under load restarts healthy pods in a loop; no readiness probe sends traffic to cold pods; a dependency-checking liveness probe turns a database blip into a full restart storm.
- **How:** configure each mistake deliberately.
- **Observe:** restart counts, error spikes, recovery.
- **Explains:** what each probe type is for.

### I10 Graceful shutdown failures

- **Hypothesis:** without `preStop` and readiness handling, rolling updates produce a burst of connection errors; consumers that exit without a clean leave cause rebalance storms and duplicates.
- **How:** run rollouts with each protection removed.
- **Observe:** error count per rollout, duplicates after the rollout.
- **Explains:** why zero-downtime deploys require several cooperating settings.

### I11 Autoscaling pathologies

- **Hypothesis:** scaling consumers beyond the partition count adds nothing; short cooldowns cause flapping; each scale event triggers rebalances; HPA on CPU misjudges an IO-bound service; scaling consumers up can overwhelm Postgres or ClickHouse.
- **How:** inject bursts and lag with different scaler settings; watch the downstream.
- **Observe:** replica counts over time, lag, rebalance count, downstream latency and errors.
- **Explains:** autoscaling as a control system with delays, not a free lunch.

### I12 Observability failures

- **Hypothesis:** adding a high-cardinality label (such as a tenant id) to a hot metric blows up Prometheus memory; a down Prometheus or Alertmanager leaves you blind unless something watches the watchers.
- **How:** introduce the label; stop Prometheus; misroute an alert.
- **Observe:** series count, memory, alert delivery, a dead-man's-switch alert.
- **Explains:** metric design and meta-monitoring.

### I13 Infrastructure as code failures

- **Hypothesis:** manual changes cause drift that Terraform reverts or breaks on; an interrupted apply leaves partial state; a forgotten destroy leaks cost.
- **How:** change a resource by hand; kill an apply midway; skip a teardown in a test and see if the budget alert fires.
- **Observe:** plan output, state lock behavior, leaked resources, alert timing.
- **Explains:** the discipline around state, tagging, and teardown.

### I14 Noisy alerts and alert quality

- **Hypothesis:** alerts based on causes (CPU, queue depth) fire often without user impact; burn-rate alerts fire only when users are hurt.
- **How:** run a week of normal load and chaos with both alert styles.
- **Observe:** pages that were not actionable, missed incidents.
- **Explains:** which alerts deserve to page.

## 9. Metrics and Reporting

- **Per experiment:** time to detect (injection to alert), time to recover, error budget consumed, whether the audit reconciled, actions taken.
- **Operations:** deploy frequency, rollback time, autoscaling reaction time, rebalance counts.
- **Platform:** the Stage 0-6 metrics plus pod restarts, throttling, OOM kills, node pressure.
- **Cost:** hourly cost of each environment, cost per test session, cost per million events (preview for Stage 8), leaked-resource check results.

## 10. Artifacts

1. **Runbooks** for every paging alert (symptoms, likely causes, diagnosis steps, mitigation, escalation).
2. **SLO document** with SLIs, targets, error budget policy, and alert definitions.
3. **Chaos report:** a table of experiments with hypothesis, result, time to detect, time to recover, and fixes.
4. **Postmortem** for an incident you caused, blameless format:
   - Summary and impact (who or what was affected, for how long, error budget burned)
   - Timeline with timestamps (detection, diagnosis, mitigation, resolution)
   - Root cause and contributing factors
   - What went well, what went poorly, where you got lucky
   - Action items with owners and due dates, then verified
5. **Deployment and architecture diagrams** and ADRs (where stateful systems run, Nginx vs Envoy, scaler choices, probe policy).
6. **Infrastructure code, pipeline, and dashboards** committed and reproducible.
7. **Cost report** per test session.
8. **Optional blog post:** "I killed my Kafka broker on purpose, and here is what my dashboards showed" or "Why my liveness probe caused an outage."

## 11. Common Pitfalls

- Adding observability after the chaos work, then not being able to explain what happened.
- Liveness probes that check dependencies.
- No resource requests, or CPU limits chosen without measuring throttling.
- Autoscaling consumers beyond partition count, or on CPU for IO-bound work.
- High-cardinality metric labels.
- Alerting on causes instead of symptoms, and alerts with no runbook.
- Chaos experiments without hypotheses, abort conditions, or recorded results.
- Terraform state kept locally, secrets in the repo, no tagging, and no teardown.
- PodDisruptionBudgets that block every drain, or none at all.
- Ignoring endpoint propagation delay during shutdown.
- Never testing rollback until the day it is needed.
- Running a stateful system on Kubernetes without understanding its storage and failure modes.

## 12. Exit Criteria

- [ ] One-command local deployment and one-command cloud create and destroy, with budget alarms and a clean leak check.
- [ ] Zero-failure rolling deploy under load, verified by the pipeline audit.
- [ ] Graceful shutdown proven for API and consumers.
- [ ] HPA and KEDA working, with the pathologies from I11 understood and documented.
- [ ] Dashboards, traces, and logs cover the whole path; one trace follows an event end to end.
- [ ] SLOs and burn-rate alerts defined, unit tested, and each paging alert fired in chaos tests.
- [ ] Experiments I1 through I14 recorded with hypotheses, MTTD, MTTR, and fixes.
- [ ] Runbooks written for each paging alert.
- [ ] A blameless postmortem written.
- [ ] Repo tagged `stage-7-complete`.

**Self-test questions (answer aloud, without notes):**

1. Why must a liveness probe not check the database? What does readiness do instead?
2. Walk through exactly what happens during a rolling update, from SIGTERM to the pod's exit, and what can drop requests.
3. Why does KEDA scaling stop helping past the partition count, and what else can scaling up break?
4. What is a burn-rate alert and why is it better than a CPU alert for paging?
5. A node dies. List what recovers automatically, what needs a human, and how you would know.
6. Describe your postmortem: root cause, contributing factors, and which action item mattered most.

## 13. Handoff to Stage 8

Stage 7 leaves a platform you can deploy, observe, and break. You still do not know how efficient it is or where scaling stops:

- Per-node throughput of the ingest path is unprofiled; you do not know where CPU time goes or how much is framework, JSON, or GC.
- The language and framework choice (Node and Express vs Go or Rust) has not been measured.
- Scaling from 1 to 2 to 4 to 8 ingest nodes has not been charted, and the first real bottleneck (broker, network, or something else) is unknown.
- The cost per million events and the cost model for 1M rps do not exist yet.

Stage 8 profiles and tunes the ingest path, builds the same endpoint in Go for comparison, runs the scaling tests with your new infrastructure, and produces the benchmark report and cost model. Before starting, write down your predictions for the per-node ceiling of Node and Express, the Go speedup, and which component limits scaling first.

When you are ready, say **Stage 8**.
