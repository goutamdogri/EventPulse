# EventPulse: What It Is, Why It Exists, and How to Explain It

*A reference for you: the problem, the end-to-end flow, what users configure, the features, the use cases, where it fits in the bigger picture, and how to answer "is there a real use case?" in an interview.*

## 1. The Short Version

**One sentence:** EventPulse is a multi-tenant backend that takes a firehose of events from applications, turns them into live metrics and alerts within seconds, and keeps the raw history queryable, without losing or double counting anything.

**One paragraph:** Modern applications constantly produce events: an order was placed, a payment failed, a sensor reported a temperature, a user logged in. Teams want to know *right now* what is happening (how many payments are failing per minute?), want to be *notified automatically* when something crosses a threshold, and want to *query history* later. Doing this with a plain database collapses at scale and gives you no live computation. EventPulse is the pipeline that sits between "things happening in applications" and "answers and actions for people," built to be fast, correct, and safe to share between many customers (tenants).

**Interview-ready 30 seconds:** "EventPulse is a real-time event processing platform. Applications send events through an API; I buffer them durably in Kafka, compute windowed aggregates in a stream processor, store raw and aggregated data in ClickHouse, and run a rule engine that triggers deduplicated webhooks. The project is about the hard parts: delivery guarantees, backpressure, tenant isolation, scaling, and failure behavior, and I measured and broke each layer to prove how it behaves."

## 2. What Is an Event?

An **event** is a record that *something happened at a specific time*. It is a fact about the past, usually immutable, and usually small (hundreds of bytes). It is not a request for the system to do something (that is a command), and it is not a continuous measurement stream (that is a metric, although a metric sample can be modeled as an event).

| Domain | Example events |
| --- | --- |
| E-commerce | `order.created`, `payment.failed`, `cart.abandoned`, `item.shipped` |
| SaaS product | `user.signed_up`, `feature.used`, `subscription.cancelled` |
| Payments and fintech | `transaction.authorized`, `transaction.declined`, `chargeback.opened` |
| DevOps | `deploy.finished`, `service.error`, `job.failed` |
| IoT and devices | `sensor.reading`, `device.offline`, `door.opened` |
| Security | `login.failed`, `password.reset_requested`, `api_key.created` |
| Logistics and mobility | `driver.location_updated`, `ride.requested`, `package.delivered` |

An event as it arrives at EventPulse:

```
{
  "event_id": "6f1c...",                // unique id chosen by the sender
  "type": "payment.failed",
  "occurred_at": "2026-10-07T10:15:03.120Z",   // when it happened (event time)
  "schema_version": 1,
  "payload": { "amount": 49.99, "currency": "INR", "reason": "insufficient_funds", "user_id": "u_881" }
}
```

The tenant is not in the body; it is derived from the API key, so a customer cannot claim to be someone else.

## 3. The Problem It Solves

### Without a platform like this

- Each team writes events straight into an application database or log files.
- Questions like "payment failures per minute by country" run as slow scans or nightly batch jobs, so answers are hours old.
- Alerts are hand-built cron scripts that poll the database, miss spikes, and fire late.
- A traffic burst (a sale, an outage retry storm) overloads the database and loses data.
- Retries create duplicates, and crashes create gaps, so counts are untrustworthy.
- Every team re-implements pipelines, queues, dashboards, and rate limits, inconsistently.

### With EventPulse

- Senders do one simple thing: POST events and get a fast acknowledgment.
- Events are buffered durably, so bursts and downstream slowness do not lose data.
- Aggregates are computed continuously, so dashboards are seconds old, not hours.
- Rules turn aggregates into actions (webhook, notification) automatically, each action firing once.
- Raw history stays queryable for investigation.
- One tenant's flood cannot hurt another tenant.

**The core insight:** a database *stores and fetches*. EventPulse *continuously computes on a moving stream and acts on the result*, reliably, at a scale where a single database in the request path would fall over.

## 4. Who Sends Events, and Do They Need to Configure Anything?

### Who sends events

The customer's own systems: backend services, mobile and web apps, devices and gateways, batch jobs. Third-party software (payments, version control, shipping providers) typically *emits webhooks*; you can accept those through a small adapter that converts them into EventPulse events. That adapter is an optional extension, not part of the ten stages.

### Can they send anything?

There are two layers, and this is a product decision you should be able to explain:

| Layer | Rule |
| --- | --- |
| **Envelope** (required, strict) | `event_id`, `type`, `occurred_at`, `schema_version`, plus the API key identifying the tenant. Without these the platform cannot deduplicate, order by event time, route, or isolate tenants |
| **Payload** (flexible, governed) | Free-form JSON by default, but for each event type a tenant can *register a schema* (fields and types) that is enforced at the edge |

Policy options for unregistered event types, to choose and document: **permissive** (accept and store, no typed analytics), **strict** (reject unknown types), or **quarantine** (accept but hold for review). A reasonable default for a platform like this: strict envelope, permissive payload on first use, schema registration encouraged so the platform can validate, extract typed columns, and protect consumers from breaking changes.

### What they configure (and what they do not)

| Configuration | Required? | What it gives them |
| --- | --- | --- |
| API key | Yes | Authentication and tenant identity |
| Rate limits and quotas | Defaults provided | Protection for them and for neighbors |
| Event type schemas | Optional, recommended | Validation, typed fields, safe evolution |
| Aggregations (what to count or sum, by which field, over which windows) | Defaults provide count per type per minute and hour | Dashboards and rule inputs |
| Rules (condition, duration, cooldown) | Optional | Automatic alerts and actions |
| Webhook endpoints and secrets | Needed only for actions | Where notifications are delivered, with signing |
| Dashboards | Optional | Saved views of the metrics they care about |

**Zero-config path:** create an API key, POST events, and immediately get raw storage and per-type counts. Everything else is opt-in.

## 5. The End-to-End Flow (with a Concrete Story)

**Scenario:** ShopKart, an online store, is a tenant. During a sale, 5,000 orders per minute arrive and their payment provider starts failing intermittently.

```
ShopKart apps -> POST /v1/events -> [ API: auth, rate limit, validate ] -> 202 Accepted
                                              |
                                     Kafka (durable log, replicated)
                         +--------------------+--------------------+
                         v                    v                    v
                  Raw event sink      Stream processor      Rule engine
                  (to ClickHouse)     (windows, state)      (conditions)
                         |                    |                    |
                         +---> ClickHouse <---+                    v
                                   |                      Notifier -> webhook/Slack
                                   v
                           Query API -> Dashboard (live) and ad hoc queries
```

1. **Send:** ShopKart's checkout service sends `payment.failed` events with an `event_id` and a timestamp. The API replies `202` within milliseconds.
2. **Protect:** the API checks the key, applies ShopKart's rate limit, and validates the event against its schema. Bad events get a clear error and never enter the pipeline.
3. **Buffer:** the event is appended to Kafka, replicated across brokers. Acknowledgment happens only after that, so "accepted" means "will not be lost."
4. **Deduplicate:** if ShopKart's service retries a request, the same `event_id` is recognized and counted once.
5. **Process:** the stream processor groups events into one-minute windows by *event time* (when it happened, not when it arrived) and computes `payment.failed` counts, even if some events arrive late or out of order.
6. **Store:** raw events and aggregates are written to ClickHouse in batches, so queries over hundreds of millions of rows return in milliseconds.
7. **Act:** a rule "payment failures above 100 per minute for 2 minutes" fires once. The notifier sends a signed webhook to ShopKart's alerting channel, retrying on failure and never sending the same alert twice.
8. **See:** the dashboard updates live (throttled), showing failures per minute, data freshness, and the active alert. An engineer can query raw failed events to see which provider and country are affected.
9. **Survive:** a broker dies mid-sale; consumers restart; nothing is lost and nothing is double counted, and ShopKart's competitors on the same platform never notice because limits and isolation protect them.

## 6. Features, and Why Each One Matters

| Area | Feature | Why a customer cares | Stage |
| --- | --- | --- | --- |
| Ingestion | Fast HTTP API, batch endpoint, API keys, 202 acknowledgment | Senders are never slowed down by analytics | 1-2 |
| Reliability | Durable queue, retries, dead-letter queue and replay | Events survive crashes and bad messages | 2-3 |
| Correctness | Idempotency keys, dedup, outbox, schema registry | Numbers can be trusted; formats can evolve safely | 3 |
| Analytics | Event-time windows, watermarks, late-data handling | Correct metrics even with delays and disorder | 4 |
| Storage and query | ClickHouse, query API | Fast dashboards and history over huge datasets | 4 |
| Protection | Per-tenant rate limits, quotas, isolation | One noisy customer cannot break others | 5 |
| Automation | Rule engine, deduplicated notifications, signed webhooks | Problems are detected and acted on automatically | 5 |
| Scale | Sharding, hot-partition handling, replicas, pooling | Grows with load; moves capacity safely | 6 |
| Operations | Autoscaling, observability, SLOs, runbooks, chaos-tested | Runs reliably and failures are understood | 7 |
| Efficiency | Profiled ingest path, cost model | Known capacity and known cost | 8 |
| Visibility | Real-time dashboard with throttled updates | Humans can watch it working | 9 |
| Depth | A self-built distributed component | Shows understanding below the tools | 10 |

## 7. "Isn't This Just Storing Events in a Database?"

| Need | Plain database approach | EventPulse approach |
| --- | --- | --- |
| Absorb bursts | Writes pile up, pool exhausts, requests fail | Durable log absorbs bursts; consumers catch up |
| Live metrics | Re-scan growing tables on every query | Incremental windowed aggregates, queried in milliseconds |
| Alerts | Poll with cron, delayed and brittle | Rules evaluated on the stream, fired once |
| Retries and duplicates | Double counts or lost writes | Idempotent end to end, proven by tests |
| Late and out-of-order data | Wrong time-based numbers | Event-time windows with an explicit lateness policy |
| Multiple customers | Shared tables, no protection | Per-tenant limits, quotas, isolation |
| Failures | Mostly manual recovery | Defined behavior for every dependency, tested with chaos |
| Scaling | Vertical scaling, then rewrite | Horizontal at each layer with measured rebalancing |

Storing and fetching is *one small part* of the system. The value is in the live computation, the guarantees, and the protection.

## 8. Use Cases

| Use case | Events | What is computed | Action |
| --- | --- | --- | --- |
| Payment health monitoring | `payment.authorized`, `payment.failed` | Failure rate per minute by provider and country | Alert when failure rate exceeds a threshold for several minutes |
| E-commerce live operations | `order.created`, `cart.abandoned`, `item.shipped` | Orders per minute, conversion funnel counts | Notify on sudden drop in orders |
| SaaS product analytics | `user.signed_up`, `feature.used` | Daily active users, feature adoption counts | Trigger onboarding or churn workflows |
| DevOps and reliability | `deploy.finished`, `service.error`, `job.failed` | Error counts per service and release | Alert and annotate dashboards after a deploy |
| IoT and telemetry | `sensor.reading`, `device.offline` | Average temperature per site per window, missing heartbeats | Webhook to maintenance system |
| Security signals | `login.failed`, `api_key.created` | Failed logins per user or IP per minute | Lock account or notify security |
| Logistics and ride-hailing | `driver.location_updated`, `ride.requested` | Supply and demand per area per minute | Trigger surge or rebalance |
| Gaming and ads | `level.completed`, `ad.impression`, `ad.click` | Live counts, click-through rates | Detect anomalies, budget caps |

### Two worked rule examples

```
Rule: payment-failure-spike
  metric: count of payment.failed, window 1m, per tenant
  condition: > 100 for 2 consecutive windows, cooldown 5m
  action: signed webhook to https://shopkart.example/alerts

Rule: sensor-offline
  metric: absence of sensor.reading from a device for 5m
  action: create maintenance ticket via webhook
```

## 9. The Bigger Picture

### Where it sits in a data stack

```
Sources (apps, devices) -> Ingestion -> Durable stream -> Processing -> Serving (queries, dashboards) + Actions (alerts)
                           EventPulse covers this whole spine for event-shaped data.
```

It complements other systems: the transactional database keeps the source of truth for business records; a data warehouse handles long-term, cross-source analysis; EventPulse handles the *live, high-volume, time-sensitive* slice.

### Alternatives in the market

- **Product analytics and event routing:** Segment, RudderStack, Snowplow, Mixpanel, Amplitude, PostHog.
- **Observability:** Datadog, Grafana Cloud, Honeycomb, Better Stack.
- **Building blocks:** managed Kafka (Confluent Cloud, MSK, Redpanda), Flink or managed stream processors, ClickHouse Cloud, Tinybird.
- **Alerting and workflows:** PagerDuty, EventBridge, workflow engines.

EventPulse is a small, opinionated combination of those building blocks. Large engineering organizations have publicly described similar internal pipelines built from the same ingredients (a durable log, stream processing, and a fast analytical store); verify specifics before quoting any company in an interview.

### Why would anyone use this instead of a vendor?

Honest answers, from strongest to weakest:

1. **Control and customization:** custom rules, custom aggregation logic, and a data model you own.
2. **Cost at volume:** per-event vendor pricing can become expensive; a self-run pipeline with known cost per million events can be cheaper at scale.
3. **Data residency and self-hosting:** some teams cannot send data to third parties.
4. **Integration depth:** one pipeline for ingestion, analytics, and action rather than stitching several products.

And the candid caveat: mature vendors already cover most of this well. For you, EventPulse's main value is as a **vehicle for learning and proving distributed systems engineering**, not as a company-grade product. A narrow differentiated angle (self-hostable, cheaper, specialized for one domain) would be needed to turn it into a business.

## 10. Using the API: A Walkthrough

```
# 1. Get an API key (admin)
POST /v1/admin/api-keys                        -> { "key": "ep_live_..." }

# 2. Optionally register an event type schema
PUT  /v1/event-types/payment.failed            (fields: amount number, currency string, ...)

# 3. Send events
curl -X POST https://api.eventpulse.example/v1/events \
  -H "Authorization: Bearer ep_live_..." -H "Idempotency-Key: 6f1c..." \
  -d '{ "event_id":"6f1c...", "type":"payment.failed", "occurred_at":"2026-10-07T10:15:03Z",
        "schema_version":1, "payload":{"amount":49.99,"currency":"INR"} }'
-> 202 Accepted

# 4. Create a rule
POST /v1/rules   { metric, window, condition, for_windows, cooldown_s, action }

# 5. Query results
GET /v1/aggregates?type=payment.failed&from=...&to=...&interval=1m
GET /v1/events?type=payment.failed&from=...&to=...&limit=100
```

## 11. Interview Answers You Can Adapt

**"What does EventPulse do?"** It ingests events from applications, processes them as a stream, and turns them into live metrics, alerts, and queryable history, reliably and at scale. I treated it as a way to study real distributed systems problems: durability, deduplication, event-time processing, tenant isolation, scaling, and failure handling.

**"Is there an actual use case, or is it a toy?"** The use case is real and common: any product that needs to know what is happening right now and react automatically, like payment failure monitoring, order flow tracking, device telemetry, or security signals. Companies solve it with a combination of a durable log, stream processing, and an analytical database, either self-built or through vendors. My system implements that architecture end to end with measured behavior. I am not claiming it beats mature vendors; I built it to understand and demonstrate the engineering underneath them.

**"Why not just use Postgres?"** Postgres is great for the transactional parts and I use it for metadata. For high-rate events, synchronous writes in the request path hit connection pool and write limits, and aggregations over large tables are slow. I measured that in my baseline (Stage 1) and then showed what the queue and columnar store changed, with numbers.

**"Why not Datadog, Segment, or PostHog?"** They are excellent, and for many teams they are the right answer. A custom pipeline makes sense when you need control over rules and data, cost predictability at high volume, or self-hosting. I built the equivalent to learn how they work, and I can compare my measured cost per million events against vendor pricing.

**"Who is the customer?"** A product team that emits events and wants live metrics and automatic alerts without building a streaming platform: for example an online store watching payment failures, or an IoT operator watching device health.

**"What is missing or what would you change?"** Multi-region deployment, richer query language, source connectors for third-party webhooks, billing and quotas per plan, and a managed schema workflow. I scoped those out deliberately to go deep on correctness, scale, and failure behavior.

**"How do you demo it?"** I run a simulated tenant (an online store) sending steady traffic, spike it, flood one tenant to show isolation, kill a broker, and show alerts firing, recovery, and a zero-loss audit.

## 12. Glossary

| Term | Meaning |
| --- | --- |
| Event | A record that something happened at a time |
| Producer | The application sending events |
| Tenant | A customer of the platform, isolated from others |
| Topic and partition | A named log in Kafka, split into ordered parts for parallelism |
| Offset | A position in a partition |
| Consumer group | Consumers that share the work of reading a topic |
| Aggregate | A computed summary such as a count per minute |
| Window | A time bucket used for aggregation (tumbling, sliding) |
| Event time vs processing time | When it happened vs when the system saw it |
| Watermark | The system's estimate of how far event time has progressed |
| Idempotency | Repeating an operation has the same effect as doing it once |
| DLQ | Dead-letter queue holding messages that could not be processed |
| Backpressure | Slowing or shedding input when downstream cannot keep up |
| SLO | A reliability target such as 99.9% availability |

## 13. One-Page Cheat Sheet

- **Problem:** high-volume application events must become trustworthy live metrics and automatic actions, without losing, duplicating, or letting one customer harm another.
- **Solution:** durable log, stream processing with event-time windows, a fast analytical store, a rule engine with idempotent actions, rate limiting and isolation, all operated with SLOs.
- **Input:** JSON events with a strict envelope and flexible, optionally schema-governed payload.
- **Output:** live aggregates, history queries, dashboards, and signed webhooks.
- **Differentiators in your story:** measured numbers, delivery guarantees you can prove, failure experiments, a cost model, and a depth component you built yourself.
- **Honest limits:** 1M rps is modeled, not measured; mature vendors exist; the project is a proof of engineering skill first.
