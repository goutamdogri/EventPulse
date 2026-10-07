# EventPulse Stage 9: Frontend and Real-Time

*Duration: about 2 weeks. Keep it light. Goal: make the system visible with a React dashboard fed by a real-time gateway, and prove that a spike in event volume cannot freeze the browser or take down the gateway. This stage matters least for the skill goal, so the aim is a convincing demo, not a product.*

## 1. Objective and Motivating Pain

Stage 8 completed the backend story. There is still no product surface: nobody can watch events, aggregates, and alerts move. The naive way to build one, pushing every update to every browser, fails exactly when it matters (a spike). The real lesson is backpressure at the last mile: the browser is the slowest consumer in the system.

By the end of this stage you must be able to:

1. Stream aggregates (never raw events) to a browser over WebSocket and SSE, and explain when you would choose each.
2. Show server-side throttling and conflation keeping both the gateway and the browser healthy under a 100x spike.
3. Handle slow clients, reconnects, gaps, and resumption correctly, with a stale-data indicator instead of silent failure.
4. Scale the gateway horizontally and show the cost of fan-out.
5. Produce the live demo and the script for the 5-minute video that closes the project.

**Predictions to write down first:** how many updates per second can a browser comfortably render in your chart? What happens to gateway memory when a client stops reading? How many WebSocket connections can one gateway node hold before CPU or memory limits it?

## 2. Prerequisites and Setup

| Item | Requirement |
| --- | --- |
| Stages 0-8 | Complete: aggregate stream `aggregates.out`, Query API over ClickHouse, rule engine alerts, deployable platform, demo environment |
| Frontend | React with TypeScript and Vite; a lightweight state store (for example Zustand) or `useSyncExternalStore`; a performance-oriented charting library (compare an SVG-based library with a canvas-based one such as uPlot) |
| Gateway | Node service using the `ws` library for WebSockets and plain HTTP streaming for SSE |
| Testing | React Testing Library, Playwright for end-to-end, browser DevTools Performance panel with CPU throttling, k6 with its WebSocket support for connection load |
| Demo | A traffic scenario script (steady load, a spike, a noisy tenant, a chaos event) from your existing k6 and chaos tooling |

## 3. Learning Plan

### Real-time transports

- **Polling and long polling:** simplest, wasteful, higher latency. Useful as a baseline and a fallback.
- **Server-Sent Events (SSE):** one-way server to client over HTTP, automatic reconnect, `Last-Event-ID` for resumption, text only. Over HTTP/1.1 browsers limit connections per host, so many open tabs can starve; HTTP/2 multiplexing largely removes this. Some proxies buffer streaming responses and need configuration.
- **WebSocket:** full duplex over one upgraded connection. You must build your own heartbeats, reconnect logic, and message framing. The browser API cannot set arbitrary headers on the handshake, so authentication uses a short-lived ticket in the URL, a cookie, or a first message.
- **Choosing:** a dashboard that mainly receives updates is a good fit for SSE; subscription changes and client messages favor WebSocket. You will implement both and compare.

### Gateway design

- **Fan-out:** one aggregate must reach every subscriber of that tenant and metric. Each gateway instance needs the stream data its subscribers want. Options: each instance consumes the aggregate topic with its own consumer group, or aggregates are published through Redis pub/sub. Know the cost of each.
- **State:** the gateway keeps the latest value per key so it can send an immediate snapshot on subscribe. It rebuilds this on start from the Query API or by reading the stream.
- **Throttling and conflation:** for fast-changing state, send at most N updates per second per subscription, and when several updates occur between sends, keep only the latest per key. Dropping intermediate values is correct for dashboards because only the current state matters.
- **Backpressure:** each connection has an outgoing buffer (`bufferedAmount` for WebSocket). If it grows, stop adding to it, conflate harder, and disconnect clients that stay slow, with an explicit reason code.
- **Sequencing and resync:** per-subscription sequence numbers let a client detect gaps and request a snapshot instead of showing wrong data.
- **Scaling limits:** file descriptors, memory per connection, CPU for heartbeats and serialization, load balancer idle timeouts, and the reconnect storm when a gateway restarts.
- **Security:** authorize every subscription against the tenant in the ticket, never against a client-supplied tenant id; cap subscriptions and message sizes per connection; rate limit connection attempts per tenant.

### Browser rendering

- **Never render on every message.** Buffer incoming data and update state at a fixed rate, aligned with animation frames.
- **React costs:** frequent state updates cause re-renders of whole component trees. Keep hot data in a store outside React state, memoize, and subscribe components to narrow slices.
- **Charts:** SVG with many points is slow; canvas scales better. Cap the number of points with a ring buffer.
- **Resources:** avoid unbounded arrays, pause updates when the tab is hidden, and clean up listeners.
- **Reconnect behavior:** exponential backoff with jitter so a gateway restart does not cause a thundering herd.

### Reading

MDN documentation on `WebSocket`, `EventSource`, and `requestAnimationFrame`. The WebSocket chapter of *High Performance Browser Networking*. React documentation on rendering performance and external stores. Chrome DevTools documentation on the Performance panel and long tasks. In `notebook/realtime-notes.md`, write down why conflation is correct for a dashboard but wrong for an audit log.

## 4. Requirements

| ID | Requirement |
| --- | --- |
| S9-F1 | A realtime gateway service with WebSocket and SSE endpoints; clients subscribe to (tenant, metric, window) and receive a snapshot, then updates |
| S9-F2 | Server-side throttling and per-key conflation with a configurable maximum update rate per subscription (default 4 per second) |
| S9-F3 | Per-connection backpressure policy: conflate, stop sending when the buffer is above a limit, disconnect persistently slow clients with a reason |
| S9-F4 | Sequence numbers, resume after reconnect, and a resync (fresh snapshot) when a gap is detected |
| S9-F5 | Authentication by short-lived ticket scoped to a tenant; subscription authorization; limits on connections and subscriptions |
| S9-F6 | At least two gateway instances behind the load balancer, with a documented fan-out choice (own consumer group per instance, or Redis pub/sub) |
| S9-F7 | A React dashboard with three views: Overview (live throughput by event type, top types, freshness), Alerts (active and recent from the rule engine), and Explore (historical queries with a time range picker over the Query API) |
| S9-F8 | A visible data-age indicator and connection status; the UI marks data as stale when updates stop, instead of showing a flat line |
| S9-F9 | Performance budget: the dashboard stays smooth and memory stable for an hour under a high-rate input |
| S9-F10 | Scripted demo scenarios: steady load, a spike, a noisy tenant being rate limited, and a chaos event, ready for the recorded video |

### Performance budget (hypotheses to replace with data)

| Area | Target |
| --- | --- |
| Render rate | about 55 frames per second or better on a mid-range laptop during a spike |
| Long tasks | main-thread tasks over 50 ms are rare and recorded |
| Memory | JS heap stable (no growth trend) over 1 hour |
| Update rate to browser | bounded by design (for example 4 per second per subscription) regardless of input rate |
| Data age | displayed end-to-end freshness consistent with your 5 s freshness SLO |
| Gateway | connections per node and per-connection memory measured, with the limiting resource named |

## 5. Design

```
aggregates.out (Kafka) ---> Gateway node A (group gw-A) --\
                       \--> Gateway node B (group gw-B) ---+--> WS/SSE --> Browser(s)
                                                            |
Browser -> Query API (history, snapshots) -> ClickHouse
Browser -> API: POST /v1/realtime/ticket  (short-lived, tenant-scoped)
Alerts: rule engine state changes -> topic -> gateway -> subscribers

Gateway internals:
  latest[(tenant,type,window)] = value      (for snapshots)
  subscription.dirty = Map<key, value>      (conflation buffer)
  tick at 1/RATE_HZ: send dirty entries, clear
```

### Message protocol (sketch)

```
client -> {"type":"subscribe","id":"s1","metric":"count","window":"1m","types":["order.created"]}
server -> {"type":"snapshot","sub":"s1","seq":1000,"data":[{"key":"...","v":42}]}
server -> {"type":"update","sub":"s1","seq":1001,"data":[{"key":"...","v":43}]}
server -> {"type":"resync","sub":"s1"}          // client must request a new snapshot
server -> {"type":"error","code":"forbidden|slow|limit","message":"..."}
client -> {"type":"ping"} / server -> {"type":"pong"}
```

The tenant is taken from the ticket, never from the subscribe message.

### Throttle and backpressure sketch (gateway)

```ts
const MAX_BUFFER = 1_000_000;           // bytes queued on a socket
const RATE_HZ = Number(process.env.RATE_HZ ?? 4);

setInterval(() => {
  for (const sub of subscriptions) {
    if (sub.dirty.size === 0) continue;
    if (sub.socket.bufferedAmount > MAX_BUFFER) {          // client is not keeping up
      if (++sub.slowTicks > SLOW_LIMIT) closeSlow(sub);     // explicit policy
      continue;                                             // keep conflating, send nothing
    }
    sub.slowTicks = 0;
    const data = [...sub.dirty.entries()].map(([key, v]) => ({ key, v }));
    sub.dirty.clear();
    sub.socket.send(JSON.stringify({ type: 'update', sub: sub.id, seq: ++sub.seq, data }));
  }
}, 1000 / RATE_HZ);
```

Think about serializing once per distinct payload for many subscribers of the same data, and measure whether it matters.

### Client store sketch

- Incoming messages write into a plain mutable buffer (not React state).
- A `requestAnimationFrame` loop, limited to about 10 updates per second, copies buffered values into the store and appends to a fixed-size ring buffer for chart series.
- Components subscribe to narrow slices. A "last update time" value drives the data-age indicator.
- On disconnect: show status, retry with exponential backoff and jitter, resume with the last sequence number, and accept a resync.

### Repo additions

```
services/realtime-gateway/ (ws and sse handlers, state, throttle, auth, metrics)
apps/dashboard/ (Vite, React, TypeScript: views, store, charts, connection layer)
tools/demo/ (traffic scenarios and chaos triggers for the video)
tests/e2e/ (Playwright)  tests/ws-load/ (k6 WebSocket scenarios)
docs/ (realtime-protocol.md, perf-budget.md, demo-script.md, adr/)
```

## 6. Step-by-Step Build Plan

1. **Gateway skeleton:** ticket endpoint, authentication, protocol parser, metrics.
2. **Aggregate consumption and snapshot state** with the chosen fan-out approach.
3. **WebSocket and SSE endpoints** sharing the same core logic.
4. **Throttling and conflation.**
5. **Backpressure and slow-client policy.**
6. **Sequence numbers, resume, and resync.**
7. **React app:** connection layer, store, Overview view, charts with ring buffers, stale-data indicator.
8. **Performance pass:** profile with CPU throttling, fix re-render and chart costs against the budget.
9. **Second gateway instance** behind the load balancer; tune WebSocket upgrade and idle timeouts.
10. **Alerts and Explore views.**
11. **Demo scenarios and the break-it catalog.**
12. **Write up and record the demo.**

## 7. Test Plan

| Layer | Tests | Pass condition |
| --- | --- | --- |
| Unit | Conflation keeps the latest value per key; sequence and resync logic; ring buffer bounds; protocol validation | Spec behavior |
| Integration | WebSocket and SSE deliver identical data for the same subscription | Same snapshot and update content |
| Security | Invalid, expired, and reused tickets; subscribing to another tenant; too many subscriptions; oversized messages; connection floods | Rejected with correct codes |
| End to end (Playwright) | Dashboard shows live data; killing the gateway shows an offline state then recovers; stalled data shows stale indicator | Behaviors as specified |
| Consistency | After quiet time, dashboard values equal Query API results for the same windows | Equal |
| Browser performance | Headless or manual runs with CPU throttling (for example 4x) under a high input rate | Meets the performance budget |
| Connection load | k6 WebSocket scenarios at 1k, 5k, 10k connections per node | Resource limits identified |
| Soak | Dashboard open for 1 hour or more at a steady high input rate | Stable memory and frame rate |

## 8. Break-It Experiment Catalog

Write the hypothesis first and record before and after numbers.

### U1 Spike flood

- **Hypothesis:** pushing every update to the browser freezes the UI at a rate far below your input rate; with server-side conflation the UI remains smooth regardless of input.
- **How:** raise the aggregate update rate 100x with throttling disabled, then enabled.
- **Observe:** frame rate, long tasks, JS heap, gateway CPU and bytes out, conflation ratio (updates in versus updates sent).
- **Explains:** why "never push every event to the browser" is a design rule, with your own evidence.

### U2 Slow and stalled clients

- **Hypothesis:** a client that stops reading (paused tab, throttled CPU, blocked network) causes the gateway's per-connection buffer to grow without bound unless a policy exists.
- **How:** use a client that does not read, browser CPU throttling at 6x, and network throttling; run with and without the backpressure policy.
- **Observe:** gateway memory, buffer sizes, slow disconnects, effect on other clients.
- **Explains:** isolation between subscribers and the cost of one slow consumer.

### U3 Reconnect storm

- **Hypothesis:** killing a gateway makes all its clients reconnect at once, hammering the snapshot path and the Query API; backoff with jitter spreads the load.
- **How:** kill a gateway pod under thousands of connections, with fixed-delay and jittered reconnects.
- **Observe:** reconnect rate, snapshot latency, Query API and Redis load, time to normal.
- **Explains:** the thundering herd again, now at the edge.

### U4 Gaps and resync

- **Hypothesis:** dropped or reordered updates cause a wrong chart unless sequence numbers detect gaps and trigger a snapshot.
- **How:** drop messages in a proxy, delay some, partition the client briefly.
- **Observe:** whether the UI ever shows incorrect values, resync frequency, final consistency with the Query API.
- **Explains:** correctness at the last mile.

### U5 Connection scale and idle timeouts

- **Hypothesis:** one node's connection capacity is limited by file descriptors, memory per connection, or heartbeat CPU; load balancers close idle connections unless heartbeats run.
- **How:** ramp connections with k6; vary heartbeat intervals; set short proxy idle timeouts.
- **Observe:** connections at the limit and which resource saturates, memory per connection, unexpected disconnect rates.
- **Explains:** capacity per gateway node and the heartbeat design.

### U6 Fan-out and serialization cost

- **Hypothesis:** with many subscribers per tenant, per-client `JSON.stringify` dominates CPU; serializing once and sharing the payload, or enabling compression, shifts the balance between CPU and bandwidth.
- **How:** 1, 100, and 1000 subscribers on the same data, with different serialization strategies and per-message compression on or off.
- **Observe:** gateway CPU, bytes out, latency.
- **Explains:** the cost curve of fan-out and your chosen strategy.

### U7 Browser memory and background tabs

- **Hypothesis:** unbounded series arrays leak memory over hours; hidden tabs waste CPU unless updates pause.
- **How:** run a long session, take heap snapshots, then switch to ring buffers and visibility-based pausing.
- **Observe:** heap over time, CPU in background tabs.
- **Explains:** long-running UI hygiene.

### U8 Rendering strategies

- **Hypothesis:** re-rendering on every message is far costlier than batching to a fixed tick; canvas charts handle more points than SVG.
- **How:** compare per-message state updates, fixed-rate batching, and animation-frame batching; compare chart implementations.
- **Observe:** frame rate, main-thread time, React commit counts.
- **Explains:** where the browser's budget goes.

### U9 Authorization abuse

- **Hypothesis:** if the tenant is read from the subscribe message, a client can read another tenant's data; tickets that are long-lived or reusable widen the risk.
- **How:** attempt cross-tenant subscribes, ticket replay, subscription floods, and giant messages against a deliberately weak build, then the real one.
- **Observe:** what leaks, what gets limited, audit logs.
- **Explains:** why the tenant comes only from a server-issued ticket.

### U10 SSE versus WebSocket

- **Hypothesis:** SSE is simpler and reconnects automatically but is one-way and constrained by per-host connection limits on HTTP/1.1; WebSocket needs more custom code but handles bidirectional needs cleanly.
- **How:** run the same dashboard on each transport, behind your load balancer and through a restrictive proxy, with many tabs open.
- **Observe:** reconnect behavior, connection limits, proxy compatibility, code size, overhead.
- **Explains:** a transport decision you can defend, recorded as an ADR.

### U11 Backend degradation shown honestly

- **Hypothesis:** if the processor stalls or the stream stops, a naive UI keeps showing the last value as if live; a data-age indicator makes the failure visible.
- **How:** stop the processor, slow the Query API, and kill the gateway while the dashboard is open.
- **Observe:** whether the UI clearly shows stale data, offline status, and recovery.
- **Explains:** observability for end users, and a good demo moment.

### U12 Chaos with the dashboard open

- **Hypothesis:** during a Stage 7 chaos experiment (broker or pod kill), the dashboard shows the freshness SLO breach and active alerts, then recovers without a page reload.
- **How:** run the experiment with the dashboard on screen and recording.
- **Observe:** what the user sees, how long recovery takes, any wrong data.
- **Explains:** and provides footage for the video.

## 9. Metrics and Diagnostics

**Gateway:** connections, messages and bytes out per second, per-connection send buffer size, conflation ratio, slow disconnects, resyncs, reconnects per second, memory per connection, CPU, event loop delay.

**Browser:** frame rate, long task count and duration (Performance API `longtask` entries), JS heap, render commit count, update-to-display latency, reconnect count.

**End to end:** displayed data age versus actual freshness measured with marker events; correctness check against the Query API.

## 10. Artifacts

1. **Live dashboard demo** running on the small always-on demo environment.
2. **Realtime design note and protocol spec:** transports, fan-out choice, throttling, backpressure, resync, security.
3. **Performance budget report** with before and after charts for U1 and U2 (frame rate, long tasks, memory, gateway buffers).
4. **ADRs:** SSE versus WebSocket, fan-out via consumer groups versus Redis pub/sub, throttle rate, chart library.
5. **The demo video script** (below) and the recorded 5-minute video.
6. **Optional blog post:** "Never push every event to the browser."

### 5-minute demo video outline

| Time | Scene | Show |
| --- | --- | --- |
| 0:00-0:30 | The problem and architecture | Diagram, one sentence per component |
| 0:30-1:15 | Live traffic | Dashboard with steady load, freshness indicator, trace of one event end to end |
| 1:15-2:00 | Spike | 100x spike, flat latency, smooth browser, conflation counters |
| 2:00-2:30 | Noisy neighbor | One tenant floods, rate limiting engages, other tenants unaffected |
| 2:30-3:30 | Chaos | Kill a broker or pods under load, alerts fire, recovery, audit shows no loss |
| 3:30-4:15 | Scaling and cost | 1, 2, 4, 8 node chart, bottleneck, cost per million events |
| 4:15-5:00 | Guarantees and evidence | Delivery guarantees document, crash matrix, link to repo and reports |

## 11. Common Pitfalls

- Pushing raw events or every aggregate update to the browser.
- Storing high-rate data in React state and re-rendering on each message.
- Unbounded buffers, on the server (per-connection) or in the client (series arrays).
- No heartbeats, so load balancers silently drop idle connections.
- Reconnect loops without backoff and jitter.
- Trusting a client-supplied tenant id.
- Showing stale data as if live.
- Over-investing in UI polish. This stage is a demo vehicle for the backend; stop when the budget and the demo are met.
- Testing with one client and one tab and concluding the design scales.

## 12. Exit Criteria

- [ ] Gateway serves snapshots and throttled updates over both WebSocket and SSE.
- [ ] Conflation, backpressure, resume, and resync implemented and tested.
- [ ] Authorization tests pass; no cross-tenant access.
- [ ] Dashboard meets the performance budget during a 100x spike and over a one-hour soak.
- [ ] Two gateway instances run behind the load balancer; per-node connection capacity measured.
- [ ] Experiments U1 through U12 recorded with before and after data.
- [ ] Stale-data and offline states shown correctly during failures.
- [ ] Demo scenarios scripted; the 5-minute video recorded.
- [ ] Repo tagged `stage-9-complete`.

**Self-test questions (answer aloud, without notes):**

1. Why is conflation correct for a dashboard, and when would it be wrong?
2. What happens inside your gateway when a client stops reading, and why does that matter for other clients?
3. How do you authenticate a WebSocket from a browser, and why must the tenant not come from the subscribe message?
4. Why do many clients reconnecting at once hurt, and what did you do about it?
5. Why SSE or why WebSocket for your dashboard?
6. How does your UI behave when the processor is down, and how can the user tell?

## 13. Handoff to Stage 10

The platform is now complete end to end: ingest, correctness, analytics, protection, scaling, operations, performance, and a visible product. What turns an excellent project into a rare one is one area where you went deeper than any library required.

Stage 10 is the depth project: implement one small distributed component yourself and prove it correct under faults. Choose ONE:

- A Raft-based replicated log (following the MIT 6.824 labs).
- A log-structured storage engine.
- Your own mini stream-processing windowing engine, extending the Stage 4 processor.
- Or a deep Kafka or ClickHouse internals study built on experiments.

Before starting, write down which one you would choose, why it connects to what you have already built, how you would test it under faults, and what a public write-up would demonstrate. Also reserve time for the final deliverables: the repo README and diagrams, the blog posts, and the video.

When you are ready, say **Stage 10**.
