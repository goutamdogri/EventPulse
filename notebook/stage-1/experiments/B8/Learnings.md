# B8 Learnings: what happens to a pool when the database goes away

The API does not talk to Postgres directly for every request. It keeps a pool of open connections and lends one to each request. A connection is a TCP socket plus a Postgres backend process behind it. When the database restarts or the network breaks, those sockets die, but the pool only finds out when it touches them. How fast it finds out, and what it does then, decides how long the API stays broken. The cycle in a restart:

1. Postgres gets a shutdown request and ends every client session.
2. Each pooled socket is closed from the other side. Idle connections report an error to the pool; connections that are lent out fail the query that is running on them.
3. The pool throws the dead connections away and opens new ones when requests ask for a connection.
4. While Postgres is down or starting, new connections fail (refused, closed, or an error code that says "starting up"). The request that asked for the connection gets an error, and the API answers 503.
5. When Postgres accepts connections again, the next request opens a connection and succeeds. The pool is back to normal as soon as it holds live connections again.

A network pause is different: nobody closes anything. Steps 1 and 2 do not happen, so the pool does not learn that its connections are dead.

See also: [node-pg-pool](../../learning/nodeAndNest/1.%20node-pg-pool.md), [http-status-semantics](../../learning/nodeAndNest/4.%20http-status-semantics.md), [postgres-process-per-connection](../../learning/Postgres/1.%20postgres-process-per-connection.md), [postgres-checkpoints](../../learning/Postgres/6.%20postgres-checkpoints.md). The hypothesis and predictions are in [Explanation.md](Explanation.md).

## Concepts

**Pool life cycle in node-pg.** The pool creates a connection when a request asks and no idle one exists, up to `max`. It lends it, takes it back on release, and closes idle ones after `idleTimeoutMillis`. A connection that is broken when it is released is removed instead of going back to the idle list. An idle connection that errors makes the pool emit an `error` event and removes it. So a restart should empty the pool by itself, one connection at a time, as they are found dead. Number that matters here: `max` = 25 in these experiments.

**Different ways a connection can fail.**

- Closed by the peer (FIN or RST): the next read or write fails at once. Fast and visible. This is what a database restart and a disabled proxy look like.
- Refused: nobody listens on the port. The connect attempt fails in about a millisecond.
- Black hole: packets go out and nothing comes back, and no error is sent. The socket looks open. The client only finds out through a timeout it set itself, or after the operating system gives up (TCP retransmissions, many minutes by default). This is what a cut cable or a firewall that drops packets looks like.

**Timeouts are layered and each one covers one thing.**

- Pool connection timeout (`connectionTimeoutMillis`): how long a request waits for a free or new connection. Here 2 s.
- `statement_timeout`: enforced by the Postgres server. If the packets never arrive, the server cannot cancel anything and the client never hears about it.
- A client-side query timeout would cover the black hole; the server here has none (not verified that the driver offers nothing else by default).
- The HTTP client timeout (k6: 10 s): the producer gives up, and does not know if the event was stored.
- TCP keepalive: probes an idle connection to find out if the peer is gone. Off by default in node-pg (not verified for this version).

**Postgres shutdown modes.** Smart shutdown waits for all sessions to end by themselves. Fast shutdown terminates the sessions, does a checkpoint and stops cleanly. Immediate shutdown (and a kill) skips the checkpoint, so the next start has to replay WAL (crash recovery). The official Docker image stops Postgres with the fast mode signal (the stop signal is recorded in `env.txt`). After a clean stop the start is short. After a kill the start includes recovery, and until it is done Postgres answers new connections with "the database system is starting up".

**SQLSTATE codes.** Postgres errors carry a five character code. The ones that matter here: `57P01` admin_shutdown (the server is shutting down, your session was terminated), `57P03` cannot_connect_now (starting up), class `08` connection exceptions, `53300` too many connections. Node errors have their own codes such as `ECONNREFUSED`, `ECONNRESET`, `ETIMEDOUT`. A connection that is closed while a query is running can arrive with no code at all, only the message "Connection terminated unexpectedly".

**A proxy between the API and Postgres.** Toxiproxy accepts the client's connection itself and opens a second one to Postgres. If Postgres is down, the client's connection to the proxy still succeeds and is then closed, so the client sees a reset or a termination instead of a refusal. If the proxy is disabled, its port is closed and the client sees a refusal. Toxics change what the proxy does with data; the `timeout` toxic holds data back and keeps the connection open.

**Acknowledged, lost, and unknown.** A 202 from this API means "committed" (synchronous commit is on). A 503 means "not committed, safe to retry". A timeout on the client side means "I do not know": the commit may or may not have happened. A producer that does not retry loses every event that did not get a 202. A producer that retries after an unknown result can create a duplicate unless the event has an id the server can recognise; the insert here uses `ON CONFLICT (event_id) DO NOTHING`, so a retry with the same `event_id` is safe.

**Little's law for waiting requests.** Requests in the system = arrival rate x time each spends in the system. If every request waits 2 s for a pool connection at 2000 req/s, about 4000 requests are in the system at once. This matters for the generator (k6 needs one virtual user per waiting request) and for the server's memory.

## Signals and what they mean

- Error code counter by code (server): which kind of failure. Cumulative; take a rate.
- Pool total, idle, waiting (instant): healthy = total near the number of live connections, idle above 0 under light load, waiting 0. A pool at max with idle 0 and waiting above 0 means every connection is lent out and not coming back.
- Pool connects and removes (cumulative): connects minus removes = connections alive. A restart should show removes first, then connects.
- Pool idle errors (cumulative): connections found dead while idle.
- `pg_up` of the exporter (instant): can the exporter query Postgres. It looks at Postgres, not at the API's path.
- Backends in `pg_stat_activity` (instant): sessions Postgres thinks exist. After a network pause Postgres can still count sessions whose client is gone.
- Commits per second of the database (cumulative counter): what Postgres really did.
- Client-side counts of 202, 503 and "other" (cumulative): what the producer experienced; "other" holds timeouts and resets, the unknown outcomes.
- Row count after the run: the ground truth. Rows minus 202s is zero when nothing odd happens, positive when events were committed but the client did not get the answer, negative would mean an acknowledged event is gone.

## Pitfalls

- The probe that asks Postgres directly does not see a network pause. Only the probe through the API does.
- k6 series are not marked stale after a run; the last value can stay on a panel for minutes.
- The postgres-exporter has no data while Postgres is restarting, so its panels have gaps instead of zeros.
- Requests that are waiting for a connection are not in the query-duration histogram, so a long outage lowers that panel instead of raising it.
- If k6 runs out of virtual users during a pause, it stops offering load: the offered rate is no longer constant and the loss percentage must be computed from what was really sent.
- A run ends with whatever state the pool is in. The next run starts a new server so the pool does not carry over.
- The proxy adds a little latency to every query. All variants use it, so they are comparable with each other, not with the runs that connected straight to Postgres.
- The probe loops use a little CPU on the monitoring cores in every run, the same in every variant.
