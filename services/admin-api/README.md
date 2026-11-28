# admin-api (Stage 3, S3-F4)

Tenant config admin API. A config change and the `config.changed` event announcing it are written in **one
Postgres transaction** (transactional outbox, [ADR 0007](../../docs/adr/0007-polling-outbox-vs-cdc.md)). The event is
then published by [`services/outbox-relay`](../outbox-relay/README.md).

**Why a separate service, not a module of ingest-api:** the ingest hot path has no dependency on Postgres. Its
failure modes (Kafka down: 503, Redis down: fail open) are the ones measured in Stage 2 and 3. Putting config
writes there would add a database connection pool and a new way to fail to every ingest instance.

## Endpoints

All routes under `/v1/admin` need `Authorization: Bearer <ADMIN_API_KEY>` (not a tenant key).

| Method | Path | What |
| --- | --- | --- |
| `GET` | `/v1/admin/tenants/:tenantId/config` | `{tenant_id, config, version, updated_at}`, `ETag: "<version>"`. Version 0 and `{}` when the tenant has no config yet. 404 for an unknown tenant. |
| `PUT` | `/v1/admin/tenants/:tenantId/config` | Body `{config}` replaces the whole document. Optional `If-Match: "<version>"`. Returns `{..., version, event_id, outbox_id}` and the new `ETag`. |
| `GET` | `/healthz`, `/readyz`, `/metrics`, `/debug/stats` | `readyz` = Postgres answers and the `config.changed-value` schema id is known. |
| `GET/POST/DELETE` | `/debug/faults` | Fault injector (needs `DEBUG_FAULTS=true`). |

**PUT, in one transaction:**

1. Lock the tenant row (`FOR UPDATE OF tenants`). This serializes the changes of one tenant, the first one included. Unknown tenant: 404.
2. Check `If-Match` against the current version. Mismatch: **412**, with `current_version` in the body.
3. Upsert `tenant_config` with `version = version + 1`.
4. `enqueue()` the outbox row: Avro `config.changed-value` (`schemas/config/v1.avsc`), key = tenant, headers `event_id`, `version`, `content-type`. It carries the full config, not a diff.
5. Crash point `admin:after-update-before-commit`, then `COMMIT`.

**Responses:**

| Status | When |
| --- | --- |
| 200 | Committed. The event will be published at least once. |
| 400 | Unknown config key, wrong type, value out of range, malformed `If-Match`, bad tenant id. |
| 401 | Missing or wrong admin key. |
| 404 | Unknown tenant. Tenants come from `make seed-tenants`. |
| 412 | Stale `If-Match`. |
| 413 | Body over `BODY_LIMIT` (16 kb). |
| 503 + `Retry-After` | Postgres unreachable, or the schema id not known yet. The change did **not** happen. |
| 500 | Anything else. Rolled back: the change did not happen. |

**`If-Match`:**

| Value | Meaning |
| --- | --- |
| absent | Last writer wins. |
| `"3"`, `W/"3"`, or a list | The current version must be one of them. |
| `"0"` | Only if the tenant has no config yet. |
| `*` | Only if a config exists. |

**No-op PUT** (same document as stored): it still gets a new version and publishes. That is simpler than comparing documents, and harmless, because consumers apply the same snapshot again. The version counts accepted PUTs, not distinct configs.

## Config document

Known keys only. An unknown key is a 400, so a typo cannot become a config nobody reads. Every key is optional.

| Key | Type |
| --- | --- |
| `rate_limit_per_s` | int 1..1,000,000 |
| `retention_days` | int 1..3650 |
| `allowed_event_types` | up to 200 strings of `[A-Za-z0-9_.:-]`, each at most 64 characters |
| `pii_fields` | up to 100 strings, each at most 128 characters |
| `features` | up to 50 snake_case keys, boolean values |
| `paused` | boolean |

Add a key in `src/tenant-config/tenant-config.dto.ts`. The event carries the document as JSON text (`config_json`), so the Avro schema does not change when a config key is added.

## Run

```bash
cp .env.example .env                          # ADMIN_API_KEY (>= 16 chars), DATABASE_URL (same database as the sink)
make migrate && make schemas && make seed-tenants
make admin-start                              # :8020
curl -s -X PUT localhost:8020/v1/admin/tenants/acme/config -H "authorization: Bearer $ADMIN_API_KEY" \
  -H 'content-type: application/json' -H 'if-match: "0"' -d '{"config":{"rate_limit_per_s":500}}'
```

`make up` starts it too, when `services/admin-api/.env` exists.

## Metrics

| Metric | What |
| --- | --- |
| `ep_admin_config_puts_total{result}` | `ok`, `invalid`, `not_found`, `precondition_failed`, `unavailable`, `error` |
| `ep_admin_config_put_seconds` | PUT latency (lock, upsert, enqueue, commit) |
| `ep_outbox_enqueued_total{topic}` | Committed outbox rows |

## Tests

- Unit (`pnpm test`): the DTO (every rejection is a 400) and `If-Match`.
- End to end, as a real process: `tests/correctness/test/outbox.e2e-spec.ts` covers the PUT, 412/404/400/401, rollback, and the crash point. The crash matrix covers `admin:after-update-before-commit`.
