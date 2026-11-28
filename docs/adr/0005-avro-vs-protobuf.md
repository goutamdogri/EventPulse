# ADR 0005: Avro with a Schema Registry for events.raw; avsc plus our own small registry client

- **Status:** Accepted
- **Date:** 2026-11-28
- **Author:** Goutam Dogri
- **Stage:** 3 (Correctness), requirement S3-F7
- **Affects:** `schemas/`, `packages/schema`, `tools/schema-check`, `services/ingest-api/src/schema/`, `services/consumer-sink/src/sink/parse.ts`, `.github/workflows/ci.yml`

## Context

Producers (ingest API) and consumers (sink now, stream processors in Stage 4) deploy independently, and replays read old data with new code. Until Stage 3 the value on `events.raw` was free-form JSON governed by nothing but convention. S3-F7: every event uses a registered schema, and incompatible changes fail CI.

Two choices: the format, and the Node library.

## Format: options considered

| | Avro + Confluent Schema Registry | Protobuf + registry | JSON Schema + registry |
| --- | --- | --- | --- |
| Registry compatibility rules | most mature: BACKWARD/FORWARD/FULL (+ TRANSITIVE) defined by Avro schema resolution | supported, but the rules are field-number based and laxer (renames are free, `required` is gone in proto3) | supported, weakest semantics (open content models) |
| Reader/writer resolution | built in (defaults, skipped fields) | by field number, unknown fields kept | none (validation only) |
| Size | compact binary, no field names | compact binary | text |
| Tooling to learn | schema files in JSON, `avsc` | `.proto` + codegen | reuses JSON |

**Decision: Avro**, as the PRD recommends for the first implementation: the registry's compatibility checks are the most mature for it, which is exactly what S3-F7 is about. Protobuf is the Stage 8 comparison.

## Library: options considered (checked 2026-11-28)

| | `@confluentinc/schemaregistry` 1.11.0 | `@kafkajs/confluent-schema-registry` 4.1.0 | `avsc` 5.7.9 + our client (~150 lines) |
| --- | --- | --- | --- |
| Maintenance | Confluent, released with `@confluentinc/kafka-javascript` (last 2026-10) | community, last 2026-05 | `avsc` last 2025-07 (it is also what both libraries use underneath) |
| Dependencies | ~30 direct, including the AWS, Azure and GCP KMS SDKs, Vault, axios, JSONata (field-level encryption, data contracts) | `mappersmith`, `protobufjs`, `ajv` | `avsc` only |
| Errors we must tell apart | generic / axios errors | generic | **typed**: `RegistryUnavailableError` (transient: never dead-letter) vs `UnknownSchemaIdError` / `AvroDecodeError` / `NotAvroError` / `IncompatibleWriterError` (permanent) |
| Caching, timeouts, metrics | its own LRU and retries | its own | explicit: writer schemas cached per id with a negative cache for unknown ids, bounded timeout per request, an observer for `ep_schema_registry_*` |
| Runtime registration | auto-register on by default | auto-register optional | **never**: lookup only |

**Decision: `avsc` (pinned 5.7.9) plus our own registry client** (`packages/schema/src/registry.ts`). We use five REST calls (get by id, lookup, register, compatibility test, config). Owning them gives typed errors (the sink's "registry down ≠ bad record" rule depends on it), explicit cache behavior, and no cloud SDKs in the hot path. If Stage 8 adds Protobuf or data contracts, `@confluentinc/schemaregistry` is the first candidate.

## Other decisions in this ADR

- **Subject `events.raw-value`, compatibility `FULL_TRANSITIVE`.** Producers and consumers deploy in any order, and a replay reads every old version. So a change must be readable both ways against **every** earlier version: only fields with defaults may be added or removed. The fixtures show why plain `BACKWARD` is not enough: it accepts removing a field without a default, which breaks every consumer still on the old schema (`make schema-fixtures`).
- **No runtime registration.** Schemas are registered by the deploy step (`make schemas`, idempotent). The API only looks up its writer schema's id at startup; a schema that is not registered keeps the API not-ready instead of silently creating a new version.
- **Payload as `payload_json` (string).** Avro has no "any JSON object" type. The envelope is typed and evolvable; the tenant payload stays opaque until a later stage adds per-event-type schemas. It travels exactly as sent (`JSON.stringify`, no Unicode normalization); canonical JSON is used only for hashing.
- **Times as `timestamp-millis`.** Sub-millisecond digits of `occurred_at` are dropped (the Stage 3 step 8 precision rule, now enforced by the format).
- **Legacy JSON stays readable** (`SINK_ACCEPT_JSON=true`): records from Stage 2 are still in the log, and S3-F2 requires replaying any range.
- **Registry failure policy:** producer: wait for the first lookup at startup (bounded), then background retries, `/readyz` 503 until the id is known; afterwards the registry is not needed at all. Consumer: an uncached writer id with the registry unreachable pauses the partition and retries (never DLQ); an id the registry does not know is permanent (DLQ, `unknown_schema_id`).

## Measurements

One core, Node 24, 200,000 iterations per case (`bench-serde.mjs`, run from the scratchpad, not committed). Avro means the wire format including its 5-byte header; JSON means the Stage 2 value.

| Payload | Value size JSON / Avro | Encode JSON / Avro | Decode JSON / Avro |
| --- | ---: | ---: | ---: |
| ~20 B | 183 B / 95 B (−48 %) | 0.55 / 1.68 µs | 0.62 / 1.75 µs |
| ~100 B | 263 B / 176 B (−33 %) | 0.56 / 1.71 µs | 0.65 / 1.75 µs |
| ~1 KB | 1,176 B / 1,089 B (−7 %) | 1.42 / 3.07 µs | 0.97 / 2.03 µs |

- Avro saves the envelope's field names, so the gain shrinks as the opaque `payload_json` grows.
- It costs about 1-2 µs more per event on each side, including schema validation on encode. At 10,000 events/s that is about 1-2 % of one core.
- The cost at the system level (knee of the step test with Avro on and off) is experiment C14.

## Consequences

- A new dependency (Schema Registry) for producer startup and for consumers meeting a new writer id; covered by the policy above and by tests through Toxiproxy (`tests/correctness/test/schema.e2e-spec.ts`).
- CI runs `schema-check check` and `schema-check fixtures` against an empty registry (`.github/workflows/ci.yml`), so compatibility is checked automatically, not by memory.
- `schema_version` in the table stays the client's payload version (from the API body); the registry version/id is logged and counted (`ep_schema_ids_in_use`), not stored.
