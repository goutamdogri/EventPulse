# schemas

Avro schemas of the messages EventPulse puts on Kafka (Stage 3, S3-F7), and the policy that governs them.

| Path | What |
| --- | --- |
| `subjects.json` | registry subject -> ordered schema history + compatibility level |
| `events/v1.avsc`, `events/v2.avsc` | `events.raw-value` (v2 adds the optional `source`) |
| `fixtures/incompatible/` | changes the policy must reject (rename without alias, type change, required field without default, removed field without default) |
| `fixtures/compatible/` | a change the policy must accept (optional field with a default) |

- **Never edit a published version.** Add `vN+1.avsc` and append it to `files` in `subjects.json`.
- Check locally: `make schema-check` (needs the running registry; uses throw-away subjects, removes them after).
- Register for a deploy: `make schemas` (idempotent).
- Procedure: `docs/runbooks/schema-change.md`.
