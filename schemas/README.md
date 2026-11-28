# schemas

Avro schemas of the messages EventPulse puts on Kafka (Stage 3, S3-F7), and the policy that governs them.

| Path | What |
| --- | --- |
| `subjects.json` | registry subject -> ordered schema history + compatibility level |
| `events/v1.avsc`, `events/v2.avsc` | `events.raw-value` (v2 adds the optional `source`) |
| `config/v1.avsc` | `config.changed-value` (S3-F4): tenant config change events from the outbox, full config snapshot as `config_json` |
| `fixtures/incompatible/`, `fixtures/compatible/` | `events.raw-value` changes the policy must reject (rename without alias, type change, required field without default, removed field without default) / accept (optional field with a default) |
| `fixtures/config/` | the same for `config.changed-value` (`version` long to int, removing `config_json` / adding an optional `changed_by`) |

Each subject in `subjects.json` names its own `fixtures` directory: a fixture is written against ITS subject's schema.

- **Never edit a published version.** Add `vN+1.avsc` and append it to `files` in `subjects.json`.
- Check locally: `make schema-check` (needs the running registry; uses throw-away subjects, removes them after).
- Register for a deploy: `make schemas` (idempotent).
- Procedure: `docs/runbooks/schema-change.md`.
