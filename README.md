# Eventpulse

Event ingestion pipeline: `POST /v1/events` -> Kafka `events.raw` -> `consumer-sink` -> Postgres.

- What it guarantees, each claim mapped to a test: [docs/delivery-guarantees.md](docs/delivery-guarantees.md)
- Every doc (API contract, crash matrix, ADRs, runbooks): [docs/README.md](docs/README.md)
- Every command: `make help`
