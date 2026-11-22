// One place for names shared by the producer (ingest-api) and the consumers (consumer-sink, later stages).
// Topics are created by infra/compose/docker-compose.kafka.yml (kafka-init); auto-create is off on the brokers.

export const TOPIC_EVENTS_RAW = 'events.raw';
export const TOPIC_EVENTS_DLQ = 'events.dlq';
export const GROUP_SINK_POSTGRES = 'sink-postgres';
