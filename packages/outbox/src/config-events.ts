// config.changed (S3-F4): the event services/admin-api puts in the outbox when a tenant's config changes.
// Avro, subject config.changed-value (schemas/config/v1.avsc), key = tenant id. FULL config snapshot, not a diff.

import { RecordSerde, SUBJECT_CONFIG_CHANGED, latestSchema } from '@eventpulse/schema';
import type { RegistryClient } from '@eventpulse/schema';

export const TOPIC_CONFIG_CHANGED = 'config.changed';
export const HEADER_EVENT_ID = 'event_id';
export const HEADER_VERSION = 'version';
export const HEADER_OUTBOX_ID = 'outbox_id';
export const CONTENT_TYPE_CONFIG_AVRO = 'application/vnd.eventpulse.config-changed+avro';

export interface ConfigChanged {
  eventId: string;
  tenantId: string;
  version: number;
  changedAt: Date;
  config: Record<string, unknown>;
}

interface ConfigChangedRecord {
  event_id: string;
  tenant_id: string;
  version: number;
  changed_at: number;
  config_json: string;
}

export const toConfigRecord = (e: ConfigChanged): ConfigChangedRecord => ({
  event_id: e.eventId.toLowerCase(),
  tenant_id: e.tenantId,
  version: e.version,
  changed_at: e.changedAt.getTime(),
  config_json: JSON.stringify(e.config),
});

export const fromConfigRecord = (r: ConfigChangedRecord): ConfigChanged => ({
  eventId: r.event_id,
  tenantId: r.tenant_id,
  version: Number(r.version),
  changedAt: new Date(Number(r.changed_at)),
  config: JSON.parse(r.config_json),
});

/** Serde for config.changed-value with the newest local schema (producer: init() looks the id up, never registers). */
export class ConfigChangedSerde {
  readonly serde: RecordSerde<ConfigChangedRecord>;
  constructor(registry: RegistryClient | undefined, schemasDir?: string) {
    this.serde = new RecordSerde<ConfigChangedRecord>(registry, { subject: SUBJECT_CONFIG_CHANGED, schema: latestSchema(SUBJECT_CONFIG_CHANGED, schemasDir).parsed });
  }
  init() {
    return this.serde.init();
  }
  get ready() {
    return this.serde.ready;
  }
  encode(e: ConfigChanged): Buffer {
    return this.serde.encode(toConfigRecord(e));
  }
  async decode(b: Buffer): Promise<ConfigChanged> {
    return fromConfigRecord(await this.serde.decodeAsync(b));
  }
}
