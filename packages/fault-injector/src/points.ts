// THE list of named crash points. Every component's fault injector only accepts names from here, and the
// crash-matrix runner (tests/correctness/crash-matrix) builds its scenarios from here, so a point added without
// a matrix scenario fails the build.
//
// Name format: <component>:<step>. Later Stage 3 steps add retry:*, dlq:*, replay:*, relay:*, admin:*.

export interface CrashPointInfo {
  readonly name: string;
  readonly component: string;
  /** What has happened (and what has not) when the process dies at this point. */
  readonly description: string;
}

export const CRASH_POINTS = [
  { name: 'consumer:before-parse', component: 'consumer', description: 'chunk fetched, nothing parsed or written' },
  { name: 'consumer:after-parse', component: 'consumer', description: 'chunk parsed, nothing written' },
  { name: 'consumer:before-insert', component: 'consumer', description: 'transaction open (BEGIN sent), INSERT not sent' },
  { name: 'consumer:after-insert-before-commit', component: 'consumer', description: 'INSERT done inside the open transaction, COMMIT not sent' },
  { name: 'consumer:after-db-commit', component: 'consumer', description: 'rows committed in Postgres, Kafka offset not committed' },
  { name: 'consumer:after-offset-commit', component: 'consumer', description: 'rows and Kafka offset both committed' },
  { name: 'api:before-produce', component: 'api', description: 'idempotency key acquired (inflight), event not produced, no response' },
  { name: 'api:after-produce-before-complete', component: 'api', description: 'event acknowledged by Kafka, key still inflight, no response' },
  { name: 'api:after-complete-before-response', component: 'api', description: 'event acknowledged, key marked done, no response sent' },
] as const satisfies readonly CrashPointInfo[];

export type CrashPointName = (typeof CRASH_POINTS)[number]['name'];

export const CRASH_POINT_NAMES: readonly string[] = CRASH_POINTS.map((p) => p.name);

export const isCrashPoint = (s: string): s is CrashPointName => CRASH_POINT_NAMES.includes(s);

export const pointsOf = (component: string): CrashPointInfo[] => CRASH_POINTS.filter((p) => p.component === component);

/** Stage 2 CRASH_AT values (K5 scripts) mapped to the new names. */
export const LEGACY_CRASH_AT: Readonly<Record<string, CrashPointName>> = {
  before_parse: 'consumer:before-parse',
  after_parse: 'consumer:after-parse',
  after_db_commit: 'consumer:after-db-commit',
};
