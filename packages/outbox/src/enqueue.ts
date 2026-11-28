// enqueue (S3-F4): write an outbox row with the CALLER'S transaction client, next to the state change it announces.
//
// The dual-write bug is "change the database, then publish to Kafka": a crash between the two loses the event, and
// publishing first announces a change that may roll back. With the outbox, the row commits or rolls back WITH the
// change, and the relay publishes committed rows only.
//
// enqueue() refuses to run outside a transaction, so the bug cannot come back by accident (an enqueue on a pool, in
// autocommit, would commit the row on its own). The check is txid_current_if_assigned() IS NOT NULL: it is null
// in autocommit AND in a transaction that has not written yet. Both are refused on purpose: an outbox row must follow
// the write it announces, in the same transaction.

export const OUTBOX_CHANNEL = 'outbox_new';

/** Anything with pg's query(): a PoolClient inside BEGIN ... COMMIT. */
export interface TxClient {
  query(text: string, values?: unknown[]): Promise<{ rows: any[]; rowCount?: number | null }>;
}

export interface OutboxMessage {
  /** What changed (tenant id for config changes). */
  aggregateId: string;
  topic: string;
  /** Kafka key: one key = one partition = order kept for that aggregate. */
  key: string;
  payload: Buffer;
  headers?: Record<string, string>;
}

export class OutsideTransactionError extends Error {
  readonly name = 'OutsideTransactionError';
  constructor() {
    super('outbox enqueue needs the caller\'s open transaction, after the write it announces (txid_current_if_assigned() is null)');
  }
}

export const ENQUEUE_SQL = `INSERT INTO outbox (aggregate_id, topic, msg_key, payload, headers) VALUES ($1, $2, $3, $4, $5) RETURNING id`;

export interface EnqueueOptions {
  /** NOTIFY outbox_new (delivered at COMMIT) so a listening relay publishes without waiting for its next poll. */
  notify?: boolean;
}

/** Returns the outbox row id. Throws OutsideTransactionError (nothing written) when not in a writing transaction. */
export async function enqueue(tx: TxClient, msg: OutboxMessage, opts: EnqueueOptions = {}): Promise<string> {
  const t = await tx.query('SELECT txid_current_if_assigned() AS xid');
  if (t.rows[0]?.xid === null || t.rows[0]?.xid === undefined) throw new OutsideTransactionError();
  const r = await tx.query(ENQUEUE_SQL, [msg.aggregateId, msg.topic, msg.key, msg.payload, JSON.stringify(msg.headers ?? {})]);
  if (opts.notify !== false) await tx.query('SELECT pg_notify($1, $2)', [OUTBOX_CHANNEL, msg.topic]);
  return String(r.rows[0].id);
}
