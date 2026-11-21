import { randomBytes } from 'node:crypto'
import type { MutationReceiptRow } from '../orchestration-records'
import { OrchestrationError } from '../../orchestration-error'
import { ensureMutationReceiptCapacity } from './mutation-receipt-capacity'
import type { OrchestrationDb } from '../orchestration-db'
import { MUTATION_RECEIPT_COLUMN_LIST } from '../row-column-lists'

const LOCAL_TRANSPORT = 'local_authenticated_transport'

export function readLocalMutationCallerFingerprint(this: OrchestrationDb): string | undefined {
  if (this.localMutationCallerFingerprint) {
    return this.localMutationCallerFingerprint
  }
  const fingerprint = this.db
    .prepare('SELECT caller_fingerprint FROM mutation_caller_identities WHERE transport = ?')
    .get(LOCAL_TRANSPORT)?.caller_fingerprint
  return typeof fingerprint === 'string' ? fingerprint : undefined
}

function ensureStoredLocalCaller(db: OrchestrationDb): string {
  const existing = db.readLocalMutationCallerFingerprint()
  if (existing) {
    return existing
  }
  db.db
    .prepare(
      `INSERT OR IGNORE INTO mutation_caller_identities (transport, caller_fingerprint)
       VALUES (?, ?)`
    )
    .run(LOCAL_TRANSPORT, randomBytes(32).toString('hex'))
  const created = db.readLocalMutationCallerFingerprint()
  if (!created) {
    throw new Error('Failed to create the local orchestration mutation caller identity.')
  }
  return created
}

export function getOrCreateLocalMutationCallerFingerprint(this: OrchestrationDb): string {
  return (this.localMutationCallerFingerprint ??= ensureStoredLocalCaller(this))
}

export function beginMutationReceipt(
  this: OrchestrationDb,
  params: {
    requestId: string
    method: string
    payloadHash: string
  } & (
    | { callerFingerprint: string; receipt?: undefined }
    | { callerFingerprint?: string; receipt: string }
  )
):
  | { disposition: 'started'; row: MutationReceiptRow }
  | { disposition: 'pending'; row: MutationReceiptRow }
  | { disposition: 'completed'; row: MutationReceiptRow } {
  // A selected mailbox write provides its final response and owns the transaction.
  const complete = params.receipt !== undefined
  if (complete) {
    if (!this.db.isTransaction) {
      throw new Error('A completed mutation receipt requires the effect transaction.')
    }
  } else {
    this.db.exec('BEGIN IMMEDIATE')
  }
  try {
    const caller = params.callerFingerprint ?? ensureStoredLocalCaller(this)
    const existing = this.getMutationReceipt(caller, params.requestId)
    if (existing) {
      if (
        complete ||
        existing.method !== params.method ||
        existing.payload_hash !== params.payloadHash
      ) {
        throw new OrchestrationError(
          'request_mismatch',
          `Mutation request ${params.requestId} was already used with different input.`
        )
      }
      this.db.exec('COMMIT')
      return { disposition: existing.state, row: existing }
    }
    ensureMutationReceiptCapacity(this.db, complete ? 'preserve' : 'prune-completed')
    const row = this.db
      .prepare(
        `INSERT INTO mutation_receipts (
           caller_fingerprint, request_id, method, payload_hash, state, receipt
         ) VALUES (?, ?, ?, ?, ?, ?) RETURNING ${MUTATION_RECEIPT_COLUMN_LIST}`
      )
      .get(
        caller,
        params.requestId,
        params.method,
        params.payloadHash,
        complete ? 'completed' : 'pending',
        params.receipt ?? null
      )
    if (!complete) {
      this.db.exec('COMMIT')
    }
    return { disposition: 'started', row: row as MutationReceiptRow }
  } catch (error) {
    if (!complete) {
      this.db.exec('ROLLBACK')
    }
    throw error
  }
}

export function completeMutationReceipt(
  this: OrchestrationDb,
  params: {
    callerFingerprint: string
    requestId: string
    method: string
    payloadHash: string
    receipt: string
  }
): MutationReceiptRow {
  const result = this.db
    .prepare(
      `UPDATE mutation_receipts
       SET state = 'completed', receipt = ?, updated_at = datetime('now')
       WHERE caller_fingerprint = ? AND request_id = ? AND method = ?
         AND payload_hash = ?`
    )
    .run(
      params.receipt,
      params.callerFingerprint,
      params.requestId,
      params.method,
      params.payloadHash
    )
  const row = this.getMutationReceipt(params.callerFingerprint, params.requestId)
  if (result.changes !== 1 || !row) {
    throw new OrchestrationError(
      'request_mismatch',
      `Mutation request ${params.requestId} no longer matches its pending operation.`
    )
  }
  return row
}

export function checkpointPendingMutationReceipt(
  this: OrchestrationDb,
  params: {
    callerFingerprint: string
    requestId: string
    method: string
    payloadHash: string
    receipt: string
  }
): MutationReceiptRow {
  const result = this.db
    .prepare(
      `UPDATE mutation_receipts
       SET receipt = ?, updated_at = datetime('now')
       WHERE caller_fingerprint = ? AND request_id = ? AND method = ?
         AND payload_hash = ? AND state = 'pending'`
    )
    .run(
      params.receipt,
      params.callerFingerprint,
      params.requestId,
      params.method,
      params.payloadHash
    )
  const row = this.getMutationReceipt(params.callerFingerprint, params.requestId)
  if (result.changes !== 1 || !row) {
    throw new OrchestrationError(
      'request_mismatch',
      `Mutation request ${params.requestId} no longer matches its pending operation.`
    )
  }
  return row
}

export function discardPendingMutationReceipt(
  this: OrchestrationDb,
  callerFingerprint: string,
  requestId: string
): void {
  this.db
    .prepare(
      `DELETE FROM mutation_receipts
       WHERE caller_fingerprint = ? AND request_id = ? AND state = 'pending'`
    )
    .run(callerFingerprint, requestId)
}

export function countPendingMutationReceipts(
  this: OrchestrationDb,
  callerFingerprints: readonly string[]
): number {
  const count = this.db
    .prepare(
      `SELECT COUNT(*) AS count FROM mutation_receipts
       WHERE caller_fingerprint IN (SELECT value FROM json_each(?)) AND state = 'pending'`
    )
    .get(JSON.stringify(callerFingerprints))?.count
  return Number(count ?? 0)
}

/** Completes these callers' pending rows with `receipt`, for operations no running work owns. */
export function settlePendingMutationReceipts(
  this: OrchestrationDb,
  callerFingerprints: readonly string[],
  receipt: string
): void {
  this.db
    .prepare(
      `UPDATE mutation_receipts
       SET state = 'completed', receipt = ?, updated_at = datetime('now')
       WHERE caller_fingerprint IN (SELECT value FROM json_each(?)) AND state = 'pending'`
    )
    .run(receipt, JSON.stringify(callerFingerprints))
}

export function getMutationReceipt(
  this: OrchestrationDb,
  callerFingerprint: string,
  requestId: string
): MutationReceiptRow | undefined {
  // oxlint-disable-next-line typescript/consistent-type-assertions -- SAFETY: the column list names every column of the row type, and a missing one fails compilation.
  return this.db
    .prepare(
      `SELECT ${MUTATION_RECEIPT_COLUMN_LIST} FROM mutation_receipts
       WHERE caller_fingerprint = ? AND request_id = ?`
    )
    .get(callerFingerprint, requestId) as MutationReceiptRow | undefined
}

export type MutationReceiptStoreMethods = {
  getOrCreateLocalMutationCallerFingerprint: typeof getOrCreateLocalMutationCallerFingerprint
  readLocalMutationCallerFingerprint: typeof readLocalMutationCallerFingerprint
  beginMutationReceipt: typeof beginMutationReceipt
  completeMutationReceipt: typeof completeMutationReceipt
  checkpointPendingMutationReceipt: typeof checkpointPendingMutationReceipt
  discardPendingMutationReceipt: typeof discardPendingMutationReceipt
  countPendingMutationReceipts: typeof countPendingMutationReceipts
  settlePendingMutationReceipts: typeof settlePendingMutationReceipts
  getMutationReceipt: typeof getMutationReceipt
}

export function attachMutationReceiptStore(ctor: { prototype: object }): void {
  Object.assign(ctor.prototype, {
    getOrCreateLocalMutationCallerFingerprint,
    readLocalMutationCallerFingerprint,
    beginMutationReceipt,
    completeMutationReceipt,
    checkpointPendingMutationReceipt,
    discardPendingMutationReceipt,
    countPendingMutationReceipts,
    settlePendingMutationReceipts,
    getMutationReceipt
  })
}
