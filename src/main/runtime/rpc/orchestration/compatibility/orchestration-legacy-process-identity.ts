import { parsePaneKey } from '../../../../../shared/workspace-layout/stable-pane-id'
import { OrchestrationError } from '../../../orchestration/orchestration-error'
import type { OrchestrationDb } from '../../../orchestration/db/orchestration-db'
import { LEGACY_CONTRACT_VERSION } from '../../../orchestration/db/contract-constants'

/** Selective receipts cannot authorize any mailbox through a retained legacy handle. */
export function assertSelectableMailboxHandle(db: OrchestrationDb, handle: string): void {
  const adoption = db.getLegacyAdoption()
  if (!adoption) {
    return
  }
  if (
    db.db
      .prepare(
        `SELECT 1 FROM dispatch_contexts
         WHERE run_id = ? AND contract_version = ? AND assignee_handle = ?
         LIMIT 1`
      )
      .get(adoption.adopted_run_id, LEGACY_CONTRACT_VERSION, handle)
  ) {
    throw legacyReadOnlyError()
  }
  assertNotRetainedLegacyHandle(db, adoption.adopted_run_id, handle)
}

/** Reject the retained handle, not a different current caller using the historical pane. */
export function assertNotRetainedLegacyHandle(
  db: OrchestrationDb,
  runId: string,
  terminalHandle: string | undefined
): void {
  if (
    terminalHandle &&
    (db.isLegacyCoordinatorHandle(runId, terminalHandle) ||
      db.resolveLegacyCompatibilityPrincipalByIdentity({ runId, role: 'worker', terminalHandle }) ||
      db.resolveLegacyWorkerCandidate({ runId, terminalHandle }))
  ) {
    throw legacyReadOnlyError()
  }
}

export function equivalentLegacyPaneKey(
  a: string | null | undefined,
  b: string | null | undefined
): boolean {
  if (!a || !b) {
    return false
  }
  if (a === b) {
    return true
  }
  const aLeaf = parsePaneKey(a)?.leafId
  const bLeaf = parsePaneKey(b)?.leafId
  return Boolean(aLeaf && bLeaf && aLeaf === bLeaf)
}

export function legacyReadOnlyError(): OrchestrationError {
  return new OrchestrationError(
    'legacy_read_only',
    'This retained legacy assignment could not prove authority from its original live process. No effects were applied.',
    { effectsApplied: false }
  )
}

export function legacyCoordinatorReadOnly(): OrchestrationError {
  return new OrchestrationError(
    'legacy_read_only',
    'This retained legacy coordinator could not prove its original process identity. No effects were applied.',
    { effectsApplied: false }
  )
}
