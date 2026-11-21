import type { DispatchContextRow } from '../../../../orchestration/db/orchestration-records'
import type { OrchestrationDb } from '../../../../orchestration/db/orchestration-db'
import { OrchestrationError } from '../../../../orchestration/orchestration-error'
import { isEquivalentPaneKey } from '../../../../orchestration/db/pane-key-match'

export const DISPATCH_FENCED_MESSAGE =
  'This process no longer owns its Dispatch: the Attempt was re-attached to another worker or settled. Stop; do not send worker_done and do not retry the check.'

export function dispatchFenced(): OrchestrationError {
  return new OrchestrationError('consumer_fenced', DISPATCH_FENCED_MESSAGE)
}

export function resolveCheckDispatch(
  db: OrchestrationDb,
  handle: string,
  paneKey: string | undefined,
  selective: boolean,
  residual = false
): DispatchContextRow | undefined {
  const dispatch = db.getActiveDispatchForIdentity(handle, paneKey)
  if (!selective || !dispatch) {
    return dispatch
  }
  const candidates = db.getActiveDispatchMailboxOwners(handle, paneKey)
  if (candidates.length === 1 && candidates[0]?.id === dispatch.id) {
    return dispatch
  }
  if (residual) {
    return undefined
  }
  throw new OrchestrationError(
    'consumer_fenced',
    'Selective acknowledgment requires an unambiguous Dispatch owner.',
    { effectsApplied: false }
  )
}

/** Delivery fencing is generic; a worker needs to hear that it lost the Dispatch, not the Run. */
export function asDispatchFence(error: unknown): unknown {
  return error instanceof OrchestrationError && error.code === 'consumer_fenced'
    ? dispatchFenced()
    : error
}

// Handle lookup wins over pane metadata; reject stale processes whose Dispatch moved to another pane.
export function callerHoldsDispatchPane(
  dispatch: { assignee_pane_key: string | null },
  paneKey: string | undefined
): boolean {
  return (
    paneKey === undefined ||
    dispatch.assignee_pane_key === null ||
    isEquivalentPaneKey(dispatch.assignee_pane_key, paneKey)
  )
}

/** Refuse consuming reads after abandoned/stopped/failed Attempts; an empty checkpoint would let the loser keep working.
 * Completed Attempts free the terminal for direct mail. Retry-eligible settlement already fails/circuit-breaks Dispatch. */
export function isSupersededDispatch(dispatch: DispatchContextRow): boolean {
  return dispatch.status === 'failed' || dispatch.status === 'circuit_broken'
}
