import { parsePaneKey } from '../../../../../shared/workspace-layout/stable-pane-id'
import type { OrchestrationDb } from '../orchestration-db'
import { DISPATCH_PANE_KEY_MATCH_SUFFIX_SQL } from '../pane-key-match'

/** Unlike routing precedence, a destructive selection must not hide another matching owner. */
export function hasCompetingActiveDispatch(
  db: OrchestrationDb,
  dispatchId: string,
  handle: string,
  recordedHandle: string | null,
  paneKey: string | undefined
): boolean {
  const leafId = paneKey ? parsePaneKey(paneKey)?.leafId : undefined
  return (
    db.db
      .prepare(
        `SELECT 1 FROM dispatch_contexts
         WHERE status IN ('pending', 'dispatched') AND id <> ?
           AND (assignee_handle IN (?, ?) OR assignee_pane_key = ? OR
             (instr(assignee_pane_key, ':') > 1 AND ${DISPATCH_PANE_KEY_MATCH_SUFFIX_SQL} = ?))
         LIMIT 1`
      )
      .get(dispatchId, handle, recordedHandle, paneKey ?? null, leafId ?? null) !== undefined
  )
}
