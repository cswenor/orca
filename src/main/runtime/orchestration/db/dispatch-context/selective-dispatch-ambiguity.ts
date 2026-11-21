import { parsePaneKey } from '../../../../../shared/workspace-layout/stable-pane-id'
import type { OrchestrationDb } from '../orchestration-db'
import type { DispatchContextRow } from '../orchestration-records'
import { DISPATCH_PANE_KEY_MATCH_SUFFIX_SQL } from '../pane-key-match'

/** Unlike routing precedence, a destructive selection must not hide another matching owner. */
export function hasCompetingActiveDispatch(
  db: OrchestrationDb,
  dispatch: DispatchContextRow,
  handle: string,
  paneKey: string | undefined,
  sessionId: string | null
): boolean {
  const leafId = paneKey ? parsePaneKey(paneKey)?.leafId : undefined
  return (
    db.db
      .prepare(
        `SELECT 1 FROM dispatch_contexts
         WHERE status IN ('pending', 'dispatched') AND id <> ?
           AND (assignee_handle IN (?, ?) OR assignee_orca_session_id IN (?, ?) OR assignee_pane_key = ? OR
             (instr(assignee_pane_key, ':') > 1 AND ${DISPATCH_PANE_KEY_MATCH_SUFFIX_SQL} = ?))
         LIMIT 1`
      )
      .get(
        dispatch.id,
        handle,
        dispatch.assignee_handle,
        dispatch.assignee_orca_session_id,
        sessionId,
        paneKey ?? null,
        leafId ?? null
      ) !== undefined
  )
}
