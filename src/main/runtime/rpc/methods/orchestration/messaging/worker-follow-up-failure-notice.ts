import { createHash } from 'node:crypto'
import type { OrcaRuntimeService } from '../../../../orca-runtime'
import type { OrchestrationDb } from '../../../../orchestration/db/orchestration-db'
import { dispatchReportMailbox } from '../../../../orchestration/workers/reports/dispatch-report-mailbox'
import { escalateWorkerFailure } from '../../../../orchestration/workers/reports/worker-failure-escalation'

/** Nothing is lost silently: the coordinator hears that its mail did not reach the worker, once per batch. */
export function noticeUnreadMailNotContinued(
  runtime: OrcaRuntimeService,
  db: OrchestrationDb,
  dispatchId: string,
  reason: string,
  restoring = false
): Promise<void> | undefined {
  const dispatch = db.getDispatchContextById(dispatchId)
  if (!dispatch) {
    return
  }
  let recovery: Promise<void> | undefined
  escalateWorkerFailure(
    db,
    dispatch,
    {
      id: `msg_followup_${createHash('sha256')
        .update(JSON.stringify([dispatchId, dispatch.follow_up_mail_at]))
        .digest('hex')
        .slice(0, 24)}`,
      from: `dispatch:${dispatchId}`,
      mailbox: () => dispatchReportMailbox(db, dispatch),
      describe: (task) => ({
        subject: 'Mail did not reach the finished worker',
        body: `Orca could not continue the worker of ${task} with the mail it had not read before reporting: ${reason}. The mail is kept for your next send to dispatch:${dispatchId}.`
      }),
      payload: { dispatchId, reason }
    },
    (handle, type) => {
      recovery = runtime.notifyMessageArrived(handle, type, restoring)
    }
  )
  return recovery
}
