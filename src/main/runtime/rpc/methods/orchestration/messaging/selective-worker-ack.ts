import {
  callerHoldsDispatchPane,
  dispatchFenced,
  resolveCheckDispatch
} from './dispatch-mailbox-fence'
import type { WorkerMailboxCheckInput } from './check-worker-mailbox-input'
import { orchestrationCallerIdentity } from '../runs/run-scope'

export function acknowledgeSelectedWorkerMail(args: WorkerMailboxCheckInput, ids: string[]) {
  const { activeDispatch, db, handle, paneKey } = args
  if (!activeDispatch) {
    throw dispatchFenced()
  }
  const { id, run_id: runId, consumer_generation: generation } = activeDispatch
  return args.reconciliation.acknowledgeSelected({
    mailbox: `dispatch:${id}`,
    ids,
    direct: false,
    mailboxIdentity: { runId, dispatchId: id },
    routeSource: { caller: handle, assignee: activeDispatch.assignee_handle },
    recordReceipt: args.recordMutationReceipt,
    revalidateConsumer: () => {
      args.revalidateCaller?.()
      args.revalidateConsumer?.()
      const current = resolveCheckDispatch(db, handle, paneKey, {
        sessionId: orchestrationCallerIdentity(args.runtime, {
          handle,
          paneKey,
          readOnly: true,
          session: args.reader
        }).orcaSessionId
      })
      if (
        current?.id !== id ||
        current.consumer_generation !== generation ||
        !callerHoldsDispatchPane(current, paneKey)
      ) {
        throw dispatchFenced()
      }
    }
  })
}
