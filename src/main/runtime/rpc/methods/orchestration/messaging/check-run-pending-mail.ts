import type {
  DispatchContextRow,
  MessageType,
  RunRow
} from '../../../../orchestration/db/orchestration-records'
import type { OrchestrationDb } from '../../../../orchestration/db/orchestration-db'
import type { OrcaRuntimeService } from '../../../../orca-runtime'
import { interruptedAcknowledgedCheck } from '../routing'
import { checkWorkerMailbox } from './check-worker'
import { acknowledgedOnlyReceipt, handsOverBatch } from './mailbox-message-receipt'
import type { CheckMailReconciliation } from './check-mail-reconciliation'
import { currentDispatchAssigneeRun } from './recipient-routing'
import {
  callerHoldsDispatchPane,
  dispatchFenced,
  resolveCheckDispatch
} from './dispatch-mailbox-fence'
import { orchestrationCallerIdentity } from '../runs/run-scope'
import type {
  OrchestrationEntryCaller,
  OrchestrationSessionCaller
} from '../../../../orchestration/addressing/orchestration-caller-identity'
import type { MailboxCheckParams } from '../../../../../../shared/rpc-contract/orchestration/orchestration-params'

export function callerHoldsResidualDispatch(
  runtime: OrcaRuntimeService,
  db: OrchestrationDb,
  residual: DispatchContextRow,
  runId: string,
  paneKey: string | undefined,
  assigneeSessionId: string | null,
  readOnly = false
): boolean {
  return (
    residual.run_id !== runId &&
    (residual.assignee_orca_session_id === null ||
      residual.assignee_orca_session_id === assigneeSessionId) &&
    callerHoldsDispatchPane(residual, paneKey) &&
    currentDispatchAssigneeRun(runtime, db, residual, readOnly)?.id === runId
  )
}

export async function checkRunPendingMail(args: {
  params: MailboxCheckParams
  runtime: OrcaRuntimeService
  db: OrchestrationDb
  run: RunRow
  handle: string
  paneKey: string | undefined
  callerSession: OrchestrationEntryCaller | undefined
  reader: OrchestrationSessionCaller | undefined
  typeFilter: MessageType[] | undefined
  signal: AbortSignal | undefined
  revalidateConsumer: () => void
  reconciliation: CheckMailReconciliation
  recordMutationReceipt: ((receipt: unknown) => void) | undefined
}): Promise<{ acknowledged?: string; result?: unknown }> {
  const {
    params,
    runtime,
    db,
    run,
    handle,
    paneKey,
    typeFilter,
    signal,
    revalidateConsumer,
    recordMutationReceipt
  } = args
  const generation = run.consumer_generation
  const address = `run:${run.id}`
  const caller = orchestrationCallerIdentity(runtime, {
    handle,
    paneKey,
    session: args.callerSession
  })
  // Drain pre-bind mail through its original Dispatch owner, never by changing its Run.
  const residualCheck = { sessionId: caller.orcaSessionId, residual: true }
  const residual = !params.run
    ? resolveCheckDispatch(db, handle, paneKey, params.ackIds ? residualCheck : undefined)
    : undefined
  const residualAck =
    params.ack &&
    residual &&
    db.getDeliveryRaw(params.ack)?.mailbox_handle === `dispatch:${residual.id}`
      ? params.ack
      : undefined
  const ownsResidual = (): boolean => {
    if (!residual) {
      return false
    }
    return (
      callerHoldsResidualDispatch(
        runtime,
        db,
        residual,
        run.id,
        paneKey,
        caller.orcaSessionId,
        params.ackIds !== undefined
      ) && Boolean(residualAck || !db.hasOutstandingMailboxDelivery(address))
    )
  }
  if (params.ackIds) {
    return {
      result: await args.reconciliation.acknowledgeSelected({
        mailbox: address,
        ids: params.ackIds,
        direct: false,
        mailboxIdentity: { runId: run.id },
        routeSource: {
          caller: handle,
          assignee: run.coordinator_handle,
          excludeDispatchOwned: true
        },
        recordReceipt: recordMutationReceipt,
        revalidateConsumer,
        residual:
          residual && ownsResidual()
            ? {
                mailbox: `dispatch:${residual.id}`,
                runId: residual.run_id,
                caller: handle,
                assignee: residual.assignee_handle,
                revalidate: () => {
                  const current = resolveCheckDispatch(db, handle, paneKey, {
                    sessionId: orchestrationCallerIdentity(runtime, {
                      handle,
                      paneKey,
                      session: args.callerSession
                    }).orcaSessionId,
                    residual: true
                  })
                  if (
                    current?.id !== residual.id ||
                    current.consumer_generation !== residual.consumer_generation ||
                    db.hasOutstandingMailboxDelivery(address) ||
                    !callerHoldsResidualDispatch(
                      runtime,
                      db,
                      current,
                      run.id,
                      paneKey,
                      caller.orcaSessionId,
                      true
                    )
                  ) {
                    throw dispatchFenced()
                  }
                }
              }
            : undefined
      })
    }
  }
  const acknowledgeRun = () =>
    params.ack
      ? db.acknowledgeRunDelivery({
          runId: run.id,
          consumerGeneration: generation,
          deliveryId: params.ack
        })
      : undefined
  let acknowledged: { delivery: { id: string } } | undefined = residualAck
    ? undefined
    : acknowledgeRun()
  const recordAcknowledged = () => {
    if (acknowledged) {
      recordMutationReceipt?.(
        interruptedAcknowledgedCheck(run.id, acknowledged.delivery.id, 'outcome_unknown')
      )
    }
  }
  recordAcknowledged()
  if (acknowledged && !params.wait) {
    const result = acknowledgedOnlyReceipt({ runId: run.id }, acknowledged.delivery.id)
    return { acknowledged: acknowledged.delivery.id, result }
  }
  if (residual && ownsResidual()) {
    const result = await checkWorkerMailbox({
      params: { ...params, ack: residualAck, wait: false },
      reader: args.reader,
      runtime,
      db,
      handle,
      paneKey,
      typeFilter,
      signal,
      activeDispatch: residual,
      remoteAttachment: undefined,
      wakeTypes: params.wait ? typeFilter : undefined,
      // Accept the original owner's ack without creating a batch ahead of Run replay.
      deferDelivery: () => db.hasOutstandingMailboxDelivery(address),
      revalidateConsumer: () => {
        revalidateConsumer()
        const current = db.getActiveDispatchForIdentity(handle, paneKey)
        if (
          !current ||
          current.id !== residual.id ||
          currentDispatchAssigneeRun(runtime, db, current)?.id !== run.id
        ) {
          throw dispatchFenced()
        }
      },
      recordMutationReceipt,
      reconciliation: args.reconciliation,
      runAcknowledged: acknowledged?.delivery.id,
      acknowledgeOnly: !params.wait
    })
    if (handsOverBatch(result) || (residualAck && !params.wait)) {
      return { acknowledged: result?.acknowledged ?? undefined, result }
    }
    if (result?.acknowledged) {
      acknowledged = { delivery: { id: result.acknowledged } }
    }
    recordAcknowledged()
    try {
      revalidateConsumer()
    } catch (error) {
      if (acknowledged) {
        return {
          acknowledged: acknowledged.delivery.id,
          result: interruptedAcknowledgedCheck(run.id, acknowledged.delivery.id, 'consumer_fenced')
        }
      }
      throw error
    }
    const inspectingHistory =
      params.all === true || (params.unread === false && params.peek !== true)
    if (
      result &&
      result.count > 0 &&
      (!inspectingHistory || db.getUnreadMessages(`dispatch:${residual.id}`).length > 0)
    ) {
      return {
        acknowledged: acknowledged?.delivery.id,
        result: { ...result, acknowledged: acknowledged?.delivery.id ?? null }
      }
    }
  }

  // A supplied Delivery outside this caller's current Dispatch must still fail acknowledgment.
  if (params.ack && !acknowledged) {
    acknowledgeRun()
  }
  return { acknowledged: acknowledged?.delivery.id }
}
