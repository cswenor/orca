import { ORCHESTRATION_UNBOUND_RUN_ID } from '../../../../../shared/orchestration/orchestration-rpc-contract'
import type { OrchestrationEntryCaller } from '../../../orchestration/addressing/orchestration-caller-identity'
import { getMessagesByIds } from '../../../orchestration/db/messages/messages-by-id'
import type { OrcaRuntimeService } from '../../../orca-runtime'
import type { RpcRequest } from '../../core'
import { assertSelectiveAckCaller } from '../callers/selective-ack-caller'
import {
  orchestrationCallerIdentity,
  resolveRunScope
} from '../../methods/orchestration/runs/run-scope'

/** Read only: receipt replay happens before the mailbox handler and must not route or take mail. */
export function selectiveAckReceiptBinding(
  runtime: OrcaRuntimeService,
  request: RpcRequest,
  params: unknown,
  caller: OrchestrationEntryCaller | undefined
) {
  if (request.method !== 'orchestration.ackMessages' && request.method !== 'orchestration.check') {
    return undefined
  }
  const { ackIds, terminal, terminalPaneKey, run } = params as {
    ackIds?: string[]
    terminal?: string
    terminalPaneKey?: string
    run?: string
  }
  if (!ackIds) {
    return undefined
  }
  const handle = terminal ?? 'unknown'
  const livePane = runtime.getTerminalPaneKey(handle)
  const process = runtime.getTerminalProcessIncarnation(handle)
  const lease = assertSelectiveAckCaller(caller, process)
  if (run === ORCHESTRATION_UNBOUND_RUN_ID) {
    return { process, lease, direct: handle }
  }
  const db = runtime.getOrchestrationDb()
  const identity = orchestrationCallerIdentity(runtime, {
    handle,
    paneKey: livePane ?? terminalPaneKey,
    session: caller
  })
  const paneKey = identity.paneKey ?? undefined
  const boundRun = run ?? db.getCurrentRunForCoordinator(identity)?.id
  const currentRun = boundRun
    ? resolveRunScope(runtime, {
        runId: boundRun,
        callerTerminalHandle: handle,
        callerPaneKey: paneKey,
        callerSession: caller,
        requireCurrentConsumer: true,
        callerEvidence: request.orchestrationCompatibilityEvidence
      })
    : undefined
  const dispatch = run ? undefined : db.getActiveDispatchForIdentity(handle, paneKey)
  if (currentRun) {
    let residual: [string, number] | undefined
    if (dispatch) {
      const mailbox = `dispatch:${dispatch.id}`
      for (const row of getMessagesByIds(db, ackIds).values()) {
        if (row.to_handle === mailbox) {
          residual = [dispatch.id, dispatch.consumer_generation]
          break
        }
      }
    }
    return {
      process,
      lease,
      run: [currentRun.id, currentRun.consumer_generation],
      dispatch: residual
    }
  }
  const attachment =
    !dispatch && paneKey ? db.findActiveRemoteAttachmentForPane(paneKey) : undefined
  return {
    process,
    lease,
    dispatch: dispatch
      ? [dispatch.id, dispatch.run_id, dispatch.consumer_generation]
      : attachment
        ? [attachment.dispatch_id, attachment.home_run_id, attachment.consumer_generation]
        : undefined,
    direct: dispatch || attachment ? undefined : handle
  }
}
