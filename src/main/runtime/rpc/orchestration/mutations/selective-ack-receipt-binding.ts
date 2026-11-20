import { ORCHESTRATION_UNBOUND_RUN_ID } from '../../../../../shared/orchestration/orchestration-rpc-contract'
import type { OrchestrationEntryCaller } from '../../../orchestration/addressing/orchestration-caller-identity'
import { getMessagesByIds } from '../../../orchestration/db/messages/messages-by-id'
import type { OrcaRuntimeService } from '../../../orca-runtime'
import { OrchestrationError } from '../../../orchestration/orchestration-error'
import type { RpcRequest } from '../../core'
import {
  assertSelectiveAckCaller,
  assertSelectiveAckDispatchProcess
} from '../callers/selective-ack-caller'
import {
  orchestrationCallerIdentity,
  resolveRunScope
} from '../../methods/orchestration/runs/run-scope'
import type { DurableMutationInvocation } from './orchestration-mutation-executor'
import {
  attachMutationReceipt,
  hashCanonical,
  replayStableCallerParams
} from './orchestration-mutation-receipt'

export async function runSelectiveAckMutation(
  runtime: OrcaRuntimeService,
  request: RpcRequest,
  params: unknown,
  invoke: (mutation: DurableMutationInvocation) => unknown,
  callerFingerprint: string | undefined,
  caller: OrchestrationEntryCaller | undefined,
  paired: boolean
): Promise<unknown> {
  const requestId = request.orchestrationRequestId
  if (!requestId) {
    throw new OrchestrationError(
      'invalid_argument',
      'Selective acknowledgment requires a request ID.',
      { effectsApplied: false }
    )
  }
  const readBinding = () =>
    hashCanonical(selectiveAckReceiptBinding(runtime, request, params, caller, paired))
  const binding = readBinding()
  const { method } = request
  const payloadHash = hashCanonical({
    method,
    params: replayStableCallerParams(runtime, params, caller, method),
    binding
  })
  const db = runtime.getOrchestrationDb()
  const fingerprint = callerFingerprint ?? db.readLocalMutationCallerFingerprint()
  const existing = fingerprint ? db.getMutationReceipt(fingerprint, requestId) : undefined
  if (existing) {
    if (existing.method !== method || existing.payload_hash !== payloadHash) {
      throw new OrchestrationError(
        'request_mismatch',
        `Mutation request ${requestId} was already used with different input.`
      )
    }
    if (existing.state !== 'completed' || existing.receipt === null) {
      throw new OrchestrationError(
        'operation_unknown',
        `Mutation ${requestId} has no completed receipt.`
      )
    }
    return attachMutationReceipt(JSON.parse(existing.receipt), requestId, true)
  }
  const result = await invoke({
    recordReceipt: (receipt) => {
      if (readBinding() !== binding) {
        throw new OrchestrationError('consumer_fenced', 'The selective mailbox caller changed.', {
          effectsApplied: false
        })
      }
      db.beginMutationReceipt({
        callerFingerprint: fingerprint,
        requestId,
        method,
        payloadHash,
        receipt: JSON.stringify(attachMutationReceipt(receipt, requestId, false))
      })
    }
  })
  return attachMutationReceipt(result, requestId, false)
}

/** Read only: receipt replay must not route or take mail. */
function selectiveAckReceiptBinding(
  runtime: OrcaRuntimeService,
  request: RpcRequest,
  params: unknown,
  caller: OrchestrationEntryCaller | undefined,
  paired: boolean
) {
  if (
    !params ||
    typeof params !== 'object' ||
    !('ackIds' in params) ||
    !Array.isArray(params.ackIds)
  ) {
    throw new OrchestrationError(
      'invalid_argument',
      'Selective acknowledgment requires message IDs.'
    )
  }
  const ackIds = params.ackIds.filter((id): id is string => typeof id === 'string')
  const handle =
    'terminal' in params && typeof params.terminal === 'string' ? params.terminal : 'unknown'
  const terminalPaneKey =
    'terminalPaneKey' in params && typeof params.terminalPaneKey === 'string'
      ? params.terminalPaneKey
      : undefined
  const run = 'run' in params && typeof params.run === 'string' ? params.run : undefined
  const livePane = runtime.getTerminalPaneKey(handle)
  const process = runtime.getTerminalProcessIncarnation(handle)
  const callerBinding = assertSelectiveAckCaller(caller, runtime, handle, process, {
    evidence: request.orchestrationCompatibilityEvidence,
    paired
  })
  if (run === ORCHESTRATION_UNBOUND_RUN_ID) {
    return { process, callerBinding, livePane, direct: handle }
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
          assertSelectiveAckDispatchProcess(
            dispatch,
            process,
            callerBinding.lease ? caller?.orcaSessionId : null
          )
          residual = [dispatch.id, dispatch.consumer_generation]
          break
        }
      }
    }
    return {
      process,
      callerBinding,
      livePane,
      run: [currentRun.id, currentRun.consumer_generation],
      dispatch: residual
    }
  }
  const attachment =
    !dispatch && paneKey ? db.findActiveRemoteAttachmentForPane(paneKey) : undefined
  if (dispatch) {
    assertSelectiveAckDispatchProcess(
      dispatch,
      process,
      callerBinding.lease ? caller?.orcaSessionId : null
    )
  }
  if (
    attachment &&
    paneKey &&
    !db.isRemoteAttachmentProcessCurrent({
      dispatchId: attachment.dispatch_id,
      paneKey,
      processIncarnation: process
    })
  ) {
    throw new OrchestrationError('consumer_fenced', 'The remote Dispatch caller was replaced.', {
      effectsApplied: false
    })
  }
  return {
    process,
    callerBinding,
    livePane,
    dispatch: dispatch
      ? [dispatch.id, dispatch.run_id, dispatch.consumer_generation]
      : attachment
        ? [attachment.dispatch_id, attachment.home_run_id, attachment.consumer_generation]
        : undefined,
    direct: dispatch || attachment ? undefined : handle
  }
}
