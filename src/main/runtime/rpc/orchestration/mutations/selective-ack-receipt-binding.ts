import {
  sessionCallerOf,
  type OrchestrationEntryCaller
} from '../../../orchestration/addressing/orchestration-caller-identity'
import { getMessagesByIds } from '../../../orchestration/db/messages/messages-by-id'
import { structuredWorkerSession } from '../../../orchestration/terminal-authority/structured-sessions/structured-worker-authority'
import { sessionIdFromStructuredWorkerIncarnation } from '../../../orchestration/terminal-authority/structured-sessions/structured-worker-incarnation'
import { parsePaneKey } from '../../../../../shared/workspace-layout/stable-pane-id'
import { agentSessionOwnerObservationIsLive } from '../../../../../shared/agent-session/lifecycle/leases/agent-session-lease-adjudication'
import { assertSelectableDirectMailbox } from '../../methods/orchestration/messaging/check-direct'
import type { OrcaRuntimeService } from '../../../orca-runtime'
import { OrchestrationError } from '../../../orchestration/orchestration-error'
import type { RpcRequest } from '../../core'
import { callerHoldsResidualDispatch } from '../../methods/orchestration/messaging/check-run-pending-mail'
import { resolveCheckDispatch } from '../../methods/orchestration/messaging/dispatch-mailbox-fence'
import {
  assertDirectMailboxConsumer,
  resolveMailboxCheckScope
} from '../../methods/orchestration/messaging/check-methods'
import { resolveRunScope } from '../../methods/orchestration/runs/run-scope'
import { legacyReadOnlyError } from '../compatibility/orchestration-legacy-process-identity'
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
  caller: OrchestrationEntryCaller | undefined
): Promise<unknown> {
  const requestId = request.orchestrationRequestId
  if (!requestId) {
    throw new OrchestrationError(
      'invalid_argument',
      'Selective acknowledgment requires a request ID.',
      { effectsApplied: false }
    )
  }
  const db = runtime.getOrchestrationDb()
  const recovery = runtime.finishOrchestrationStartupMailRecovery()
  if (recovery) {
    await recovery
  }
  const readBinding = () => selectiveAckReceiptBinding(runtime, request, params, caller)
  const target = readBinding()
  const fingerprint = callerFingerprint ?? db.readLocalMutationCallerFingerprint()
  if (fingerprint === undefined) {
    throw new Error('Local mutation caller identity was not initialized.')
  }
  const existing = db.getMutationReceipt(fingerprint, requestId)
  if (
    !existing &&
    params &&
    typeof params === 'object' &&
    'replayOnly' in params &&
    params.replayOnly === true
  ) {
    throw new OrchestrationError(
      'operation_unknown',
      `Selective acknowledgment ${requestId} has no stored receipt. Inspect the mailbox before issuing a fresh request.`,
      { effectsApplied: false }
    )
  }
  const binding = hashCanonical(target)
  const { method } = request
  const payloadHash = hashCanonical({
    method,
    params: replayStableCallerParams(runtime, params, caller, method),
    binding
  })
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
    if (!target.replayable) {
      throw new OrchestrationError(
        'operation_unknown',
        `Selective acknowledgment ${requestId} has no verifiable original process or lease.`,
        { effectsApplied: false }
      )
    }
    return attachMutationReceipt(JSON.parse(existing.receipt), requestId, true)
  }
  const result = await invoke({
    recordReceipt: (receipt) => {
      if (hashCanonical(readBinding()) !== binding) {
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
  caller: OrchestrationEntryCaller | undefined
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
  const db = runtime.getOrchestrationDb()
  const scope = resolveMailboxCheckScope(
    runtime,
    db,
    { terminal: handle, terminalPaneKey, run, ackIds },
    caller
  )
  const { livePane, paneKey } = scope
  // This is the authorized consumer snapshot, not a second origin check.
  const process = runtime.getTerminalProcessIncarnation(handle)
  const structuredSessionId =
    sessionIdFromStructuredWorkerIncarnation(process) ??
    scope.caller.orcaSessionId ??
    (scope.kind === 'dispatch' ? scope.activeDispatch?.assignee_orca_session_id : undefined) ??
    null
  const lease =
    sessionCallerOf(caller)?.revalidateAckLease?.() ??
    (structuredSessionId ? currentWorkerLease(structuredSessionId) : undefined)
  const replayable =
    lease !== undefined ||
    (structuredSessionId === null &&
      process !== null &&
      runtime.getLiveTerminalPaneKey(handle) !== null)
  const target = {
    process,
    lease,
    livePane: stablePane(livePane),
    pane: stablePane(paneKey),
    replayable
  }
  if (scope.kind === 'direct') {
    assertDirectMailboxConsumer(db, scope)
    assertSelectableDirectMailbox(db, handle, scope.unboundOnly)
    return { ...target, direct: handle }
  }
  if (scope.kind === 'dispatch') {
    const { activeDispatch: dispatch, remoteAttachment: attachment } = scope
    return {
      ...target,
      dispatch: dispatch
        ? [dispatch.id, dispatch.run_id, dispatch.consumer_generation]
        : attachment
          ? [attachment.dispatch_id, attachment.home_run_id, attachment.consumer_generation]
          : undefined
    }
  }
  const currentRun = resolveRunScope(runtime, {
    runId: run,
    callerTerminalHandle: handle,
    callerPaneKey: paneKey,
    callerSession: caller,
    requireCurrentConsumer: true,
    callerEvidence: request.orchestrationCompatibilityEvidence
  })
  // A retained binding stays legacy even if the request omits compatibility evidence.
  if (
    currentRun.coordinator_handle &&
    currentRun.coordinator_pane_key &&
    db.getLegacyAdoption()?.adopted_run_id === currentRun.id &&
    db.resolveLegacyCoordinatorCandidate({
      runId: currentRun.id,
      terminalHandle: currentRun.coordinator_handle,
      paneKey: currentRun.coordinator_pane_key
    })
  ) {
    throw legacyReadOnlyError()
  }
  const dispatch = !run ? resolveCheckDispatch(db, handle, paneKey, true, true) : undefined
  let residual: [string, number] | undefined
  if (
    dispatch &&
    !db.hasOutstandingMailboxDelivery(`run:${currentRun.id}`) &&
    callerHoldsResidualDispatch(
      runtime,
      db,
      dispatch,
      currentRun.id,
      paneKey,
      scope.caller.orcaSessionId
    )
  ) {
    const mailbox = `dispatch:${dispatch.id}`
    for (const row of getMessagesByIds(db, ackIds).values()) {
      if (row.to_handle === mailbox) {
        residual = [dispatch.id, dispatch.consumer_generation]
        break
      }
    }
  }
  return { ...target, run: [currentRun.id, currentRun.consumer_generation], dispatch: residual }
}

function stablePane(pane: string | null | undefined) {
  return pane ? (parsePaneKey(pane)?.leafId ?? pane) : null
}

function currentWorkerLease(sessionId: string): readonly [string, number] | undefined {
  const session = structuredWorkerSession({ sessionId })
  return session.kind === 'here' &&
    agentSessionOwnerObservationIsLive(session.record.ownerObservation)
    ? [session.sessionId, session.record.currentFence]
    : undefined
}
