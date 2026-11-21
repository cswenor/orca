import { defineOrchestrationMethod } from '../access/orchestration-method-access'
import { OrchestrationError } from '../../../../orchestration/orchestration-error'
import {
  AckMessagesParams,
  CheckParams,
  type MailboxCheckParams
} from '../../../../../../shared/rpc-contract/orchestration/orchestration-params'
import type { RpcContext } from '../../../core'
import type { OrcaRuntimeService } from '../../../../orca-runtime'
import type { OrchestrationDb } from '../../../../orchestration/db/orchestration-db'
import { parseMessageTypes } from '../routing'
import { checkRunMailbox } from './check-run'
import { checkWorkerMailbox } from './check-worker'
import { assertDirectMailboxConsumer, checkDirectMailbox } from './check-direct'
import { withOwnDirectMail } from './check-own-direct-mail'
import { CheckMailReconciliation } from './check-mail-reconciliation'
import {
  checkedMailboxes,
  handsOverBatch,
  withReplyFollowUps,
  withReportItems
} from './mailbox-message-receipt'
import { withRunCheckNext } from '../receipt-next'
import {
  ORCHESTRATION_UNBOUND_RUN_ID,
  orchestrationSkillRecoveryData
} from '../../../../../../shared/orchestration/orchestration-rpc-contract'
import {
  hasRunBindingKey,
  sessionCallerOf,
  type OrchestrationEntryCaller
} from '../../../../orchestration/addressing/orchestration-caller-identity'
import { orchestrationCallerIdentity } from '../runs/run-scope'
import {
  callerHoldsDispatchPane,
  dispatchFenced,
  isSupersededDispatch,
  resolveCheckDispatch
} from './dispatch-mailbox-fence'
import { assertSelectableMailboxHandle } from '../../../orchestration/compatibility/orchestration-legacy-process-identity'
import { isEquivalentPaneKey } from '../../../../orchestration/db/pane-key-match'

/** The ordinary consuming check and selective receipt replay authorize the same mailbox. */
export function resolveMailboxCheckScope(
  runtime: OrcaRuntimeService,
  db: OrchestrationDb,
  params: Pick<
    MailboxCheckParams,
    'terminal' | 'terminalPaneKey' | 'run' | 'peek' | 'all' | 'unread' | 'ackIds'
  >,
  orchestrationCaller: OrchestrationEntryCaller | undefined
) {
  const paneOnly = params.ackIds && !params.terminal && params.terminalPaneKey
  const handle = paneOnly
    ? runtime.getExistingTerminalHandleForPaneKey(paneOnly)
    : (params.terminal ?? 'unknown')
  const readOnly = params.ackIds !== undefined
  const livePane = handle ? runtime.getTerminalPaneKey(handle, undefined, readOnly) : null
  if (!handle || (paneOnly && (!livePane || !isEquivalentPaneKey(livePane, paneOnly)))) {
    throw new OrchestrationError('consumer_fenced', 'No live pane caller. Pass --terminal.', {
      effectsApplied: false
    })
  }
  if (params.ackIds && (handle.startsWith('run:') || handle.startsWith('dispatch:'))) {
    throw new OrchestrationError(
      'invalid_argument',
      'Selective acknowledgment requires a terminal, not a role mailbox address. Use the Run coordinator or Dispatch worker terminal.',
      { effectsApplied: false }
    )
  }
  if (params.ackIds) {
    assertSelectableMailboxHandle(db, handle)
  }
  const caller = orchestrationCallerIdentity(runtime, {
    handle,
    session: orchestrationCaller,
    readOnly,
    // A live runtime handle is authoritative; pane metadata is only the restart fallback.
    paneKey: livePane ?? params.terminalPaneKey
  })
  const paneKey = caller.paneKey ?? undefined
  const boundRun = hasRunBindingKey(caller) ? db.getCurrentRunForCoordinator(caller) : undefined
  const consumingCheck = params.peek !== true && params.all !== true && params.unread !== false
  // run_unbound names the caller's direct mailbox, not a Run.
  const unboundOnly = params.run === ORCHESTRATION_UNBOUND_RUN_ID
  const callerScope = { handle, livePane, caller, paneKey, consumingCheck }
  if (!unboundOnly && (params.run || boundRun)) {
    return { ...callerScope, kind: 'run' as const, boundRun }
  }

  const activeDispatch = resolveCheckDispatch(
    db,
    handle,
    paneKey,
    params.ackIds ? { sessionId: caller.orcaSessionId } : undefined
  )
  // Reading another pane's Dispatch mail is wrong in every mode, so peek is fenced too.
  if (activeDispatch && !callerHoldsDispatchPane(activeDispatch, paneKey)) {
    throw dispatchFenced()
  }
  const remoteAttachment =
    !activeDispatch && paneKey ? db.findActiveRemoteAttachmentForPane(paneKey) : undefined
  if (params.ackIds && remoteAttachment && !unboundOnly) {
    throw new OrchestrationError(
      'legacy_read_only',
      'Legacy mailboxes do not support selective acknowledgment.',
      { effectsApplied: false }
    )
  }
  if (
    remoteAttachment &&
    !db.isRemoteAttachmentProcessCurrent({
      dispatchId: remoteAttachment.dispatch_id,
      paneKey: paneKey ?? null,
      processIncarnation: runtime.getTerminalProcessIncarnation(handle, undefined, readOnly)
    })
  ) {
    throw new OrchestrationError(
      'dispatch_inactive',
      `Dispatch ${remoteAttachment.dispatch_id} is no longer attached to this worker process.`
    )
  }
  if ((activeDispatch || remoteAttachment) && !unboundOnly) {
    return { ...callerScope, kind: 'dispatch' as const, activeDispatch, remoteAttachment }
  }

  // A superseded Attempt must receive a stop signal, not an empty checkpoint.
  const settledDispatch = consumingCheck ? db.getLatestDispatchForTerminal(handle) : undefined
  if (settledDispatch && isSupersededDispatch(settledDispatch)) {
    throw dispatchFenced()
  }
  if (!hasRunBindingKey(caller) && consumingCheck) {
    throw new OrchestrationError(
      'stable_pane_required',
      `Terminal ${handle} has no live pane and no active Dispatch, so a consuming check here can never receive mail. Run check from the agent's own live terminal.`,
      orchestrationSkillRecoveryData()
    )
  }
  return { ...callerScope, kind: 'direct' as const, unboundOnly }
}

async function checkMailbox(
  params: MailboxCheckParams,
  {
    orchestrationCompatibilityEvidence,
    orchestrationCaller,
    runtime,
    signal,
    legacyCoordinatorRunId,
    revalidateLegacyCoordinator,
    recordMutationReceipt
  }: RpcContext
): Promise<unknown> {
  const db = runtime.getOrchestrationDb()
  const typeFilter = parseMessageTypes(params.types)
  const scope = resolveMailboxCheckScope(runtime, db, params, orchestrationCaller)
  const { handle, livePane, paneKey, consumingCheck } = scope

  const reader = sessionCallerOf(orchestrationCaller)
  // Mail this check takes may still wait as a card in a chat's queue; that card must not run too.
  // The item line is read for this answer only; the check's recorded receipt never holds it.
  const releasing = <T>(result: T): Promise<T> => {
    if (params.ackIds) {
      return Promise.resolve(result)
    }
    if (consumingCheck && params.ack && !params.wait) {
      runtime.withdrawStaleChatMail(checkedMailboxes(result))
    }
    return withReportItems(
      runtime,
      db,
      consumingCheck && !(params.ack && !params.wait)
        ? result
        : withReplyFollowUps(runtime, db, reader, result)
    )
  }
  const readOnly = params.ackIds !== undefined
  const process = runtime.getTerminalProcessIncarnation(handle, undefined, readOnly)
  const revalidateCaller = () => {
    reader?.revalidateAckLease?.()
    if (params.ackIds) {
      assertSelectableMailboxHandle(db, handle)
    }
    if (
      runtime.getTerminalPaneKey(handle, undefined, readOnly) !== livePane ||
      runtime.getTerminalProcessIncarnation(handle, undefined, readOnly) !== process
    ) {
      throw new OrchestrationError(
        'consumer_fenced',
        'This mailbox caller was replaced while checking mail.'
      )
    }
  }
  const reconciliation = new CheckMailReconciliation(runtime, db, signal)
  try {
    if (scope.kind === 'run') {
      const { boundRun } = scope
      const result = await checkRunMailbox({
        params,
        runtime,
        db,
        handle,
        paneKey,
        callerSession: orchestrationCaller,
        typeFilter,
        signal,
        legacyCoordinatorRunId,
        revalidateLegacyCoordinator,
        orchestrationCompatibilityEvidence,
        recordMutationReceipt,
        reconciliation,
        reader,
        revalidateCaller
      })
      if (params.ackIds) {
        return result
      }
      const answer =
        consumingCheck && !(params.ack && !params.wait) && !handsOverBatch(result)
          ? await withRunCheckNext(runtime, result)
          : result
      return await releasing(
        !params.run && boundRun && consumingCheck && !params.ack && !params.wait
          ? await withOwnDirectMail({
              runtime,
              db,
              reader,
              handle,
              runId: boundRun.id,
              typeFilter,
              format: params.format === true || params.inject === true,
              reconciliation,
              revalidate: revalidateCaller,
              record: recordMutationReceipt,
              result: answer
            })
          : answer
      )
    }

    if (scope.kind === 'dispatch') {
      const { activeDispatch, remoteAttachment } = scope
      return await releasing(
        await checkWorkerMailbox({
          params,
          runtime,
          db,
          handle,
          paneKey,
          typeFilter,
          signal,
          activeDispatch,
          remoteAttachment,
          recordMutationReceipt,
          reconciliation,
          reader,
          revalidateCaller,
          acknowledgeOnly: !params.wait
        })
      )
    }
    const { unboundOnly } = scope
    return await releasing(
      await checkDirectMailbox({
        params: unboundOnly ? { ...params, run: undefined } : params,
        ...(unboundOnly ? { runId: ORCHESTRATION_UNBOUND_RUN_ID } : {}),
        runtime,
        db,
        handle,
        typeFilter,
        signal,
        reconciliation,
        reader,
        recordMutationReceipt,
        revalidateConsumer: () => {
          revalidateCaller()
          assertDirectMailboxConsumer(db, scope)
        }
      })
    )
  } finally {
    reconciliation.dispose()
  }
}

export const ORCHESTRATION_CHECK_METHODS = [
  defineOrchestrationMethod({
    name: 'orchestration.check',
    access: {
      kind: 'writes',
      caller: 'terminal',
      callerPane: 'terminalPaneKey',
      targets: { run: 'run', ack: 'message' },
      collection: true
    },
    permission: 'workspace',
    params: CheckParams,
    handler: checkMailbox
  }),
  defineOrchestrationMethod({
    name: 'orchestration.ackMessages',
    permission: 'workspace',
    params: AckMessagesParams,
    access: {
      kind: 'writes',
      caller: 'terminal',
      callerPane: 'terminalPaneKey',
      targets: { run: 'run', ackIds: 'message' },
      collection: true
    },
    handler: checkMailbox
  })
]
