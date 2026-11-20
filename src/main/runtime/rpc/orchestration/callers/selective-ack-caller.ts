import type { OrchestrationEntryCaller } from '../../../orchestration/addressing/orchestration-caller-identity'
import type { DispatchContextRow } from '../../../orchestration/db/orchestration-records'
import { OrchestrationError } from '../../../orchestration/orchestration-error'
import { sessionIdFromStructuredWorkerIncarnation } from '../../../orchestration/terminal-authority/structured-sessions/structured-worker-incarnation'
import type { OrcaRuntimeService } from '../../../orca-runtime'
import type { RpcRequest } from '../../core'

export function isSelectiveAckRequest({ method, params }: RpcRequest): boolean {
  return (
    method === 'orchestration.ackMessages' ||
    (method === 'orchestration.check' &&
      !!params &&
      typeof params === 'object' &&
      'ackIds' in params)
  )
}

export function selectiveAckPeerError(): OrchestrationError {
  return new OrchestrationError(
    'invalid_argument',
    'Selective acknowledgment is unavailable to peer-Orca federation callers without a local mailbox lease.',
    { effectsApplied: false }
  )
}

export function assertSelectiveAckCaller(
  caller: OrchestrationEntryCaller | undefined,
  runtime: OrcaRuntimeService,
  handle: string,
  process: string | null
) {
  if (caller?.kind === 'peer') {
    throw selectiveAckPeerError()
  }
  const lease = caller?.revalidateAckLease?.()
  if (
    !lease &&
    (!process ||
      sessionIdFromStructuredWorkerIncarnation(process) ||
      !runtime.getLiveTerminalPaneKey(handle))
  ) {
    throw new OrchestrationError(
      'consumer_fenced',
      'Selective acknowledgment requires a live mailbox caller.',
      { effectsApplied: false }
    )
  }
  return lease
}

export function assertSelectiveAckDispatchProcess(
  dispatch: Pick<DispatchContextRow, 'process_incarnation' | 'assignee_orca_session_id'>,
  process: string | null,
  verifiedAssignee: string | null | undefined
): void {
  const liveTerminal =
    process !== null &&
    dispatch.process_incarnation === process &&
    !sessionIdFromStructuredWorkerIncarnation(process)
  const liveAssignee =
    verifiedAssignee != null && verifiedAssignee === dispatch.assignee_orca_session_id
  if (!liveTerminal && !liveAssignee) {
    throw new OrchestrationError(
      'consumer_fenced',
      'This Dispatch is not attached to the current process. No messages were acknowledged.',
      { effectsApplied: false }
    )
  }
}
