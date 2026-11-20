import type { OrchestrationCompatibilityEvidence } from '../../../../../shared/orchestration/orchestration-compatibility-evidence'
import type { OrchestrationEntryCaller } from '../../../orchestration/addressing/orchestration-caller-identity'
import type { DispatchContextRow } from '../../../orchestration/db/orchestration-records'
import { OrchestrationError } from '../../../orchestration/orchestration-error'
import { sessionIdFromStructuredWorkerIncarnation } from '../../../orchestration/terminal-authority/structured-sessions/structured-worker-incarnation'
import type { OrcaRuntimeService } from '../../../orca-runtime'
import type { RpcRequest } from '../../core'

export function isSelectiveAckRequest({ method }: RpcRequest): boolean {
  return method === 'orchestration.ackMessages'
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
  process: string | null,
  origin?: { evidence: OrchestrationCompatibilityEvidence | undefined; paired: boolean }
) {
  if (caller?.kind === 'peer') {
    throw selectiveAckPeerError()
  }
  const lease = caller?.revalidateAckLease?.()
  if (lease) {
    return { lease }
  }
  // General pane/process lookups can include a relay-only SSH attachment, not a hosted PTY.
  const authority =
    process && !sessionIdFromStructuredWorkerIncarnation(process)
      ? runtime.getOrchestrationDispatchAuthority(handle)
      : null
  if (
    !authority ||
    authority.terminalHandle !== handle ||
    !authority.ptyId ||
    !authority.paneKey ||
    authority.paneKey !== runtime.getTerminalPaneKey(handle) ||
    authority.processIncarnation !== process
  ) {
    throw new OrchestrationError(
      'consumer_fenced',
      'Selective acknowledgment requires a live mailbox caller.',
      { effectsApplied: false }
    )
  }
  if (origin) {
    const { evidence, paired } = origin
    const requiresLaunchProof = !paired && authority.launchTokenHash !== null
    if (requiresLaunchProof || evidence?.launchToken || evidence?.host) {
      const proof = runtime.verifyOrchestrationCompatibilityCaller(evidence, {
        currentRuntimeLaunchSufficient: true
      })
      if (
        !proof ||
        proof.terminalHandle !== handle ||
        proof.paneKey !== authority.paneKey ||
        proof.processIncarnation !== process ||
        (requiresLaunchProof && proof.launchTokenHash !== authority.launchTokenHash)
      ) {
        throw terminalEvidenceMismatch(requiresLaunchProof)
      }
    } else if (
      (evidence?.terminalHandle || evidence?.paneKey) &&
      (evidence.terminalHandle !== handle || evidence.paneKey !== authority.paneKey)
    ) {
      throw terminalEvidenceMismatch(false)
    }
  }
  return { pty: { id: authority.ptyId, launchTokenHash: authority.launchTokenHash } }
}

function terminalEvidenceMismatch(requiresLaunchProof: boolean): OrchestrationError {
  return new OrchestrationError(
    'consumer_fenced',
    requiresLaunchProof
      ? 'Selective acknowledgment requires the current agent pane launch token. Run it from that pane or use an ordinary check.'
      : 'Terminal evidence does not match this mailbox caller.',
    { effectsApplied: false }
  )
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
