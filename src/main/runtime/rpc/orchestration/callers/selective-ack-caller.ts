import type { OrchestrationEntryCaller } from '../../../orchestration/addressing/orchestration-caller-identity'
import { OrchestrationError } from '../../../orchestration/orchestration-error'
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
  process: string | null
) {
  if (caller?.kind === 'peer') {
    throw selectiveAckPeerError()
  }
  const lease = caller?.revalidateAckLease?.()
  if (!process && !lease) {
    throw new OrchestrationError(
      'consumer_fenced',
      'Selective acknowledgment requires a live mailbox caller.',
      { effectsApplied: false }
    )
  }
  return lease
}
