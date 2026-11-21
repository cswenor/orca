import { OrchestrationError } from '../../../orchestration/orchestration-error'
import type { RpcRequest } from '../../core'

export function isSelectiveAckRequest({ method }: RpcRequest): boolean {
  return method === 'orchestration.ackMessages'
}

export function selectiveAckPeerError(): OrchestrationError {
  return new OrchestrationError(
    'invalid_argument',
    'Selective acknowledgment is unavailable to peer-Orca federation callers because retry receipts cannot be bound safely.',
    { effectsApplied: false }
  )
}
